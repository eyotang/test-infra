/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package diskcache implements disk backed cache storage for use in greenhouse
package diskcache

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"k8s.io/test-infra/greenhouse/diskutil"

	"github.com/sirupsen/logrus"
	"sync"
	"sort"
	"github.com/djherbis/atime"
)

const (
	MAX_DISK_SIZE = 5*1024*1024*1024*1024
)

// ErrTooBig is returned by Cache::Put when when the item size is bigger than the
// cache size limit.
type ErrTooBig struct{}

func (e *ErrTooBig) Error() string {
	return "item bigger than the cache size limit"
}

// lruItem is the type of the values stored in SizedLRU to keep track of items.
// It implements the SizedItem interface.
type lruItem struct {
	size      int64
	committed bool
}

func (i *lruItem) Size() int64 {
	return i.size
}

// ReadHandler should be implemeted by cache users for use with Cache.Get
type ReadHandler func(exists bool, contents io.ReadSeeker) error

// Cache implements disk backed cache storage
type Cache struct {
	diskRoot string
	logger   *logrus.Entry
	mux      *sync.RWMutex
	lru      SizedLRU
}

// NewCache returns a new Cache given the root directory that should be used
// on disk for cache storage
func NewCache(diskRoot string) *Cache {
	// Create the directory structure
	ensureDir(filepath.Join(diskRoot, "cas"))
	ensureDir(filepath.Join(diskRoot, "ac"))
	ensureDir(filepath.Join(diskRoot, "build-cache", "android"))

	// The eviction callback deletes the file from disk.
	onEvict := func(key Key, value SizedItem) {
		// Only remove committed items (as temporary files have a different filename)
		if value.(*lruItem).committed {
			blobPath := filepath.Join(diskRoot, key.(string))
			err := os.Remove(blobPath)
			if err != nil {
				logrus.WithError(err).Error("Remove file %s failed!!!", blobPath)
			}
		}
	}

	cache := &Cache{
		diskRoot: strings.TrimSuffix(diskRoot, string(os.PathListSeparator)),
		mux: &sync.RWMutex{},
		lru: NewSizedLRU(MAX_DISK_SIZE, onEvict),
	}

	cache.loadExistingFiles()
	return cache
}

// KeyToPath converts a cache entry key to a path on disk
func (c *Cache) KeyToPath(key string) string {
	return filepath.Join(c.diskRoot, key)
}

// PathToKey converts a path on disk to a key, assuming the path is actually
// under DiskRoot() ...
func (c *Cache) PathToKey(key string) string {
	return strings.TrimPrefix(key, c.diskRoot+string(os.PathSeparator))
}

// DiskRoot returns the root directory containing all on-disk cache entries
func (c *Cache) DiskRoot() string {
	return c.diskRoot
}

// file path helper
func exists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

// file path helper
func ensureDir(dir string) error {
	if exists(dir) {
		return nil
	}
	return os.MkdirAll(dir, os.FileMode(0744))
}

func removeTemp(path string) {
	err := os.Remove(path)
	if err != nil {
		logrus.WithError(err).Errorf("Failed to remove a temp file: %v", path)
	}
}

// Put copies the content reader until the end into the cache at key
// if contentSHA256 is not "" then the contents will only be stored in the
// cache if the content's hex string SHA256 matches
func (c *Cache) Put(key string, content io.Reader, contentSHA256 string, size int64) error {
	// make sure directory exists
	path := c.KeyToPath(key)
	dir := filepath.Dir(path)
	err := ensureDir(dir)
	if err != nil {
		logrus.WithError(err).Errorf("error ensuring directory '%s' exists", dir)
	}

	c.mux.Lock()
	// If `key` is already in the LRU, don't touch the cache and just discard
	// the incoming stream. This applies to both committed an uncommitted files
	// (we don't want to upload again if an upload of the same file is already
	// in progress).
	if _, found := c.lru.Get(key); found {
		c.mux.Unlock()
		io.Copy(ioutil.Discard, content)
		logrus.Warningf("key: %s has already been there, not required to store it again.", key)
		return nil
	}

	// Try to add the item to the LRU.
	newItem := &lruItem{
		size:      size,
		committed: false,
	}
	ok := c.lru.Add(key, newItem)
	c.mux.Unlock()
	if !ok {
		return &ErrTooBig{}
	}

	// By the time this function exits, we should either mark the LRU item as committed
	// (if the upload went well), or delete it. Capturing the flag variable is not very nice,
	// but this stuff is really easy to get wrong without defer().
	shouldCommit := false
	defer func() {
		c.mux.Lock()
		defer c.mux.Unlock()

		if shouldCommit {
			newItem.committed = true
		} else {
			c.lru.Remove(key)
		}
	}()

	// create a temp file to get the content on disk
	temp, err := ioutil.TempFile(dir, "temp-put")
	if err != nil {
		return fmt.Errorf("failed to create cache entry: %v", err)
	}

	// fast path copying when not hashing content,s
	if contentSHA256 == "" {
		_, err = io.Copy(temp, content)
		if err != nil {
			removeTemp(temp.Name())
			return fmt.Errorf("failed to copy into cache entry: %v", err)
		}

	} else {
		hasher := sha256.New()
		_, err = io.Copy(io.MultiWriter(temp, hasher), content)
		if err != nil {
			removeTemp(temp.Name())
			return fmt.Errorf("failed to copy into cache entry: %v", err)
		}
		actualContentSHA256 := hex.EncodeToString(hasher.Sum(nil))
		if actualContentSHA256 != contentSHA256 {
			removeTemp(temp.Name())
			return fmt.Errorf(
				"hashes did not match for '%s', given: '%s' actual: '%s",
				key, contentSHA256, actualContentSHA256)
		}
	}

	// move the content to the key location
	err = temp.Sync()
	if err != nil {
		removeTemp(temp.Name())
		return fmt.Errorf("failed to sync cache entry: %v", err)
	}
	temp.Close()
	err = os.Rename(temp.Name(), path)
	if err != nil {
		removeTemp(temp.Name())
		return fmt.Errorf("failed to insert contents into cache: %v", err)
	}

	// Only commit if renaming succeeded
	// This flag is used by the defer() block above.
	shouldCommit = true

	return nil
}

// Get provides your readHandler with the contents at key
func (c *Cache) Get(key string, readHandler ReadHandler) error {
	ok := func() bool {
		c.mux.Lock()
		defer c.mux.Unlock()

		val, found := c.lru.Get(key)
		// Uncommitted (i.e. uploading items) should be reported as not ok
		return found && val.(*lruItem).committed
	}()

	if !ok {
		return readHandler(ok, nil)
	}

	path := c.KeyToPath(key)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return readHandler(false, nil)
		}
		return fmt.Errorf("failed to get key: %v", err)
	}
	return readHandler(true, f)
}

func (c *Cache) Contains(key string) (ok bool, err error) {
	c.mux.Lock()
	defer c.mux.Unlock()

	val, found := c.lru.Get(key)
	return found && val.(*lruItem).committed, nil
}

// EntryInfo are returned when getting entries from the cache
type EntryInfo struct {
	Path       string
	LastAccess time.Time
}

// GetEntries walks the cache dir and returns all paths that exist
// In the future this *may* be made smarter
func (c *Cache) GetEntries() []EntryInfo {
	entries := []EntryInfo{}
	// note we swallow errors because we just need to know what keys exist
	// some keys missing is OK since this is used for eviction, but not returning
	// any of the keys due to some error is NOT
	_ = filepath.Walk(c.diskRoot, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			logrus.WithError(err).Error("error getting some entries")
			return nil
		}
		if !f.IsDir() {
			atime := diskutil.GetATime(path, time.Now())
			entries = append(entries, EntryInfo{
				Path:       path,
				LastAccess: atime,
			})
		}
		return nil
	})
	return entries
}

// Delete deletes the file at key
func (c *Cache) Delete(key string) error {
	return os.Remove(c.KeyToPath(key))
}


// loadExistingFiles lists all files in the cache directory, and adds them to the
// LRU index so that they can be served. Files are sorted by access time first,
// so that the eviction behavior is preserved across server restarts.
func (c *Cache) loadExistingFiles() {
	// Walk the directory tree
	type NameAndInfo struct {
		info os.FileInfo
		name string
	}
	var files []NameAndInfo
	filepath.Walk(c.diskRoot, func(name string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			files = append(files, NameAndInfo{info, name})
		}
		return nil
	})

	// Sort in increasing order of atime
	sort.Slice(files, func(i int, j int) bool {
		return atime.Get(files[i].info).Before(atime.Get(files[j].info))
	})

	for _, f := range files {
		key := f.name[len(c.diskRoot)+1:]
		c.lru.Add(key, &lruItem{
			size:      f.info.Size(),
			committed: true,
		})
	}
}

func(c *Cache) EvictCache(evictUntilPercentBlocksFree float64) {
	c.mux.Lock()
	maxSize := (int64)(((99 - evictUntilPercentBlocksFree) * MAX_DISK_SIZE) / 100)
	c.lru.RemoveCache(maxSize)
	c.mux.Unlock()
}