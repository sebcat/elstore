/*
Implementation of a file-system backed element store. All inserted
elements are written to disk. The N most accessed elements are kept in memory

"Elements" in this case are byte slices. A wrapper around ElementStore
can achieve type safety and use encoding/* for T->[]byte transformation

Use case

* persistency over a moderate number of items (<100K, <1M?)
  - All items have (smallish) hash table entries in memory for housekeeping
* distinctive, non-random access pattern
  - Most read elements are cached in-mem
* immutable elements
  - Once inserted, elements can't change

Best-effort; if a write error occurs future writes will be prevented and
the error condition can be checked for, but a caller may not notice a
write error since writes are asyncronous
*/
package elstore

import (
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"
)

var ErrAlreadyExists = errors.New("Element already exists in store")
var ErrDoesNotExist = errors.New("Element does not exist in store")
var ErrSyncTimeout = errors.New("Syncronization timeout")

type cacheElement struct {
	Element     []byte
	ID          uint64
	accessCount uint64 // used for caching the read count

}

type elCache []*cacheElement

func (c elCache) Len() int           { return len(c) }
func (c elCache) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }
func (c elCache) Less(i, j int) bool { return c[i].accessCount < c[j].accessCount }

type ElementStore struct {
	maxInMem int
	workdir  string

	storeMutex   sync.RWMutex
	inMem        elCache
	inMemIDMap   map[uint64][]byte
	inTransfer   map[uint64][]byte
	onDisk       map[uint64]struct{}
	readCounters map[uint64]uint64

	activeWrites sync.WaitGroup
	writeFailure error
}

func elDir(base string, id uint64) string {
	subdir := strconv.FormatUint(id&0x3f, 16)
	return filepath.Join(base, subdir)
}

func elFile(base string, id uint64) string {
	file := strconv.FormatUint(id, 16)
	return filepath.Join(elDir(base, id), file)
}

// Returns a new ElementStore

// Uses the directory  'workdir' for persistent storage and keeps at most
// 'maxInMem' elements in memory over time, not counting elements that are
// currenly in transfer to disk
//
// 'workdir' should point to a directory reserved for the use of ElementStore
// and should not contain any other files
//
// If 'workdir' is prevously used, the new ElementStore will be initiated using
// the old values, though no cache is initially set
func NewElementStore(maxInMem int, workdir string) (c *ElementStore, err error) {

	if err := os.MkdirAll(workdir, 0700); err != nil {
		return nil, err
	}

	store := &ElementStore{
		maxInMem:     maxInMem,
		workdir:      workdir,
		inMemIDMap:   make(map[uint64][]byte),
		inTransfer:   make(map[uint64][]byte),
		onDisk:       make(map[uint64]struct{}),
		readCounters: make(map[uint64]uint64),
	}

	// load IDs from disk
	walker := func(path string, info os.FileInfo, err error) error {
		if err == nil && info.Mode()&os.ModeType == 0 {
			id, err := strconv.ParseUint(info.Name(), 16, 64)
			if err == nil {
				// no error, regular file, hexname ~= elem on disk
				var x struct{}
				store.onDisk[id] = x
			}
		}

		return nil
	}

	if err := filepath.Walk(workdir, walker); err != nil {
		return nil, err
	}

	return store, nil
}

// Returns when all writes are completed
func (c *ElementStore) Sync() error {
	c.activeWrites.Wait()
	return nil
}

// Syncs and returns when done or after a timeout. If
// the timeout is reached, ErrSyncTimeout is returned
func (c *ElementStore) SyncFor(timeout time.Duration) error {
	syncChan := make(chan error, 1)
	go func() {
		syncChan <- c.Sync()
		close(syncChan)
	}()

	select {
	case err := <-syncChan:
		return err
	case <-time.After(timeout):
		return ErrSyncTimeout
	}
}

// Remove the ElementStore from the file system permanently
func (c *ElementStore) Remove() error {
	if err := c.Sync(); err != nil {
		return err
	}

	return os.RemoveAll(c.workdir)
}

// XXX: Assumes a storeMutex-lock is held
func (c *ElementStore) has(id uint64) bool {
	if _, ok := c.inMemIDMap[id]; ok {
		return true
	}

	if _, ok := c.inTransfer[id]; ok {
		return true
	}

	if _, ok := c.onDisk[id]; ok {
		return true
	}

	return false
}

// Returns true if an ID exists in the store
func (c *ElementStore) Has(id uint64) bool {
	c.storeMutex.RLock()
	defer c.storeMutex.RUnlock()
	return c.has(id)
}

// NB: signals error by setting c.writeFailure
//     to prevent future writes
func (c *ElementStore) write(elem []byte, id uint64) {
	defer func() {
		c.storeMutex.Lock()
		delete(c.inTransfer, id)
		c.storeMutex.Unlock()
		c.activeWrites.Done()
	}()

	dir := elDir(c.workdir, id)
	if err := os.MkdirAll(dir, 0700); err != nil {
		c.writeFailure = err
		return
	}

	f, err := os.Create(elFile(c.workdir, id))
	if err != nil {
		c.writeFailure = err
		return
	}

	defer f.Close()
	_, err = f.Write(elem)
	if err != nil {
		c.writeFailure = err
		return
	}

	var x struct{}
	c.storeMutex.Lock()
	c.onDisk[id] = x
	c.storeMutex.Unlock()
}

// Check to see if a write error has occurred
func (c *ElementStore) WriteError() error {
	return c.writeFailure
}

// Insert an element into the element store
//
// Returns ErrAlreadyExists if the ID is already in use
func (c *ElementStore) Put(elem []byte, id uint64) error {
	if c.writeFailure != nil {
		return c.writeFailure
	}

	c.storeMutex.Lock()
	defer c.storeMutex.Unlock()

	if c.has(id) {
		return ErrAlreadyExists
	}

	c.inTransfer[id] = elem
	c.activeWrites.Add(1)
	go c.write(elem, id)
	return nil
}

func (c *ElementStore) read(id uint64) ([]byte, error) {
	f, err := os.Open(elFile(c.workdir, id))
	if err != nil {
		return nil, err
	}

	defer f.Close()
	var ret []byte
	if ret, err = ioutil.ReadAll(f); err != nil {
		return nil, err
	}

	return ret, nil
}

func (c *ElementStore) incrReadCounter(id uint64) {
	c.storeMutex.Lock()
	defer c.storeMutex.Unlock()

	val, ok := c.readCounters[id]
	if ok {
		val += 1
		if val == 0 { //overflow check
			val -= 1
		}
	} else {
		val = 1
	}

	c.readCounters[id] = val
}

func (c *ElementStore) maybeCacheElement(el []byte, id uint64) {

	if c.maxInMem < 1 {
		return
	}

	c.storeMutex.Lock()
	defer c.storeMutex.Unlock()

	newElem := &cacheElement{
		Element:     el,
		ID:          id,
		accessCount: c.readCounters[id]}

	// always cache if cache is not full
	if len(c.inMem) < c.maxInMem {
		c.inMem = append(c.inMem, newElem)
		c.inMemIDMap[id] = el
		return
	}

	// prepare c.inMem for sorting
	for _, inMemEl := range c.inMem {
		inMemEl.accessCount = c.readCounters[inMemEl.ID]
	}

	// sort cache so that higher read count is to the left
	sort.Sort(sort.Reverse(c.inMem))

	lastIx := len(c.inMem) - 1
	lowestEl := c.inMem[lastIx]
	if lowestEl.accessCount < newElem.accessCount {
		c.inMem[lastIx] = newElem
		delete(c.inMemIDMap, lowestEl.ID)
		c.inMemIDMap[newElem.ID] = newElem.Element
	}
}

// Get an element from the element store
//
// returns ErrDoesNotExist if the ID is not recognized
func (c *ElementStore) Get(id uint64) ([]byte, error) {

	c.storeMutex.RLock()
	if el, ok := c.inMemIDMap[id]; ok {
		c.storeMutex.RUnlock()
		c.incrReadCounter(id)
		return el, nil
	} else if el, ok := c.inTransfer[id]; ok {
		c.storeMutex.RUnlock()
		c.incrReadCounter(id)
		return el, nil
	} else if _, ok := c.onDisk[id]; ok {
		c.storeMutex.RUnlock()
		// It's key that we don't hold a lock at this point
		el, err := c.read(id)
		if err != nil {
			return nil, err
		}

		// important to increment the read counter  *before* caching
		// to ensure that the ID exists in the access counter map
		c.incrReadCounter(id)

		c.maybeCacheElement(el, id)
		return el, nil
	}

	c.storeMutex.RUnlock()
	return nil, ErrDoesNotExist
}
