# elstore
--
    import "github.com/sebcat/elstore"

Implementation of a file-system backed element store. All inserted elements are
written to disk. The N most accessed elements are kept in memory

"Elements" in this case are byte slices. A wrapper around ElementStore can
achieve type safety and use encoding/* for T->[]byte transformation


Use case

* persistency over a moderate number of items (<100K, <1M?)

    - All items have (smallish) hash table entries in memory for housekeeping

* distinctive, non-random access pattern

    - Most read elements are cached in-mem

* immutable elements

    - Once inserted, elements can't change

Best-effort; if a write error occurs future writes will be prevented and the
error condition can be checked for, but a caller may not notice a write error
since writes are asyncronous

## Usage

```go
var ErrAlreadyExists = errors.New("Element already exists in store")
```

```go
var ErrDoesNotExist = errors.New("Element does not exist in store")
```

```go
var ErrSyncTimeout = errors.New("Syncronization timeout")
```

#### type ElementStore

```go
type ElementStore struct {
}
```


#### func  NewElementStore

```go
func NewElementStore(maxInMem int, workdir string) (c *ElementStore, err error)
```
Uses the directory 'workdir' for persistent storage and keeps at most 'maxInMem'
elements in memory over time, not counting elements that are currenly in
transfer to disk

'workdir' should point to a directory reserved for the use of ElementStore and
should not contain any other files

If 'workdir' is prevously used, the new ElementStore will be initiated using the
old values, though no cache is initially set

#### func (*ElementStore) Get

```go
func (c *ElementStore) Get(id uint64) ([]byte, error)
```
Get an element from the element store

returns ErrDoesNotExist if the ID is not recognized

#### func (*ElementStore) Has

```go
func (c *ElementStore) Has(id uint64) bool
```
Returns true if an ID exists in the store

#### func (*ElementStore) Put

```go
func (c *ElementStore) Put(elem []byte, id uint64) error
```
Insert an element into the element store

Returns ErrAlreadyExists if the ID is already in use

#### func (*ElementStore) Remove

```go
func (c *ElementStore) Remove() error
```
Remove the ElementStore from the file system permanently

#### func (*ElementStore) Sync

```go
func (c *ElementStore) Sync() error
```
Returns when all writes are completed

#### func (*ElementStore) SyncFor

```go
func (c *ElementStore) SyncFor(timeout time.Duration) error
```
Syncs and returns when done or after a timeout. If the timeout is reached,
ErrSyncTimeout is returned

#### func (*ElementStore) WriteError

```go
func (c *ElementStore) WriteError() error
```
Check to see if a write error has occurred

#### Example

```
$ go test -bench .
PASS
BenchmarkCreation          		   10000            101247 ns/op
BenchmarkInsertionMediumDataCache16        10000            114098 ns/op
BenchmarkInsertionSmallDataCache16         10000            129615 ns/op
BenchmarkInsertionMediumDataCache128       10000            119313 ns/op
BenchmarkInsertionSmallDataCache128        10000            113977 ns/op
BenchmarkInsertionMediumDataCache1024      10000            131640 ns/op
BenchmarkInsertionSmallDataCache1024       10000            108901 ns/op
BenchmarkCacheReads      		 5000000               720 ns/op
BenchmarkDiskReads       		 1000000             17231 ns/op
ok      elstore 30.041s
```

