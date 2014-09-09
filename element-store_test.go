package elstore

import (
	"bytes"
	"os"
	"strconv"
	"testing"
)

var testDir = "n0n3x1s73n7d1r"
var testData = []byte(`
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB`)

var testData2 = []byte("Lima Oscar Victor Echo Yankee Oscar Uniform")

func dirExists(dir string) bool {
	fi, err := os.Stat(dir)
	if err == nil && (fi.Mode()&os.ModeDir) == os.ModeDir {
		return true
	} else {
		return false
	}
}

func TestCreation(t *testing.T) {

	if dirExists(testDir) {
		t.Fail()
	}

	c, err := NewElementStore(0, testDir)
	if err != nil {
		t.Error(err)
	}

	if !dirExists(testDir) {
		t.Fail()
	}

	err = c.Remove()
	if err != nil {
		t.Error(err)
	}

	if dirExists(testDir) {
		t.Error("directory exists after removal: ", testDir)
	}
}

func TestPersistentInsertRetrieve(t *testing.T) {
	testEl := []byte("FOOBAR")
	id := uint64(10)

	for cacheSize := 0; cacheSize < 3; cacheSize++ {
		c, err := NewElementStore(cacheSize, testDir)
		if err != nil {
			t.Fatal("Unable to create element store", err)
		}

		defer c.Remove()

		c.Put(testEl, id)
		ret, err := c.Get(id)
		if err != nil {
			t.Fatal(err)
		}

		if bytes.Compare(testEl, ret) != 0 {
			t.Fatal("expected", testEl, "got", ret)
		}

		c.Sync()
		c, err = NewElementStore(cacheSize, testDir)
		if err != nil {
			t.Fatal("Unable to create element store", err)
		}

		ret, err = c.Get(id)
		if err != nil {
			t.Fatal(err)
		}

		if bytes.Compare(testEl, ret) != 0 {
			t.Fatal("expected", testEl, "got", ret)
		}

	}
}

func TestNonExistingRetrieval(t *testing.T) {
	c, err := NewElementStore(0, testDir)
	if err != nil {
		t.Fatal("Unable to create element store", err)
	}

	defer c.Remove()
	val, err := c.Get(0x29a)
	if len(val) > 0 || err != ErrDoesNotExist {
		t.Fatal(val, err)
	}
}

func TestNonDuplicateInsertion(t *testing.T) {
	c, err := NewElementStore(1, testDir)
	if err != nil {
		t.Fatal(err)
	}

	defer c.Remove()
	err = c.Put(testData, 1)
	if err != nil {
		t.Fatal(err)
	}

	err = c.Put(testData2, 2)
	if err != nil {
		t.Fatal(err)
	}

	val, err := c.Get(1)
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Compare(testData, val) != 0 {
		t.Fatalf("expected\n%v\n\ngot\n%v\n\n", testData, val)
	}

	val, err = c.Get(2)
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Compare(testData2, val) != 0 {
		t.Fatalf("expected\n%v\n\ngot\n%v\n\n", testData2, val)
	}
}

func TestDuplicateInsertion(t *testing.T) {

	c, err := NewElementStore(0, testDir)
	if err != nil {
		t.Fatal(err)
	}

	defer c.Remove()
	err = c.Put(testData, 1)
	if err != nil {
		t.Fatal(err)
	}

	err = c.Put(testData, 1)
	if err != ErrAlreadyExists {
		t.Fatal("expected ErrAlreadyExists, got", err)
	}

	data, err := c.Get(1)
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Compare(testData, data) != 0 {
		t.Fatalf("expected\n%v\n\ngot\n%v\n\n", testData, data)
	}
}

func BenchmarkCreation(b *testing.B) {
	for i := 0; i < b.N; i++ {
		c, err := NewElementStore(5, testDir)
		if err != nil {
			b.Fatal(err)
		}

		c.Remove()
	}
}

func benchmarkInsertion(b *testing.B, data []byte, cacheSize int) {
	c, err := NewElementStore(cacheSize, testDir)
	if err != nil {
		b.Fatal(err)
	}

	defer c.Remove()

	for i := 0; i < b.N; i++ {
		uniqueData := append(data, []byte(strconv.Itoa(i))...)
		err := c.Put(uniqueData, uint64(i))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInsertionMediumDataCache16(b *testing.B) {
	benchmarkInsertion(b, testData, 16)

}

func BenchmarkInsertionSmallDataCache16(b *testing.B) {
	benchmarkInsertion(b, testData2, 16)
}

func BenchmarkInsertionMediumDataCache128(b *testing.B) {
	benchmarkInsertion(b, testData, 128)

}

func BenchmarkInsertionSmallDataCache128(b *testing.B) {
	benchmarkInsertion(b, testData2, 128)
}

func BenchmarkInsertionMediumDataCache1024(b *testing.B) {
	benchmarkInsertion(b, testData, 1024)

}

func BenchmarkInsertionSmallDataCache1024(b *testing.B) {
	benchmarkInsertion(b, testData2, 1024)
}

func BenchmarkCacheReads(b *testing.B) {
	c, err := NewElementStore(1, testDir)
	if err != nil {
		b.Fatal(err)
	}

	defer c.Remove()
	if err := c.Put(testData2, 0x29a); err != nil {
		b.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		// get three times to be able to compare val with
		// BenchmarkDiskReads
		for j := 0; j < 3; j++ {
			data, err := c.Get(0x29a)
			if err != nil {
				b.Fatal(err)
			}

			if bytes.Compare(data, testData2) != 0 {
				b.Fatalf("expected\n%v\n\ngot\n%v\n\n", testData2, data)
			}
		}
	}
}

func BenchmarkDiskReads(b *testing.B) {
	c, err := NewElementStore(1, testDir)
	if err != nil {
		b.Fatal(err)
	}

	defer c.Remove()
	// 0x29a should be in cache, 1 shouldn't
	if err := c.Put(testData2, 0x29a); err != nil {
		b.Fatal(err)
	}

	if err := c.Put(testData2, 1); err != nil {
		b.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		data, err := c.Get(0x29a)
		if err != nil {
			b.Fatal(err)
		}

		if bytes.Compare(data, testData2) != 0 {
			b.Fatalf("expected\n%v\n\ngot\n%v\n\n", testData2, data)
		}

		data, err = c.Get(0x29a)
		if err != nil {
			b.Fatal(err)
		}

		if bytes.Compare(data, testData2) != 0 {
			b.Fatalf("expected\n%v\n\ngot\n%v\n\n", testData2, data)
		}

		data, err = c.Get(1)
		if err != nil {
			b.Fatal(err)
		}

		if bytes.Compare(data, testData2) != 0 {
			b.Fatalf("expected\n%v\n\ngot\n%v\n\n", testData2, data)
		}
	}
}
