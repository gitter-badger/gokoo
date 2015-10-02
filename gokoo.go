package gokoo

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
	"math/rand"

	"github.com/dchest/siphash"
	"github.com/vova616/xxhash"
)

type GokooItem interface {
	Bytes() []byte
}

type GokooHash func([]byte) []byte

type GokooTable struct {
	rebuild  bool
	nBuckets int
	nSlots   int
	nBytes   int
	nTries   int

	bBytes   int
	occupied []bool
	buckets  []byte
	hash     GokooHash
	buf      *bytes.Buffer
}

func Sha256Hash(input []byte) []byte {
	array := sha256.Sum256(input)
	return array[:]
}

func SipHash(input []byte) []byte {
	number := siphash.Hash(0, 0, input)
	output := make([]byte, 8)
	binary.LittleEndian.PutUint64(output, number)
	return output
}

func New(options ...func(*GokooTable)) (*GokooTable, error) {

	gt := &GokooTable{
		rebuild:  false,
		hash:     Sha256Hash,
		nBuckets: 8,
		nSlots:   4,
		nBytes:   1,
	}

	for _, option := range options {
		option(gt)
	}

	gt.bBytes = int(math.Ceil(math.Sqrt(float64(gt.nBuckets))))
	hashLen := len(gt.hash([]byte{}))
	if hashLen < gt.bBytes+gt.nBytes {
		return nil, errors.New("hash byte length insufficient for given" +
			" number of buckets and fingerprint bytes")
	}

	gt.occupied = make([]bool, gt.nBuckets*gt.nSlots)
	gt.buckets = make([]byte, gt.nBuckets*gt.nSlots*gt.nBytes)

	return gt, nil
}

// EnableRebuild will allow the table to automatically rebuild if it is full.
func EnableRebuild(rebuild bool) func(*GokooTable) {
	return func(gt *GokooTable) {
		gt.rebuild = rebuild
	}
}

// SetHashFunc allows us to define the hash function to be used with our cuckoo
// table.
func SetHashFunc(hash GokooHash) func(*GokooTable) {
	return func(gt *GokooTable) {
		gt.hash = hash
	}
}

// SetNumBuckets sets the number of buckets our cuckoo table initially uses.
func SetNumBuckets(nBuckets int) func(*GokooTable) {
	return func(gt *GokooTable) {
		gt.nBuckets = nBuckets
	}
}

// SetNumSlots sets the number of slots per bucket.
func SetNumSlots(nSlots int) func(*GokooTable) {
	return func(gt *GokooTable) {
		gt.nSlots = nSlots
	}
}

// SetNumBytes sets the number of bytes our cuckoo table uses for item
// fingerprints.
func SetNumBytes(nBytes int) func(*GokooTable) {
	return func(gt *GokooTable) {
		gt.nBytes = nBytes
	}
}

func SetNumTries(nTries int) func(*GokooTable) {
	return func(gt *GokooTable) {
		gt.nTries = nTries
	}
}

// Add will try to add an item to the cuckoo table.
func (gt *GokooTable) Add(item GokooItem) bool {

	// get hash and fingerprint
	hash := gt.hash(item.Bytes())
	f := gt.fingerPrint(hash)

	// get first index and try to add to that bucket
	i1 := gt.primaryIndex(hash)
	ok := gt.add(i1, f)
	if ok {
		return true
	}

	// get second index and try to add to that bucket
	i2 := gt.secondaryIndex(i1, f)
	ok = gt.add(i2, f)
	if ok {
		return true
	}

	// randomly pick i1 or i2 and keep evicting in that direction
	if rand.Int()%2 == 1 {
		i1 = i2
	}

	// try for max tries number of time to kick back
	for n := 0; n < gt.nTries; n++ {

		// insert f into i1 and get the previous finger print
		f = gt.evict(i1, f)

		// get the alternative index for the previous finger print
		i1 = gt.secondaryIndex(i1, f)

		ok = gt.add(i1, f)
		if ok {
			return true
		}
	}

	// at this point we did not manage to insert it without eviction for nTries
	return false
}

// primaryIndex will return the primary index for a given hash.
func (gt *GokooTable) primaryIndex(hash []byte) int {

	// create empty index
	i1 := int(0)

	// create a buffer around the hash bytes used for indexing buckets
	buf := bytes.NewBuffer(hash[0:gt.bBytes])

	// read the bytes into our index
	binary.Write(buf, binary.LittleEndian, i1)

	// return the index modulated for number of buckets
	return i1 % gt.nBuckets
}

// secondaryIndex will return the secondary index of any given index.
func (gt *GokooTable) secondaryIndex(i1 int, f []byte) int {

	// get the xxhash of the finger print
	i2 := int(xxhash.Checksum32(f))

	// XOR the primary index with the hash of the finger print
	i2 = i1 ^ i2

	// return the alternative index
	return i2
}

// fingerPrint will return the finger print for a given hash.
func (gt *GokooTable) fingerPrint(hash []byte) []byte {

	// return the byte slice starting at right index and having right length
	return hash[gt.bBytes:gt.nBytes]
}
