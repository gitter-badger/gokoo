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

// Insert will try to add an item to the cuckoo table.
func (gt *GokooTable) Insert(item GokooItem) bool {

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

		// insert f into i1 and get the previous fingerprint
		f = gt.evict(i1, f)

		// get the alternative index for the previous fingerprint
		i1 = gt.secondaryIndex(i1, f)

		ok = gt.add(i1, f)
		if ok {
			return true
		}
	}

	// at this point we did not manage to insert it without eviction for nTries
	return false
}

// Lookup will check if the cuckoo table contains the given item.
func (gt *GokooTable) Lookup(item GokooItem) bool {

	// get the hash of the item bytes and the fingerprint
	hash := gt.hash(item.Bytes())
	f := gt.fingerPrint(hash)

	// get the first index and check if it contains the item
	i1 := gt.primaryIndex(hash)
	if gt.has(i1, f) {
		return true
	}

	// get the second index and check if it contains the item
	i2 := gt.secondaryIndex(i1, f)
	if gt.has(i2, f) {
		return true
	}

	// item wasn't found
	return false
}

// Delete will remove the item from the cuckoo table.
func (gt *GokooTable) Remove(item GokooItem) bool {

	// get the hash of the item and the fingerprint
	hash := gt.hash(item.Bytes())
	f := gt.fingerPrint(hash)

	// get the first index and check if we can delete
	i1 := gt.primaryIndex(f)
	if gt.del(i1, f) {
		return true
	}

	// get the second index and check if we can delete
	i2 := gt.secondaryIndex(i1, f)
	if gt.del(i2, f) {
		return true
	}

	// item could not be deleted
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

	// get the xxhash of the fingerprint
	i2 := int(xxhash.Checksum32(f))

	// XOR the primary index with the hash of the fingerprint
	i2 = i1 ^ i2

	// return the alternative index
	return i2
}

// fingerPrint will return the fingerprint for a given hash.
func (gt *GokooTable) fingerPrint(hash []byte) []byte {

	// return the byte slice starting at right index and having right length
	return hash[gt.bBytes:gt.nBytes]
}

// add will add an item to the given bucket, if possible.
func (gt *GokooTable) add(i int, f []byte) bool {

	// check all slots for this bucket
	for n := 0; n < gt.nSlots; n++ {

		// start index and stop index
		index := i + n
		begin := index * gt.nBytes
		cutoff := begin + gt.nBytes

		// check if spot is free
		if gt.occupied[index] {
			continue
		}

		// save fingerprint and return
		gt.occupied[index] = true
		gt.buckets[begin:cutoff] = f
		return true
	}

	// we could not insert it anywhere
	return false
}

// has will check if a given bucket contains fingerprint f.
func (gt *GokooTable) has(i int, f []byte) bool {

	// check all slots for this bucket
	for n := 0; n < gt.nSlots; n++ {

		// start index and stop index
		index := i + n
		begin := index * gt.nBytes
		cutoff := begin + gt.nBytes

		// check if spot is used
		if !gt.occupied[index] {
			continue
		}

		// check if values match
		if gt.buckets[begin:cutoff] == f {
			return true
		}
	}

	// we could not find the fingerpnint
	return false
}

// del will delete an item from the given bucket, if possible.
func (gt *GokooTable) del(i int, f []byte) bool {

	// check all slots for this bucket
	for n := 0; n < gt.nSlots; n++ {

		// start and stop indexes
		index := i + n
		begin := index * gt.nBytes
		cutoff := begin + gt.nBytes

		// check if spot is used
		if !gt.occupied[index] {
			continue
		}

		// check if values match
		if gt.bucktes[begin:cutoff] == f {
			gt.occupied[index] = false
			return true
		}
	}

	// we could not delete the fingerprint
	return false
}

// evict will evict a fingerprint from the bucket to insert the new one.
func (gt *GokooTable) evict(i int, f []byte) []byte {

	// pick a random slot for this bucket
	slot := rand.Int() % gt.nSlots
	begin := (i + slot) * gt.nBytes
	cutoff := begin + gt.nBytes

	// get the old fingerprint and replace
	fOld := gt.buckets[begin:cutoff]
	gt.buckets[begin:cutoff] = f

	// return old fingerprint
	return fOld
}
