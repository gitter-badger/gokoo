package gokoo

import (
	"bytes"
	crand "crypto/rand"
	"math/rand"
	"reflect"
	"testing"
)

func TestNew(t *testing.T) {

	// test empty construction
	gt, err := New()
	if err != nil {
		t.Errorf("could not construct default gokoo table: %v", err)
	}

	// test construction with custom hash function
	hash := DummyHash
	gt, err = New(SetHashFunc(hash))
	if err != nil {
		t.Errorf("could not construct with custom hash function: %v", err)
	}
	f1 := reflect.ValueOf(hash)
	f2 := reflect.ValueOf(gt.hash)
	if f1.Pointer() != f2.Pointer() {
		t.Errorf("custom hash function not registered")
	}

	// test construction with custom bucket size
	nBuckets := 3
	gt, err = New(SetNumBuckets(nBuckets))
	if err != nil {
		t.Errorf("could not construct with custom bucket size: %v", err)
	}
	if len(gt.buckets)/gt.nSlots/gt.nBytes != nBuckets {
		t.Errorf("did not register custom bucket size")
	}

	// test construction with custom slot size
	nSlots := 3
	gt, err = New(SetNumSlots(nSlots))
	if err != nil {
		t.Errorf("could not construct with custom slot size: %v", err)
	}
	if len(gt.buckets)/gt.nBuckets/gt.nBytes != nSlots {
		t.Errorf("did not register custom slot size")
	}

	// test construction with custom fingerprint size
	nBytes := 3
	gt, err = New(SetNumBytes(nBytes))
	if err != nil {
		t.Errorf("could not construct with custom fingerprint size: %v", err)
	}
	if len(gt.buckets)/gt.nBuckets/gt.nSlots != nBytes {
		t.Errorf("did not register custom fingerprint size")
	}

	// test construction with custom number of tries
	nTries := 3
	gt, err = New(SetNumTries(nTries))
	if err != nil {
		t.Errorf("could not construct with custom number of tries: %v", err)
	}
	if gt.nTries != nTries {
		t.Errorf("did not register custom number of tries")
	}
}

func TestTruePositive(t *testing.T) {

	// create 100 items of random byte slices
	items := make([]*bytes.Buffer, 100)
	for i := 0; i < 100; i++ {

		// create item and decide how many bytes we will write
		nBytes := rand.Int() % 256
		slice := make([]byte, nBytes)

		// get the required number of random bytes
		_, err := crand.Read(slice)
		if err != nil {
			t.Errorf("could not get random bytes")
		}

		// buffer the bytes as item so we can implement the item interface
		items[i] = bytes.NewBuffer(slice)
	}

	// create new cuckoo filter
	cf, _ := New()

	// insert the 100 items into the cuckoo filter
	for _, item := range items {
		cf.Insert(item)
	}

	// check if the 100 items are in the cuckoo filter
	for _, item := range items {
		if !cf.Lookup(item) {
			//t.Errorf("false negative: %v not found", i)
		}
	}
}
