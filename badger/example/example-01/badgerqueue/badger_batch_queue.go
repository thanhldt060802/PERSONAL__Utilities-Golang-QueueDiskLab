package badgerqueue

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/dgraph-io/badger/v4"
)

type BatchQueueDisk struct {
	db      *badger.DB
	counter int64
}

func NewBatchQueueDisk(path string) *BatchQueueDisk {
	opts := badger.DefaultOptions(path)
	// opts.WithSyncWrites(true)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}

	bqd := &BatchQueueDisk{
		db:      db,
		counter: 0,
	}
	go bqd.GarbageCollection()

	return bqd
}

func (bqd *BatchQueueDisk) GarbageCollection() {
	if err := bqd.db.RunValueLogGC(0.5); err != nil && err != badger.ErrNoRewrite {
		log.Printf("GC error: %v", err)
	}

	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		if err := bqd.db.RunValueLogGC(0.5); err != nil && err != badger.ErrNoRewrite {
			log.Printf("GC error: %v", err)
		}
	}
}

func (bqd *BatchQueueDisk) Close() error {
	return bqd.db.Close()
}

// Enqueue: key = counter (no value), value = queueName + ":::" + payload
func (bqd *BatchQueueDisk) Enqueue(values []string) error {
	return bqd.db.Update(func(txn *badger.Txn) error {
		for _, value := range values {
			time.Sleep(10 * time.Millisecond)
			key := []byte(fmt.Sprintf("%d", bqd.counter))
			bqd.counter++

			if err := txn.Set(key, []byte(value)); err != nil {
				return err
			}
		}

		return nil
	})
}

// Dequeue: lấy 1 item đầu tiên bất kỳ
func (bqd *BatchQueueDisk) Dequeue(batchSize int) ([]string, error) {
	keysToDelete := [][]byte{}
	values := []string{}

	err := bqd.db.Update(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		currentSize := 0
		for it.Rewind(); it.Valid() && currentSize < batchSize; it.Next() {
			item := it.Item()
			k := item.KeyCopy(nil)
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}

			values = append(values, string(v))
			keysToDelete = append(keysToDelete, k)

			currentSize++
		}

		if len(values) == 0 {
			return errors.New("queue empty")
		}

		for _, key := range keysToDelete {
			if err := txn.Delete(key); err != nil {
				return err
			}
		}

		return nil
	})

	return values, err
}
