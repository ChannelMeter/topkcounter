package topkcounter

import (
	"encoding/binary"
	"errors"
	"github.com/channelmeter/topkcounter/list"
)

type bucket struct {
	counterList *list.List // counterList<counter>
	count       int64
}

type TopKCounter struct {
	capacity   int64
	counterMap map[string]*list.Element // map[string]<Counter>
	bucketList *list.List               // bucketList<bucket>
}

type counter struct {
	bucketNode *list.Element // *bucket
	count      int64
	error      int64
	item       string
}

func newCounter(bucketNode *list.Element, item string) *counter {
	return &counter{
		bucketNode: bucketNode,
		item:       item,
		count:      0,
		error:      0,
	}
}

func newBucket(count int64) *bucket {
	return &bucket{
		counterList: list.New(),
		count:       count,
	}
}

func NewTopKCounter(capacity int64) *TopKCounter {
	return &TopKCounter{
		capacity:   capacity,
		counterMap: make(map[string]*list.Element),
		bucketList: list.New(),
	}
}

func (c *TopKCounter) Offer(item string) bool {
	return c.OfferN(item, 1)
}

func (c *TopKCounter) OfferN(item string, increment int) bool {
	r, _ := c.OfferReturnAll(item, increment)
	return r
}

func (c *TopKCounter) OfferReturnDropped(item string, increment int) string {
	_, r := c.OfferReturnAll(item, increment)
	return r
}

func (c *TopKCounter) Size() int {
	return len(c.counterMap)
}

func (c *TopKCounter) OfferReturnAll(item string, increment int) (bool, string) {
	counterNode, itemExists := c.counterMap[item]
	var droppedItem string
	if !itemExists {
		if int64(c.Size()) < c.capacity {
			counterNode = (c.bucketList.PushFront(newBucket(0)).Value.(*bucket)).counterList.PushBack(newCounter(c.bucketList.Front(), item))
		} else {
			bucketMin := c.bucketList.Front().Value.(*bucket)
			counterNode = bucketMin.counterList.Back()
			counter := counterNode.Value.(*counter)
			droppedItem = counter.item
			delete(c.counterMap, droppedItem)
			counter.item = item
			counter.error = bucketMin.count
		}
		c.counterMap[item] = counterNode
	}

	c.incrementCounter(counterNode, increment)

	return !itemExists, droppedItem
}

func (c *TopKCounter) incrementCounter(counterNode *list.Element, increment int) {
	counter := counterNode.Value.(*counter)
	oldNode := counter.bucketNode
	_bucket := oldNode.Value.(*bucket)
	_bucket.counterList.Remove(counterNode)
	counter.count += int64(increment)

	bucketNodePrev := oldNode
	bucketNodeNext := bucketNodePrev.Next()

	for bucketNodeNext != nil {
		bucketNext := bucketNodeNext.Value.(*bucket)
		if counter.count == bucketNext.count {
			bucketNext.counterList.PushFrontElement(counterNode) // Attach count_i to Bucket_i^+'s child-list
			break
		} else if counter.count > bucketNext.count {
			bucketNodePrev = bucketNodeNext
			bucketNodeNext = bucketNodePrev.Next() // Continue hunting for an appropriate bucket
		} else {
			// A new bucket has to be created
			bucketNodeNext = nil
		}
	}

	if bucketNodeNext == nil {
		bucketNext := newBucket(counter.count)
		bucketNext.counterList.PushFrontElement(counterNode)
		bucketNodeNext = c.bucketList.InsertAfter(bucketNext, bucketNodePrev) //bucketList.addAfter(bucketNodePrev, bucketNext);
	}
	counter.bucketNode = bucketNodeNext

	if _bucket.counterList.Len() == 0 {
		c.bucketList.Remove(oldNode) // Detach Bucket_i from the Stream-Summary
	}
}

func (c *TopKCounter) Peek(k int) []string {
	topK := make([]string, 0, k)
	for bNode := c.bucketList.Back(); bNode != nil; bNode = bNode.Prev() {
		b := bNode.Value.(*bucket)
		for a := b.counterList.Back(); a != nil; a = a.Prev() {
			if len(topK) == k {
				return topK
			}
			topK = append(topK, a.Value.(*counter).item)
		}
	}
	return topK
}

func (c *TopKCounter) TopK(k int) []*counter {
	topK := make([]*counter, 0, k)
	for bNode := c.bucketList.Back(); bNode != nil; bNode = bNode.Prev() {
		b := bNode.Value.(*bucket)
		for a := b.counterList.Back(); a != nil; a = a.Prev() {
			if len(topK) == k {
				return topK
			}
			topK = append(topK, a.Value.(*counter))
		}
	}
	return topK
}

func NewTopKCounterBytes(buf []byte) (c *TopKCounter, err error) {
	defer func() {
		if r := recover(); r != nil {
			switch x := r.(type) {
			case string:
				err = errors.New(x)
			case error:
				err = x
			default:
				err = errors.New("Deserialization failed.")
			}
			c = nil
		}
	}()
	capacity := int64(binary.LittleEndian.Uint64(buf))
	bucketListLen := int(binary.LittleEndian.Uint64(buf[8:]))
	p := 16
	counterMap := make(map[string]*list.Element)
	bucketList := list.New()
	for i := 0; i < bucketListLen; i++ {
		count := int64(binary.LittleEndian.Uint64(buf[p:]))
		buck := newBucket(count)
		listLen := int(binary.LittleEndian.Uint64(buf[p+8:]))
		p += 16
		buckElem := bucketList.PushBack(buck)
		counterList := list.New()
		buck.counterList = counterList
		for j := 0; j < listLen; j++ {
			ctr := &counter{}
			ctr.count = int64(binary.LittleEndian.Uint64(buf[p:]))
			ctr.error = int64(binary.LittleEndian.Uint64(buf[p+8:]))
			itemLen := int(binary.LittleEndian.Uint64(buf[p+16:]))
			p += 24
			ctr.item = string(buf[p : p+itemLen])
			p += itemLen
			ctr.bucketNode = buckElem
			ctrElem := counterList.PushBack(ctr)
			counterMap[ctr.item] = ctrElem
		}
	}

	return &TopKCounter{
		capacity:   capacity,
		counterMap: counterMap,
		bucketList: bucketList,
	}, nil
}

func (c *TopKCounter) Bytes() []byte {
	buffLen := 0
	for n := c.bucketList.Front(); n != nil; n = n.Next() {
		buck := n.Value.(*bucket)
		buffLen += 16
		for m := buck.counterList.Front(); m != nil; m = m.Next() {
			buffLen += 24 + len([]byte(m.Value.(*counter).item))
		}
	}
	buf := make([]byte, 8*2+buffLen+1)
	binary.LittleEndian.PutUint64(buf, uint64(c.capacity))
	binary.LittleEndian.PutUint64(buf[8:], uint64(c.bucketList.Len()))
	p := 16
	for n := c.bucketList.Front(); n != nil; n = n.Next() {
		buck := n.Value.(*bucket)
		binary.LittleEndian.PutUint64(buf[p:], uint64(buck.count))
		binary.LittleEndian.PutUint64(buf[p+8:], uint64(buck.counterList.Len()))
		p += 16
		for m := buck.counterList.Front(); m != nil; m = m.Next() {
			ctr := m.Value.(*counter)
			itemLen := len([]byte(ctr.item))
			binary.LittleEndian.PutUint64(buf[p:], uint64(ctr.count))
			binary.LittleEndian.PutUint64(buf[p+8:], uint64(ctr.error))
			binary.LittleEndian.PutUint64(buf[p+16:], uint64(itemLen))
			p += 24
			copy(buf[p:], ctr.item)
			p += itemLen
		}
	}
	buf[p] = 0

	return buf
}
