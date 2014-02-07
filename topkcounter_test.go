package topkcounter

import (
	"strings"
	"testing"
)

func TestTopK(t *testing.T) {
	tk := NewTopKCounter(3)
	stream := []string{"X", "X", "Y", "Z", "A", "B", "C", "X", "X", "A", "C", "A", "A", "X"}
	for _, s := range stream {
		tk.Offer(s)
	}
	top := strings.Join(tk.Peek(3), "")
	if strings.Contains(top, "A") && strings.Contains(top, "X") && strings.Contains(top, "C") {

	} else {
		t.Errorf("Expect AXC got %s", top)
	}

}

func TestTopKBytes(t *testing.T) {
	tk := NewTopKCounter(3)
	stream := []string{"X", "X", "Y", "Z", "A", "B", "C", "X", "X", "A", "C", "A", "A", "X"}
	for _, s := range stream {
		tk.Offer(s)
	}
	hb := tk.Bytes()
	tk, _ = NewTopKCounterBytes(hb)
	top := strings.Join(tk.Peek(3), "")
	if strings.Contains(top, "A") && strings.Contains(top, "X") && strings.Contains(top, "C") {

	} else {
		t.Errorf("Expect AXC got %s", top)
	}

}

func TestTopKBytes2(t *testing.T) {
	tk := NewTopKCounter(3)
	stream := []string{"X", "X", "Y", "Z", "A", "B", "C", "X", "X", "A", "C", "A", "A", "X"}
	z := 0
	for _, s := range stream {
		tk.Offer(s)
		z++
		if z == 5 {
			hb := tk.Bytes()
			tk, _ = NewTopKCounterBytes(hb)
		}
	}
	top := strings.Join(tk.Peek(3), "")
	if strings.Contains(top, "A") && strings.Contains(top, "X") && strings.Contains(top, "C") {

	} else {
		t.Errorf("Expect AXC got %s", top)
	}

}
