// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package array // import "github.com/apache/arrow/go/arrow/array"

import (
	"strings"
	"sync/atomic"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/internal/debug"
	"github.com/apache/arrow/go/arrow/memory"
)

// Map is an array of key-value pairs.
type Map struct {
	array

	dtype *arrow.MapType
	keys  Interface
	vals  Interface
}

// NewMapData returns a new Map array from the provided data.
func NewMapData(data *Data) *Map {
	a := &Map{}
	a.refCount = 1
	a.setData(data)
	return a
}

func (a *Map) Retain() {
	panic("not implemented")
}

func (a *Map) Release() {
	panic("not implemented")
}

func (a *Map) Keys() Interface   { return a.keys }
func (a *Map) Values() Interface { return a.vals }

func (a *Map) String() string {
	o := new(strings.Builder)
	o.WriteString("[")
	o.WriteString("]")
	return o.String()
}

func (a *Map) setData(data *Data) {
	a.array.setData(data)

}

type MapBuilder struct {
	builder

	ktype arrow.DataType // data type of the map's keys.
	vtype arrow.DataType // data type of the map's values.

	list *ListBuilder
	keys Builder // value builder for the map's keys.
	vals Builder // value builder for the map's values.
}

// NewMapBuilder returns a builder, using the provided memory allocator.
// The created map builder will create a map whose key-value pairs will be of type ktype and vtype.
func NewMapBuilder(mem memory.Allocator, ktype, vtype arrow.DataType) *MapBuilder {
	return &MapBuilder{
		builder: builder{refCount: 1, mem: mem},
		ktype:   ktype,
		vtype:   vtype,
		list:    NewListBuilder(mem, ktype),
		keys:    newBuilder(mem, ktype),
		vals:    newBuilder(mem, vtype),
	}
}

// Release decreases the reference count by 1.
// When the reference count goes to zero, the memory is freed.
func (b *MapBuilder) Release() {
	debug.Assert(atomic.LoadInt64(&b.refCount) > 0, "too many releases")

	if atomic.AddInt64(&b.refCount, -1) == 0 {
		if b.nullBitmap != nil {
			b.nullBitmap.Release()
			b.nullBitmap = nil
		}

		if b.list != nil {
			b.list.Release()
			b.list = nil
		}

		if b.keys != nil {
			b.keys.Release()
			b.keys = nil
		}

		if b.vals != nil {
			b.vals.Release()
			b.vals = nil
		}
	}
}

func (b *MapBuilder) Append(v bool) {
	switch {
	case v:
		b.list.Append(v)
		b.length = b.list.length
	default:
		b.AppendNull()
	}
}

func (b *MapBuilder) AppendNull() {
	b.list.AppendNull()
	b.length = b.list.length
	b.nulls = b.list.nulls
}

// Reserve ensures there is enough space for appending n elements
// by checking the capacity and calling Resize if necessary.
func (b *MapBuilder) Reserve(n int) {
	b.builder.reserve(n, b.Resize)
	b.list.Reserve(n)
}

// Resize adjusts the space allocated by b to n elements. If n is greater than b.Cap(),
// additional memory will be allocated. If n is smaller, the allocated memory may reduced.
func (b *MapBuilder) Resize(n int) {
	if n < minBuilderCapacity {
		n = minBuilderCapacity
	}

	if b.capacity == 0 {
		b.init(n)
	} else {
		b.builder.resize(n, b.builder.init)
	}
	b.list.Resize(n)
	b.keys.Resize(n)
	b.vals.Resize(n)
}

// NewArray creates a List array from the memory buffers used by the builder and resets the MapBuilder
// so it can be used to build a new array.
func (b *MapBuilder) NewArray() Interface {
	return b.NewMapArray()
}

// NewMapArray creates a Map array from the memory buffers used by the builder and resets the MapBuilder
// so it can be used to build a new array.
func (b *MapBuilder) NewMapArray() (a *Map) {
	panic("not implemented")
	//	if b.offsets.Len() != b.length+1 {
	//		b.appendNextOffset()
	//	}
	//	data := b.newData()
	//	a = NewMapData(data)
	//	data.Release()
	//	return
}

var (
	_ Interface = (*Map)(nil)
	_ Builder   = (*MapBuilder)(nil)
)
