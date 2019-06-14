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

package arrow // import "github.com/apache/arrow/go/arrow"

import "fmt" // DictionaryType represents types with categorical or
// (in memory) dictionary-encoded values.
type DictionaryType struct {
	idx DataType
	val DataType

	ordered  bool
	bitwidth int
}

// DictOf returns a new dictionary type from the provided index/value types.
func DictOf(idx, val DataType) *DictionaryType {
	return &DictionaryType{
		idx:      idx,
		val:      val,
		bitwidth: idx.(FixedWidthDataType).BitWidth(),
	}
}

// OrderedDictOf returns a new (ordered) dictionary type from the provided index/value types.
func OrderedDictOf(idx, val DataType) *DictionaryType {
	return &DictionaryType{
		idx:      idx,
		val:      val,
		ordered:  true,
		bitwidth: idx.(FixedWidthDataType).BitWidth(),
	}
}

func (*DictionaryType) ID() Type     { return DICTIONARY }
func (*DictionaryType) Name() string { return "dictionary" }

func (t *DictionaryType) String() string {
	return fmt.Sprintf("%s<values=%v, indices=%v, ordered=%v>", t.Name(), t.idx, t.val, t.ordered)
}

// BitWidth returns the number of bits required to store a single element of this data type in memory.
func (t *DictionaryType) BitWidth() int { return t.bitwidth }

func (t *DictionaryType) Index() DataType { return t.idx }
func (t *DictionaryType) Value() DataType { return t.val }
func (t *DictionaryType) Ordered() bool   { return t.ordered }
func (t *DictionaryType) Layout() DataTypeLayout {
	layout := t.idx.Layout()
	layout.hasDict = true
	return layout
}

var (
	_ DataType           = (*DictionaryType)(nil)
	_ FixedWidthDataType = (*DictionaryType)(nil)
)
