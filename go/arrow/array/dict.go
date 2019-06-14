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
	"github.com/apache/arrow/go/arrow"
	"github.com/pkg/errors"
)

// Dictionary represents an array for dictionary-encoded data with a
// data-dependent dictionary.
//
// A dictionary array contains an array of non-negative integers (the
// "dictionary indices") along with a data type containing a "dictionary"
// corresponding to the distinct values represented in the data.
//
// For example, the array
//
//   ["foo", "bar", "foo", "bar", "foo", "bar"]
//
// with dictionary ["bar", "foo"], would have dictionary array representation
//
//   indices: [1, 0, 1, 0, 1, 0]
//   dictionary: ["bar", "foo"]
//
// The indices in principle may have any integer type (signed or unsigned),
// though presently data in IPC exchanges must be signed int32.
type Dictionary struct {
	array

	dict    *arrow.DictionaryType
	indices Interface
}

// NewDictionaryData returns a new Dictionary array value from data.
func NewDictionaryData(data *Data) *Dictionary {
	a := &Dictionary{}
	a.refCount = 1
	a.dict = data.dtype.(*arrow.DictionaryType)
	a.setData(data)
	return a
}

// NewDictionaryFromArrays creates a new Dictionary from the provided indices and dictionary arrays.
func NewDictionaryFromArrays(dtype *arrow.DictionaryType, indices, dict Interface) (*Dictionary, error) {
	if indices.DataType().ID() != dtype.Index().ID() {
		return nil, errors.Errorf("arrow/array: mismatch dict/index type ids")
	}

	var (
		err   error
		upper = int64(dict.Len())
	)
	switch idx := indices.(type) {
	case *Int8:
		err = validateDictIdx8(idx, upper)
	case *Int16:
		err = validateDictIdx16(idx, upper)
	case *Int32:
		err = validateDictIdx32(idx, upper)
	case *Int64:
		err = validateDictIdx64(idx, upper)
	default:
		return nil, errors.Errorf("arrow/array: categorical index %T not supported", idx)
	}

	if err != nil {
		return nil, err
	}

	data := indices.Data().Copy()
	data.dtype = dtype
	data.dict = dict

	return NewDictionaryData(data), nil
}

func (a *Dictionary) DictType() *arrow.DictionaryType { return a.dict }
func (a *Dictionary) Indices() Interface              { return a.indices }
func (a *Dictionary) Dictionary() Interface           { return a.data.dict }

func (a *Dictionary) setData(data *Data) {
	a.array.setData(data)

	indices := data.Copy()
	indices.dtype = a.dict.Index()
	a.indices = MakeFromData(indices)
}

func validateDictIdx8(idx *Int8, upper int64) error {
	switch idx.NullN() {
	case 0:
		for i := 0; i < idx.Len(); i++ {
			v := int64(idx.Value(i))
			if v < 0 || v >= upper {
				return errors.Errorf("arrow/array: dictionary has out-of-bound index [0, dict.len)")
			}
		}
	default:
		for i := 0; i < idx.Len(); i++ {
			if idx.IsNull(i) {
				continue
			}
			v := int64(idx.Value(i))
			if v < 0 || v >= upper {
				return errors.Errorf("arrow/array: dictionary has out-of-bound index [0, dict.len)")
			}
		}
	}
	return nil
}

func validateDictIdx16(idx *Int16, upper int64) error {
	switch idx.NullN() {
	case 0:
		for i := 0; i < idx.Len(); i++ {
			v := int64(idx.Value(i))
			if v < 0 || v >= upper {
				return errors.Errorf("arrow/array: dictionary has out-of-bound index [0, dict.len)")
			}
		}
	default:
		for i := 0; i < idx.Len(); i++ {
			if idx.IsNull(i) {
				continue
			}
			v := int64(idx.Value(i))
			if v < 0 || v >= upper {
				return errors.Errorf("arrow/array: dictionary has out-of-bound index [0, dict.len)")
			}
		}
	}
	return nil
}

func validateDictIdx32(idx *Int32, upper int64) error {
	switch idx.NullN() {
	case 0:
		for i := 0; i < idx.Len(); i++ {
			v := int64(idx.Value(i))
			if v < 0 || v >= upper {
				return errors.Errorf("arrow/array: dictionary has out-of-bound index [0, dict.len)")
			}
		}
	default:
		for i := 0; i < idx.Len(); i++ {
			if idx.IsNull(i) {
				continue
			}
			v := int64(idx.Value(i))
			if v < 0 || v >= upper {
				return errors.Errorf("arrow/array: dictionary has out-of-bound index [0, dict.len)")
			}
		}
	}
	return nil
}

func validateDictIdx64(idx *Int64, upper int64) error {
	switch idx.NullN() {
	case 0:
		for i := 0; i < idx.Len(); i++ {
			v := int64(idx.Value(i))
			if v < 0 || v >= upper {
				return errors.Errorf("arrow/array: dictionary has out-of-bound index [0, dict.len)")
			}
		}
	default:
		for i := 0; i < idx.Len(); i++ {
			if idx.IsNull(i) {
				continue
			}
			v := int64(idx.Value(i))
			if v < 0 || v >= upper {
				return errors.Errorf("arrow/array: dictionary has out-of-bound index [0, dict.len)")
			}
		}
	}
	return nil
}

var (
	_ Interface = (*Dictionary)(nil)
)
