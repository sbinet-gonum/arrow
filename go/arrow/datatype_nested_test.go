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

package arrow

import (
	"reflect"
	"testing"
)

func TestListOf(t *testing.T) {
	for _, tc := range []DataType{
		FixedWidthTypes.Boolean,
		PrimitiveTypes.Int8,
		PrimitiveTypes.Int16,
		PrimitiveTypes.Int32,
		PrimitiveTypes.Int64,
		PrimitiveTypes.Uint8,
		PrimitiveTypes.Uint16,
		PrimitiveTypes.Uint32,
		PrimitiveTypes.Uint64,
		PrimitiveTypes.Float32,
		PrimitiveTypes.Float64,
		ListOf(PrimitiveTypes.Int32),
		FixedSizeListOf(10, PrimitiveTypes.Int32),
		StructOf(),
	} {
		t.Run(tc.Name(), func(t *testing.T) {
			got := ListOf(tc)
			want := &ListType{elem: tc}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("got=%#v, want=%#v", got, want)
			}

			if got, want := got.Name(), "list"; got != want {
				t.Fatalf("got=%q, want=%q", got, want)
			}

			if got, want := got.ID(), LIST; got != want {
				t.Fatalf("got=%v, want=%v", got, want)
			}

			if got, want := got.Elem(), tc; got != want {
				t.Fatalf("got=%v, want=%v", got, want)
			}
		})
	}

	for _, dtype := range []DataType{
		nil,
		// (*Int32Type)(nil), // FIXME(sbinet): should we make sure this is actually caught?
		// (*ListType)(nil), // FIXME(sbinet): should we make sure this is actually caught?
		// (*StructType)(nil), // FIXME(sbinet): should we make sure this is actually caught?
	} {
		t.Run("invalid", func(t *testing.T) {
			defer func() {
				e := recover()
				if e == nil {
					t.Fatalf("test should have panicked but did not")
				}
			}()

			_ = ListOf(dtype)
		})
	}
}

func TestStructOf(t *testing.T) {
	for _, tc := range []struct {
		fields []Field
		want   DataType
	}{
		{
			fields: nil,
			want:   &StructType{fields: nil, index: nil},
		},
		{
			fields: []Field{{Name: "f1", Type: PrimitiveTypes.Int32}},
			want: &StructType{
				fields: []Field{{Name: "f1", Type: PrimitiveTypes.Int32}},
				index:  map[string]int{"f1": 0},
			},
		},
		{
			fields: []Field{{Name: "f1", Type: PrimitiveTypes.Int32, Nullable: true}},
			want: &StructType{
				fields: []Field{{Name: "f1", Type: PrimitiveTypes.Int32, Nullable: true}},
				index:  map[string]int{"f1": 0},
			},
		},
		{
			fields: []Field{
				{Name: "f1", Type: PrimitiveTypes.Int32},
				{Name: "", Type: PrimitiveTypes.Int64},
			},
			want: &StructType{
				fields: []Field{
					{Name: "f1", Type: PrimitiveTypes.Int32},
					{Name: "", Type: PrimitiveTypes.Int64},
				},
				index: map[string]int{"f1": 0, "": 1},
			},
		},
		{
			fields: []Field{
				{Name: "f1", Type: PrimitiveTypes.Int32},
				{Name: "f2", Type: PrimitiveTypes.Int64},
			},
			want: &StructType{
				fields: []Field{
					{Name: "f1", Type: PrimitiveTypes.Int32},
					{Name: "f2", Type: PrimitiveTypes.Int64},
				},
				index: map[string]int{"f1": 0, "f2": 1},
			},
		},
		{
			fields: []Field{
				{Name: "f1", Type: PrimitiveTypes.Int32},
				{Name: "f2", Type: PrimitiveTypes.Int64},
				{Name: "f3", Type: ListOf(PrimitiveTypes.Float64)},
			},
			want: &StructType{
				fields: []Field{
					{Name: "f1", Type: PrimitiveTypes.Int32},
					{Name: "f2", Type: PrimitiveTypes.Int64},
					{Name: "f3", Type: ListOf(PrimitiveTypes.Float64)},
				},
				index: map[string]int{"f1": 0, "f2": 1, "f3": 2},
			},
		},
		{
			fields: []Field{
				{Name: "f1", Type: PrimitiveTypes.Int32},
				{Name: "f2", Type: PrimitiveTypes.Int64},
				{Name: "f3", Type: ListOf(ListOf(PrimitiveTypes.Float64))},
			},
			want: &StructType{
				fields: []Field{
					{Name: "f1", Type: PrimitiveTypes.Int32},
					{Name: "f2", Type: PrimitiveTypes.Int64},
					{Name: "f3", Type: ListOf(ListOf(PrimitiveTypes.Float64))},
				},
				index: map[string]int{"f1": 0, "f2": 1, "f3": 2},
			},
		},
		{
			fields: []Field{
				{Name: "f1", Type: PrimitiveTypes.Int32},
				{Name: "f2", Type: PrimitiveTypes.Int64},
				{Name: "f3", Type: ListOf(ListOf(StructOf(Field{Name: "f1", Type: PrimitiveTypes.Float64})))},
			},
			want: &StructType{
				fields: []Field{
					{Name: "f1", Type: PrimitiveTypes.Int32},
					{Name: "f2", Type: PrimitiveTypes.Int64},
					{Name: "f3", Type: ListOf(ListOf(StructOf(Field{Name: "f1", Type: PrimitiveTypes.Float64})))},
				},
				index: map[string]int{"f1": 0, "f2": 1, "f3": 2},
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			got := StructOf(tc.fields...)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got=%#v, want=%#v", got, tc.want)
			}

			if got, want := got.ID(), STRUCT; got != want {
				t.Fatalf("invalid ID. got=%v, want=%v", got, want)
			}

			if got, want := got.Name(), "struct"; got != want {
				t.Fatalf("invalid name. got=%q, want=%q", got, want)
			}

			if got, want := len(got.Fields()), len(tc.fields); got != want {
				t.Fatalf("invalid number of fields. got=%d, want=%d", got, want)
			}

			_, ok := got.FieldByName("not-there")
			if ok {
				t.Fatalf("expected an error")
			}

			if len(tc.fields) > 0 {
				f1, ok := got.FieldByName("f1")
				if !ok {
					t.Fatalf("could not retrieve field 'f1'")
				}
				if f1.HasMetadata() {
					t.Fatalf("field 'f1' should not have metadata")
				}

				for i := range tc.fields {
					f := got.Field(i)
					if f.Name != tc.fields[i].Name {
						t.Fatalf("incorrect named for field[%d]: got=%q, want=%q", i, f.Name, tc.fields[i].Name)
					}
				}
			}
		})
	}

	for _, tc := range []struct {
		fields []Field
	}{
		{
			fields: []Field{
				{Name: "", Type: PrimitiveTypes.Int32},
				{Name: "", Type: PrimitiveTypes.Int32},
			},
		},
		{
			fields: []Field{
				{Name: "x", Type: PrimitiveTypes.Int32},
				{Name: "x", Type: PrimitiveTypes.Int32},
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			defer func() {
				e := recover()
				if e == nil {
					t.Fatalf("should have panicked")
				}
			}()
			_ = StructOf(tc.fields...)
		})
	}
}

func TestFieldEqual(t *testing.T) {
	for _, tc := range []struct {
		a, b Field
		want bool
	}{
		{
			a:    Field{},
			b:    Field{},
			want: true,
		},
		{
			a:    Field{Name: "a", Type: PrimitiveTypes.Int32},
			b:    Field{Name: "a", Type: PrimitiveTypes.Int32},
			want: true,
		},
		{
			a:    Field{Name: "a", Type: PrimitiveTypes.Int32, Metadata: MetadataFrom(map[string]string{"k": "v"})},
			b:    Field{Name: "a", Type: PrimitiveTypes.Int32, Metadata: MetadataFrom(map[string]string{"k": "v"})},
			want: true,
		},
		{
			a:    Field{Name: "a", Type: PrimitiveTypes.Int32, Metadata: MetadataFrom(map[string]string{"k": "k"})},
			b:    Field{Name: "a", Type: PrimitiveTypes.Int32, Metadata: MetadataFrom(map[string]string{"k": "v"})},
			want: false,
		},
		{
			a:    Field{Name: "a", Type: PrimitiveTypes.Int32},
			b:    Field{Name: "a", Type: PrimitiveTypes.Int32, Metadata: MetadataFrom(map[string]string{"k": "v"})},
			want: false,
		},
		{
			a:    Field{Name: "a", Type: PrimitiveTypes.Int32},
			b:    Field{Name: "b", Type: PrimitiveTypes.Int32},
			want: false,
		},
		{
			a:    Field{Name: "a", Type: PrimitiveTypes.Int32},
			b:    Field{Name: "a", Type: PrimitiveTypes.Uint32},
			want: false,
		},
	} {
		t.Run("", func(t *testing.T) {
			got := tc.a.Equal(tc.b)
			if got != tc.want {
				t.Fatalf("got=%v, want=%v", got, tc.want)
			}
		})
	}
}

func TestFixedSizeListOf(t *testing.T) {
	for _, tc := range []DataType{
		FixedWidthTypes.Boolean,
		PrimitiveTypes.Int8,
		PrimitiveTypes.Int16,
		PrimitiveTypes.Int32,
		PrimitiveTypes.Int64,
		PrimitiveTypes.Uint8,
		PrimitiveTypes.Uint16,
		PrimitiveTypes.Uint32,
		PrimitiveTypes.Uint64,
		PrimitiveTypes.Float32,
		PrimitiveTypes.Float64,
		ListOf(PrimitiveTypes.Int32),
		FixedSizeListOf(10, PrimitiveTypes.Int32),
		StructOf(),
	} {
		t.Run(tc.Name(), func(t *testing.T) {
			const size = 3
			got := FixedSizeListOf(size, tc)
			want := &FixedSizeListType{elem: tc, n: size}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("got=%#v, want=%#v", got, want)
			}

			if got, want := got.Name(), "fixed_size_list"; got != want {
				t.Fatalf("got=%q, want=%q", got, want)
			}

			if got, want := got.ID(), FIXED_SIZE_LIST; got != want {
				t.Fatalf("got=%v, want=%v", got, want)
			}

			if got, want := got.Elem(), tc; got != want {
				t.Fatalf("got=%v, want=%v", got, want)
			}

			if got, want := got.Len(), int32(size); got != want {
				t.Fatalf("got=%v, want=%v", got, want)
			}
		})
	}

	for _, dtype := range []DataType{
		nil,
		// (*Int32Type)(nil), // FIXME(sbinet): should we make sure this is actually caught?
		// (*ListType)(nil), // FIXME(sbinet): should we make sure this is actually caught?
		// (*StructType)(nil), // FIXME(sbinet): should we make sure this is actually caught?
	} {
		t.Run("invalid", func(t *testing.T) {
			defer func() {
				e := recover()
				if e == nil {
					t.Fatalf("test should have panicked but did not")
				}
			}()

			_ = ListOf(dtype)
		})
	}
}

func TestMapOf(t *testing.T) {
	for _, tc := range []struct {
		key, val DataType
		sorted   bool
		want     string
	}{
		{
			key:  PrimitiveTypes.Int32,
			val:  PrimitiveTypes.Int32,
			want: "map<int32, int32>",
		},
		{
			key:    PrimitiveTypes.Int32,
			val:    PrimitiveTypes.Int32,
			sorted: true,
			want:   "map<int32, int32, keys_sorted>",
		},
		{
			key:  PrimitiveTypes.Int32,
			val:  BinaryTypes.String,
			want: "map<int32, utf8>",
		},
		{
			key:  PrimitiveTypes.Int32,
			val:  MapOf(PrimitiveTypes.Int32, BinaryTypes.String),
			want: "map<int32, map<int32, utf8>>",
		},
		{
			key:    PrimitiveTypes.Int32,
			val:    MapOf(PrimitiveTypes.Int32, BinaryTypes.String),
			sorted: true,
			want:   "map<int32, map<int32, utf8>, keys_sorted>",
		},
		{
			key:  PrimitiveTypes.Int32,
			val:  SortedMapOf(PrimitiveTypes.Int32, BinaryTypes.String),
			want: "map<int32, map<int32, utf8, keys_sorted>>",
		},
		{
			key:    PrimitiveTypes.Int32,
			val:    SortedMapOf(PrimitiveTypes.Int32, BinaryTypes.String),
			sorted: true,
			want:   "map<int32, map<int32, utf8, keys_sorted>, keys_sorted>",
		},
	} {
		t.Run(tc.want, func(t *testing.T) {
			var (
				mt1 *MapType
				mt2 *MapType
				mt3 *MapType
			)
			switch {
			case tc.sorted:
				mt1 = SortedMapOf(tc.key, tc.val)
				mt2 = SortedMapOf(tc.key, tc.val)
				mt3 = MapOf(tc.key, tc.val)
			default:
				mt1 = MapOf(tc.key, tc.val)
				mt2 = MapOf(tc.key, tc.val)
				mt3 = SortedMapOf(tc.key, tc.val)
			}

			if got, want := mt1.ID(), MAP; got != want {
				t.Fatalf("invalid type id: got=%v, want=%v", got, want)
			}

			if got, want := mt1.Sorted(), tc.sorted; got != want {
				t.Fatalf("invalid sorted attribute: got=%v, want=%v", got, want)
			}

			if got, want := mt1.Key(), tc.key; !TypeEquals(got, want) {
				t.Fatalf("invalid map-key types: got=%v, want=%v", got, want)
			}

			if got, want := mt1.Value(), tc.val; !TypeEquals(got, want) {
				t.Fatalf("invalid map-value types: got=%v, want=%v", got, want)
			}

			if got, want := mt1.String(), tc.want; got != want {
				t.Fatalf("invalid representation:\ngot= %q\nwant=%q", got, want)
			}

			if got, want := mt1, mt2; !TypeEquals(got, want) {
				t.Fatalf("identical maps should compare equal:\ngot= %v\nwant=%v", got, want)
			}

			if got, want := mt1, mt3; TypeEquals(got, want) {
				t.Fatalf("not identical maps should not compare equal:\ngot= %v\nwant=%v", got, want)
			}
		})
	}
}
