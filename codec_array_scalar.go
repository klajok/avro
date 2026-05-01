package avro

import (
	"errors"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"unsafe"

	"github.com/modern-go/reflect2"
)

func newScalarArrayDecoder(schema *ArraySchema, sliceType *reflect2.UnsafeSliceType) (ValDecoder, bool) {
	prim, ok := schema.Items().(*PrimitiveSchema)
	if !ok || prim.encodedType != "" || prim.logical != nil {
		return nil, false
	}

	elemKind := sliceType.Elem().Kind()
	switch prim.Type() {
	case Int:
		switch elemKind {
		case reflect.Int:
			return &scalarArrayDecoder[int]{sliceType, fillInts[int]}, true
		case reflect.Int8:
			return &scalarArrayDecoder[int8]{sliceType, fillInts[int8]}, true
		case reflect.Int16:
			return &scalarArrayDecoder[int16]{sliceType, fillInts[int16]}, true
		case reflect.Int32:
			return &scalarArrayDecoder[int32]{sliceType, fillInts[int32]}, true
		case reflect.Uint:
			return &scalarArrayDecoder[uint]{sliceType, fillInts[uint]}, true
		case reflect.Uint8:
			return &scalarArrayDecoder[uint8]{sliceType, fillInts[uint8]}, true
		case reflect.Uint16:
			return &scalarArrayDecoder[uint16]{sliceType, fillInts[uint16]}, true
		}
	case Long:
		switch elemKind {
		case reflect.Int:
			if strconv.IntSize == 64 {
				return &scalarArrayDecoder[int]{sliceType, fillLongs[int]}, true
			}
		case reflect.Int32:
			return &scalarArrayDecoder[int32]{sliceType, fillLongs[int32]}, true
		case reflect.Int64:
			return &scalarArrayDecoder[int64]{sliceType, fillLongs[int64]}, true
		case reflect.Uint32:
			return &scalarArrayDecoder[uint32]{sliceType, fillLongs[uint32]}, true
		}
	case Float:
		if elemKind == reflect.Float32 {
			return &scalarArrayDecoder[float32]{sliceType, fillFloat32s}, true
		}
	case Double:
		if elemKind == reflect.Float64 {
			return &scalarArrayDecoder[float64]{sliceType, fillFloat64s}, true
		}
	case Boolean:
		if elemKind == reflect.Bool {
			return &scalarArrayDecoder[bool]{sliceType, fillBools}, true
		}
	case String:
		if elemKind == reflect.String {
			return &scalarArrayDecoder[string]{sliceType, fillStrings}, true
		}
	}

	return nil, false
}

type scalarArrayDecoder[T any] struct {
	sliceType *reflect2.UnsafeSliceType
	fill      func(data []T, r *Reader)
}

func (d *scalarArrayDecoder[T]) Decode(ptr unsafe.Pointer, r *Reader) {
	sliceType := d.sliceType

	if sliceType.UnsafeIsNil(ptr) {
		sliceType.UnsafeSet(ptr, sliceType.UnsafeMakeSlice(0, 0))
	}

	var size int
	for {
		l, _ := r.ReadBlockHeader()
		if l == 0 {
			break
		}

		newSize := size + int(l)
		if newSize > r.cfg.getMaxSliceAllocSize() {
			r.ReportError("decode array", "size is greater than `Config.MaxSliceAllocSize`")
			return
		}

		sliceType.UnsafeGrow(ptr, newSize)

		base := sliceType.UnsafeGetIndex(ptr, size)
		data := unsafe.Slice((*T)(base), int(l))

		d.fill(data, r)
		if r.Error != nil {
			r.Error = fmt.Errorf("reading %s: %w", sliceType.String(), r.Error)
			return
		}

		size = newSize
	}

	if r.Error != nil && !errors.Is(r.Error, io.EOF) {
		r.Error = fmt.Errorf("%v: %w", sliceType, r.Error)
	}
}

func fillInts[T smallInt](data []T, r *Reader) {
	for i := range data {
		data[i] = T(r.ReadInt())
		if r.Error != nil {
			return
		}
	}
}

func fillLongs[T largeInt](data []T, r *Reader) {
	for i := range data {
		data[i] = T(r.ReadLong())
		if r.Error != nil {
			return
		}
	}
}

func fillFloat32s(data []float32, r *Reader) {
	for i := range data {
		data[i] = r.ReadFloat()
		if r.Error != nil {
			return
		}
	}
}

func fillFloat64s(data []float64, r *Reader) {
	for i := range data {
		data[i] = r.ReadDouble()
		if r.Error != nil {
			return
		}
	}
}

func fillBools(data []bool, r *Reader) {
	for i := range data {
		data[i] = r.ReadBool()
		if r.Error != nil {
			return
		}
	}
}

func fillStrings(data []string, r *Reader) {
	for i := range data {
		data[i] = r.ReadString()
		if r.Error != nil {
			return
		}
	}
}
