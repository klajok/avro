package avro_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/iskorotkov/avro/v2"
)

func benchScalarArrayDecode[T any](b *testing.B, schemaStr string, gen func(int) T) {
	b.Helper()

	for _, n := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			src := make([]T, n)
			for i := range src {
				src[i] = gen(i)
			}

			buf := &bytes.Buffer{}
			enc, err := avro.NewEncoder(schemaStr, buf)
			if err != nil {
				b.Fatal(err)
			}

			if err := enc.Encode(src); err != nil {
				b.Fatal(err)
			}

			payload := buf.Bytes()

			schema, err := avro.Parse(schemaStr)
			if err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))

			var got []T
			for b.Loop() {
				got = got[:0]
				dec := avro.NewDecoderForSchema(schema, bytes.NewReader(payload))
				if err := dec.Decode(&got); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkArrayDecode_Int32(b *testing.B) {
	benchScalarArrayDecode(b, `{"type":"array","items":"int"}`, func(i int) int32 { return int32(i) })
}

func BenchmarkArrayDecode_Int64(b *testing.B) {
	benchScalarArrayDecode(b, `{"type":"array","items":"long"}`, func(i int) int64 { return int64(i) })
}

func BenchmarkArrayDecode_Float32(b *testing.B) {
	benchScalarArrayDecode(b, `{"type":"array","items":"float"}`, func(i int) float32 { return float32(i) + 0.5 })
}

func BenchmarkArrayDecode_Float64(b *testing.B) {
	benchScalarArrayDecode(b, `{"type":"array","items":"double"}`, func(i int) float64 { return float64(i) + 0.5 })
}

func BenchmarkArrayDecode_Bool(b *testing.B) {
	benchScalarArrayDecode(b, `{"type":"array","items":"boolean"}`, func(i int) bool { return i%2 == 0 })
}

func BenchmarkArrayDecode_String(b *testing.B) {
	benchScalarArrayDecode(b, `{"type":"array","items":"string"}`, func(i int) string { return fmt.Sprintf("item-%d", i) })
}
