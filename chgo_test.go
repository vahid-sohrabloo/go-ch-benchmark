package gotests

import (
	"context"
	"io"
	"testing"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

func BenchmarkTestChGoSelect100MUint64(b *testing.B) {
	ctx := context.Background()
	c, err := ch.Dial(ctx, ch.Options{
		Password: "",
		Address:  "localhost:9000",
	})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	var (
		data proto.ColUInt64
	)
	for n := 0; n < b.N; n++ {

		if err := c.Do(ctx, ch.Query{
			Body: "SELECT number FROM system.numbers_mt LIMIT 100000000",
			OnProgress: func(ctx context.Context, p proto.Progress) error {
				return nil
			},
			OnResult: func(ctx context.Context, block proto.Block) error {
				return nil
			},
			Result: proto.Results{
				{Name: "number", Data: &data},
			},
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTestChGoSelect10MString(b *testing.B) {
	ctx := context.Background()
	c, err := ch.Dial(ctx, ch.Options{
		Password: "",
		Address:  "localhost:9000",
	})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	var (
		data proto.ColStr
	)
	var dataStr [][]byte
	for n := 0; n < b.N; n++ {

		if err := c.Do(ctx, ch.Query{
			Body: "SELECT toString(number) as string FROM system.numbers_mt LIMIT 10000000",
			OnProgress: func(ctx context.Context, p proto.Progress) error {
				return nil
			},
			OnResult: func(ctx context.Context, block proto.Block) error {
				dataStr = dataStr[:0]
				data.ForEachBytes(func(i int, b []byte) error {
					return nil
				})
				return nil
			},
			Result: proto.Results{
				{Name: "string", Data: &data},
			},
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTestChGoInsert10M(b *testing.B) {
	// return
	ctx := context.Background()
	c, err := ch.Dial(ctx, ch.Options{
		Password: "",
		Address:  "localhost:9000",
	})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	if err := c.Do(ctx, ch.Query{
		Body: "DROP TABLE IF EXISTS test_insert_ch_go",
	}); err != nil {
		b.Fatal(err)
	}
	if err := c.Do(ctx, ch.Query{
		Body: "CREATE TABLE test_insert_ch_go (id UInt64,v String) ENGINE = Null",
	}); err != nil {
		b.Fatal(err)
	}

	const (
		rowsInBlock = 10_000_000
	)

	var idColumns = make(proto.ColUInt64, 0, rowsInBlock)
	var vColumns proto.ColStr
	vColumns.Buf = make([]byte, 0, rowsInBlock*5)
	for n := 0; n < b.N; n++ {
		idColumns.Reset()
		vColumns.Reset()
		for i := 0; i < rowsInBlock; i++ {
			idColumns = append(idColumns, uint64(i))
			vColumns.AppendBytes([]byte("test"))
		}
		if err := c.Do(ctx, ch.Query{
			Body: "INSERT INTO test_insert_ch_go VALUES",
			OnInput: func(ctx context.Context) error {
				return io.EOF
			},
			Input: []proto.InputColumn{
				{Name: "id", Data: idColumns},
				{Name: "v", Data: vColumns},
			},
		}); err != nil {
			b.Fatal(err)
		}
	}
}
