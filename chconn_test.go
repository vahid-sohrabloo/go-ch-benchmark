package gotests

import (
	"context"
	"testing"

	"github.com/vahid-sohrabloo/chconn/v2"
	"github.com/vahid-sohrabloo/chconn/v2/column"
)

func BenchmarkTestChconnSelect100MUint64(b *testing.B) {
	// return
	ctx := context.Background()
	c, err := chconn.Connect(ctx, "")
	if err != nil {
		b.Fatal(err)
	}
	colRead := column.New[uint64]()
	for n := 0; n < b.N; n++ {
		s, err := c.Select(ctx, "SELECT number FROM system.numbers_mt LIMIT 100000000", colRead)
		if err != nil {
			b.Fatal(err)
		}

		for s.Next() {
			_ = colRead.Data()
		}
		if err := s.Err(); err != nil {
			b.Fatal(err)
		}
		s.Close()
	}
}

func BenchmarkTestChconnSelect10MString(b *testing.B) {
	// return
	ctx := context.Background()
	c, err := chconn.Connect(ctx, "")
	if err != nil {
		b.Fatal(err)
	}
	// var datStr [][]byte
	colRead := column.NewString()
	for n := 0; n < b.N; n++ {
		s, err := c.Select(ctx, "SELECT toString(number) FROM system.numbers_mt LIMIT 10000000", colRead)
		if err != nil {
			b.Fatal(err)
		}

		for s.Next() {
			colRead.Each(func(i int, b []byte) bool {
				return true
			})
		}
		if err := s.Err(); err != nil {
			b.Fatal(err)
		}
		s.Close()
	}
}

func BenchmarkTestChconnInsert10M(b *testing.B) {
	// return
	ctx := context.Background()
	c, err := chconn.Connect(ctx, "")
	if err != nil {
		b.Fatal(err)
	}
	err = c.Exec(ctx, "DROP TABLE IF EXISTS test_insert_chconn")
	if err != nil {
		b.Fatal(err)
	}
	err = c.Exec(ctx, "CREATE TABLE test_insert_chconn (id UInt64,v String) ENGINE = Null")
	if err != nil {
		b.Fatal(err)
	}

	const (
		rowsInBlock = 10_000_000
	)

	idColumns := column.New[uint64]()
	vColumns := column.NewString()
	idColumns.SetWriteBufferSize(rowsInBlock)
	vColumns.SetWriteBufferSize(rowsInBlock * 5)
	for n := 0; n < b.N; n++ {
		for y := 0; y < rowsInBlock; y++ {
			idColumns.Append(uint64(y))
			vColumns.AppendBytes([]byte("test"))
		}
		err := c.Insert(ctx, "INSERT INTO test_insert_chconn VALUES", idColumns, vColumns)
		if err != nil {
			b.Fatal(err)
		}

	}
}
