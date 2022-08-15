package gotests

import (
	"context"
	"testing"
	"time"

	"github.com/uptrace/go-clickhouse/ch"
)

func BenchmarkTestUptraceSelect100MUint64(b *testing.B) {
	db := ch.Connect(
		ch.WithCompression(false),
		ch.WithTimeout(time.Second*30),
	)
	err := db.Ping(context.Background())
	if err != nil {
		panic(err)
	}

	for n := 0; n < b.N; n++ {
		rows, err := db.QueryContext(context.Background(), "SELECT number FROM system.numbers_mt LIMIT 100000000")
		if err != nil {
			b.Fatal(err)
		}
		for rows.Next() {
			var value uint64
			if err := rows.Scan(&value); err != nil {
				b.Fatal(err)
			}
		}
	}
}
func BenchmarkTestUptraceSelect10MString(b *testing.B) {
	db := ch.Connect(
		ch.WithCompression(false),
		ch.WithTimeout(time.Second*30),
	)
	err := db.Ping(context.Background())
	if err != nil {
		panic(err)
	}

	for n := 0; n < b.N; n++ {
		rows, err := db.QueryContext(context.Background(), "SELECT toString(number) FROM system.numbers_mt LIMIT 10000000")
		if err != nil {
			b.Fatal(err)
		}
		for rows.Next() {
			var value string
			if err := rows.Scan(&value); err != nil {
				b.Fatal(err)
			}
		}
	}
}

type Model struct {
	ch.CHModel `ch:",columnar,engine:Null()"`

	Col1 []uint64
	Col2 []string
}

func BenchmarkTestUptraceInsert10M(b *testing.B) {
	db := ch.Connect(
		ch.WithCompression(false),
		ch.WithTimeout(time.Second*30),
	)
	ctx := context.Background()
	err := db.Ping(ctx)
	if err != nil {
		panic(err)
	}

	if err := db.ResetModel(ctx, (*Model)(nil)); err != nil {
		panic(err)
	}

	const (
		rowsInBlock = 10_000_000
	)
	for n := 0; n < b.N; n++ {
		model := Model{
			Col1: make([]uint64, 0, rowsInBlock),
			Col2: make([]string, 0, rowsInBlock),
		}
		for i := 0; i < rowsInBlock; i++ {
			model.Col1 = append(model.Col1, uint64(i))
			model.Col2 = append(model.Col2, "test")
		}
		_, err := db.NewInsert().Model(&model).Exec(ctx)
		if err != nil {
			panic(err)
		}
	}
}
