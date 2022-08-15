package gotests

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func BenchmarkTestGoClickhouseSelect100MUint64(b *testing.B) {
	c, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"127.0.0.1:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
	})
	if err != nil {
		b.Fatal(err)
	}

	for n := 0; n < b.N; n++ {
		rows, err := c.Query(context.Background(), "SELECT number FROM system.numbers_mt LIMIT 100000000")
		if err != nil {
			b.Fatal(err)
		}
		var count int
		for rows.Next() {
			var value uint64
			if err := rows.Scan(&value); err != nil {
				b.Fatal(err)
			}
			count++
		}
	}
}
func BenchmarkTestGoClickhouseSelect10MString(b *testing.B) {
	c, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"127.0.0.1:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
	})
	if err != nil {
		b.Fatal(err)
	}

	for n := 0; n < b.N; n++ {
		rows, err := c.Query(context.Background(), "SELECT toString(number) FROM system.numbers_mt LIMIT 10000000")
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

func BenchmarkTestGoClickhouseInsert10M(b *testing.B) {
	c, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"127.0.0.1:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
	})
	if err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()

	err = c.Exec(ctx, `
		DROP TABLE IF EXISTS test_insert_go_goClickhouse
	`)
	if err != nil {
		b.Fatal(err)
	}
	err = c.Exec(ctx, `
			CREATE TABLE test_insert_go_goClickhouse (id UInt64,v String) ENGINE = Null
	`)
	if err != nil {
		b.Fatal(err)
	}

	const (
		rowsInBlock = 10_000_000
	)
	var (
		col1 = make([]uint64, 0, rowsInBlock)
		col2 = make([]string, 0, rowsInBlock)
	)
	for n := 0; n < b.N; n++ {
		col1 = col1[:0]
		col2 = col2[:0]
		for i := 0; i < rowsInBlock; i++ {
			col1 = append(col1, uint64(i))
			col2 = append(col2, "test")
		}
		batch, err := c.PrepareBatch(ctx, "INSERT INTO test_insert_go_goClickhouse VALUES")
		if err != nil {
			b.Fatal(err)
		}
		if err := batch.Column(0).Append(col1); err != nil {
			b.Fatal(err)
		}
		if err := batch.Column(1).Append(col2); err != nil {
			b.Fatal(err)
		}

		if err = batch.Send(); err != nil {
			b.Fatal(err)
		}
	}
}
