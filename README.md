# Benchmark
([chconn](https://github.com/vahid-sohrabloo/chconn), [ch-go](github.com/ClickHouse/ch-go), [goclickhhouse](https://github.com/ClickHouse/clickhouse-go)), [uptrace](https://github.com/uptrace/go-clickhouse)

See their website if you are unfamiliar with ClickHouse:
[https://clickhouse.com/](https://clickhouse.com/)

Obviously, these tests are meant to help us decide and not to tell you which option is best for your project.

Using the following command, you can test the project on your computer
```
go test  -run=. -bench=. -benchtime=5x   -benchmem 

```

You  can also see [ch-bench](https://github.com/go-faster/ch-bench#benchmarks)



## Result
```
name \ time/op           chconn       chgo          go-clickhouse     uptrace
TestSelect100MUint64-16   150ms ± 0%    154ms ± 0%       8019ms ± 0%       3045ms ± 0%
TestSelect10MString-16    271ms ± 0%    447ms ± 0%        969ms ± 0%        822ms ± 0%
TestInsert10M-16          198ms ± 0%    514ms ± 0%        561ms ± 0%        304ms ± 0%

name \ alloc/op          chconn       chgo          go-clickhouse     uptrace
TestSelect100MUint64-16   111kB ± 0%    262kB ± 0%    3202443kB ± 0%     800941kB ± 0%
TestSelect10MString-16   1.63MB ± 0%   1.79MB ± 0%    1626.51MB ± 0%     241.03MB ± 0%
TestInsert10M-16         26.0MB ± 0%  283.7MB ± 0%     1680.4MB ± 0%      240.2MB ± 0%

name \ allocs/op         chconn       chgo          go-clickhouse     uptrace
TestSelect100MUint64-16    35.0 ± 0%   6683.0 ± 0%  200030937.0 ± 0%  100006069.0 ± 0%
TestSelect10MString-16     49.0 ± 0%   1748.0 ± 0%   30011991.0 ± 0%   20001120.0 ± 0%
TestInsert10M-16           26.0 ± 0%     80.0 ± 0%        224.0 ± 0%         50.0 ± 0%

```
