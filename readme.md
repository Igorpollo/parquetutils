# parquetutils

## RUN

git clone

cd parquetutils

go install

To see all commands and flags

```
parquetutils help
```

Convert parquet to JSON
```
parquetutils --file "../file.parquet" -o "mytest"  tojson
```
List parquet file columns

```
parquetutils --file "../file.parquet" readcolumns
```
