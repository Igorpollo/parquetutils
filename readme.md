# parquetutils

## RUN
```
git clone https://github.com/Igorpollo/parquetutils

cd parquetutils

go install

parquetutils help
```

## HOW TO USE


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
