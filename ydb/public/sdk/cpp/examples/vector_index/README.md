# Vector Index


Parameters description

```
./vector_index --help
Usage: ./vector_index [OPTIONS] [ARG]...

Required parameters:
  {-e|--endpoint} HOST:PORT YDB endpoint
  {-d|--database} PATH      YDB database
  {-c|--command} COMMAND    execute command: [Create|Drop|Build|Recreate]Index
                            or TopK (used for search, read request)
  --table TABLE             table name
  --index_type TYPE         index type: [flat]
  --index_quantizer QUANTIZER
                            index quantizer: [none|int8|uint8|bit]
  --primary_key PK          primary key column
  --embedding EMBEDDING     embedding (vector) column
  --distance DISTANCE       distance function:
                            [Cosine|Euclidean|Manhattan]Distance
  --rows ROWS               count of rows in table, used only for
                            [Build|Recreate]Index commands
  --top_k TOPK              count of rows in top, used only for TopK command
  --data DATA               list of columns to read, used only for TopK command
  --target TARGET           file with target vector, used only for TopK command
```

RecreateIndex -- command which sequentially executes Drop, Create and Build commands

Index table will have name like `<table>_<index_type>_<index_quantizer>`

## Examples

### Flat Index

It uses scalar quantization to speedup ANN search:
* an approximate search is performed using quantization
* an approximate list of primary keys is obtained
* we search this list without using quantization

#### Create Flat Bit Index

It creates index table, with two columns: `primary_key` and `embedding`, `embedding` will be trasformed from `<table>` `<embedding>`

```
./vector_index --endpoint=<endpoint> --database=<database> --command=RecreateIndex --table=<table> --index_type=flat --index_quantizer=bit --primary_key=<primary_key> --embedding=<embedding> --distance=CosineDistance --rows=490000 --top_k=0 --data="" --target=""
```

#### Search Flat Bit Index

Execute query like this, to get approximate candidates

```sql
$candidates = SELECT primary_key 
FROM index_table 
ORDER BY CosineDistance(target, embedding)
LIMIT top_k * 2
```

And then execute and print

```sql
SELECT CosineDistance(target, embedding) AS distance -- <list of columns from data parameter>
FROM table
WHERE primary_key IN $candidates
ORDER BY distance
LIMIT top_k
```

```
./vector_index --endpoint=<endpoint> --database=<database> --command=RecreateIndex --table=<table> --index_type=flat --index_quantizer=bit --primary_key=<primary_key> --embedding=<embedding> --distance=CosineDistance --rows=0 --top_k=15 --data=<list of string columns, e.g. "title, text"> --target=<absolute-path-to-target.json>
```
