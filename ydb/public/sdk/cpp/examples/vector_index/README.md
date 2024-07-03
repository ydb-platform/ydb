# Vector Index

## Examples

### Create

```
./vector_index -e some-enpoint -d some-database -c RecreateIndex --table=wikipedia --index_type=flat --index_quantizer=bit --primary_key=id --embedding=embedding --distance=CosineDistance --top_k=15 --rows=490000 --data=title --target=absolute-path-to-target.json
```

### Search

```
./vector_index -e some-enpoint -d some-database -c TopK --table=wikipedia --index_type=flat --index_quantizer=bit --primary_key=id --embedding=embedding --distance=CosineDistance --top_k=15 --rows=490000 --data=title --target=absolute-path-to-target.json
```
