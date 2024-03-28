## Queries Generator

```
usage: gen_queries [-h] [--syntax SYNTAX] [--profile PROFILE] [--variant VARIANT] [--output OUTPUT] [--dataset-size DATASET_SIZE] [--pragma PRAGMA]
                   [--table-path-prefix TABLE_PATH_PREFIX] [--cluster-name CLUSTER_NAME]

options:
  -h, --help            show this help message and exit
  --syntax SYNTAX       syntax "pg" or "yql"
  --profile PROFILE     profile "dqrun" "dqrun_block" or "postgres"
  --variant VARIANT     variant "h" or "ds"
  --output OUTPUT       output directory
  --dataset-size DATASET_SIZE
                        dataset size (1, 10, 100, ...)
  --pragma PRAGMA       custom pragmas
  --table-path-prefix TABLE_PATH_PREFIX
                        table path prefix
  --cluster-name CLUSTER_NAME
                        YtSaurus cluster name
```

## Generate for dqrun
pg syntax:
```
./gen_queries --variant h --syntax pg
./gen_queries --variant ds --syntax pg
```

yql syntax:
```
./gen_queries --variant h --syntax yql
./gen_queries --variant ds --syntax yql
```

## Generate for PostgreSQL/Greenplum
```
./gen_queries --variant h --profile postgres --syntax pg
./gen_queries --variant ds --profile postgres --syntax pg
```

## Generate for YtSaurus
yql syntax
```
./gen_queries --variant h --syntax yql --profile ytsaurus
./gen_queries --variant ds --syntax yql --profile ytsaurus
```

pg syntax
```
./gen_queries --variant h --syntax pg --profile ytsaurus
./gen_queries --variant ds --syntax pg --profile ytsaurus
```

