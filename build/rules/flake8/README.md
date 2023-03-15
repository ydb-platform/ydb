#Flake8 migrations

Don't edit migrations.yaml!

Style tests are checked every night, and if there are no more errors of the specified type for the specified prefix, then it is automatically excluded from the migration file.

##migrations.yaml
Format:
```
migrations:
  plugin-1:
    ignore:
      - B102
      - S103
      - F401
    prefixes:
      - devtools/ya
      - ads
      - quality
  ignore-F123:
    ignore:
      - F123
    prefixes:
      - devtools/ya
      - devtools/d
```
If arcadia-relative filepath startswith prefix from prefixes, then:

1. ignore values will be added to flake8.conf ignore section

