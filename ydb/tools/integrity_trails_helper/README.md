# Integrity Trails Helper

Tool for generating obfuscated key values for searching in integrity trail logs.

## Usage

Provide the tablet schema in JSON format and primary key values (in the declared order).

### Example

1. Getting schema:
```
ydb -e <endpoint> -d <database> scheme describe <table_name> --format proto-json-base64 > scheme.json
```

2. For a table with two columns in the primary key:
```json
...
  "primary_key": [
    "id",
    "num"
  ],
...
```

3. Generate the obfuscated key for `id=123` and `num=456`:
```
./integrity_trails_helper scheme.json 123 456

Obfuscated key: 
b4fVk9JajAVi2TKtyc31h8mAObfSRDi7MoyLg1tpnas=
```

The output can be used to search for this specific key in integrity trail logs.