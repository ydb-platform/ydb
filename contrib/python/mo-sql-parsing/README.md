# More SQL Parsing!

[![PyPI Latest Release](https://img.shields.io/pypi/v/mo-sql-parsing.svg)](https://pypi.org/project/mo-sql-parsing/)
[![Build Status](https://app.travis-ci.com/klahnakoski/mo-sql-parsing.svg?branch=master)](https://travis-ci.com/github/klahnakoski/mo-sql-parsing)
[![PyPI Downloads](https://static.pepy.tech/badge/mo-sql-parsing/month)](https://www.pepy.tech/projects/mo-sql-parsing)

Parse SQL into JSON so we can translate it for other datastores!

[See changes](https://github.com/klahnakoski/mo-sql-parsing#version-changes-features)

## Objective

The objective is to convert SQL queries to JSON-izable parse trees. This originally targeted MySQL, but has grown to include other database engines. *Please [paste some SQL into a new issue](https://github.com/klahnakoski/mo-sql-parsing/issues) if it does not work for you*


## Project Status

October 2024 -  I continue to resolve issues as they are raised. There are [over 1200 tests](https://app.travis-ci.com/github/klahnakoski/mo-sql-parsing), that cover most SQL for most databases, with limited DML and UDF support, including:

  * inner queries, 
  * with clauses, 
  * window functions
  * create/drop/alter tables and views
  * insert/update/delete statements
  * create procedure and function statements (MySQL only)


## Install

    pip install mo-sql-parsing

## Parsing SQL

    >>> from mo_sql_parsing import parse
    >>> parse("select count(1) from jobs")
    {'select': {'value': {'count': 1}}, 'from': 'jobs'}
    
Each SQL query is parsed to an object: Each clause is assigned to an object property of the same name. 

    >>> parse("select a as hello, b as world from jobs")
    {'select': [{'value': 'a', 'name': 'hello'}, {'value': 'b', 'name': 'world'}], 'from': 'jobs'}

The `SELECT` clause is an array of objects containing `name` and `value` properties. 


### SQL Flavours 

There are a few parsing modes you may be interested in:

#### Double-quotes for literal strings

MySQL uses both double quotes and single quotes to declare literal strings.  This is not ansi behaviour, but it is more forgiving for programmers coming from other languages. A specific parse function is provided: 

    result = parse_mysql(sql)

#### SQLServer Identifiers (`[]`)

SQLServer uses square brackets to delimit identifiers. For example

    SELECT [Timestamp] FROM [table]
    
which conflicts with BigQuery array constructor (eg `[1, 2, 3, 4]`). You may use the SqlServer flavour with 
    
    from mo_sql_parsing import parse_sqlserver as parse

#### NULL is None

The default output for this parser is to emit a null function `{"null":{}}` wherever `NULL` is encountered in the SQL.  If you would like something different, you can replace nulls with `None` (or anything else for that matter):

    result = parse(sql, null=None)
    
this has been implemented with a post-parse rewriting of the parse tree.


#### Normalized function call form

The default behaviour of the parser is to output function calls in `simple_op` format: The operator being a key in the object; `{op: params}`.  This form can be difficult to work with because the object must be scanned for known operators, or possible optional arguments, or at least distinguished from a query object.

You can have the parser emit function calls in `normal_op` format

    >>> from mo_sql_parsing import parse, normal_op
    >>> parse("select trim(' ' from b+c)", calls=normal_op)
    
which produces calls in a normalized format

    {"op": op, "args": args, "kwargs": kwargs}

here is the pretty-printed JSON from the example above:

```
{'select': {'value': {
    'op': 'trim', 
    'args': [{'op': 'add', 'args': ['b', 'c']}], 
    'kwargs': {'characters': {'literal': ' '}}
}}}
```


## Generating SQL

You may also generate SQL from a given JSON document. This is done by the formatter, which is usually lagging the parser (Dec2023).

    >>> from mo_sql_parsing import format
    >>> format({"from":"test", "select":["a.b", "c"]})
    'SELECT a.b, c FROM test'

## Contributing

In the event that the parser is not working for you, you can help make this better but simply pasting your sql (or JSON) into a new issue. Extra points if you describe the problem. Even more points if you submit a PR with a test.  If you also submit a fix, then you also have my gratitude. 


### Run Tests

See [the tests directory](https://github.com/klahnakoski/mo-sql-parsing/tree/dev/tests) for instructions running tests, or writing new ones.

## More about implementation

SQL queries are translated to JSON objects: Each clause is assigned to an object property of the same name.

    
    # SELECT * FROM dual WHERE a>b ORDER BY a+b
    {
        "select": {"all_columns": {}} 
        "from": "dual", 
        "where": {"gt": ["a", "b"]}, 
        "orderby": {"value": {"add": ["a", "b"]}}
    }
        
Expressions are also objects, but with only one property: The name of the operation, and the value holding (an array of) parameters for that operation. 

    {op: parameters}

and you can see this pattern in the previous example:

    {"gt": ["a","b"]}
    
## Array Programming

The `mo-sql-parsing.scrub()` method is used liberally throughout the code, and it "simplifies" the JSON.  You may find this form a bit tedious to work with because the JSON property values can be values, lists of values, or missing.  Please consider converting everything to arrays: 


```
def listwrap(value):
    if value is None:
        return []
    elif isinstance(value, list)
        return value
    else:
        return [value]
```  

then you may avoid all the is-it-a-list checks :

```
for select in listwrap(parsed_result.get('select')):
    do_something(select)
```

## Version Changes, Features

### Version 11

*October 2024*

The `PIVOT` clause has been promoted to top-level. Instead of being part of the joins found in the `FROM` clause, it is now a sibling to `SELECT`.

```
>>> from mo_sql_parsing import parse
>>> parse("SELECT * FROM table PIVOT (SUM(x) FOR y IN (1, 2, 3))")
```

now emits

```
{
    'select': {'all_columns': {}}, 
    'from': 'table', 
    'pivot': {'sum': 'x', 'for': 'y', 'in': [1, 2, 3]}
}
```

instead of

```
{
    'select': {'all_columns': {}}, 
    'from': [
        'table', 
        'pivot': {'sum': 'x', 'for': 'y', 'in': [1, 2, 3]}
    ]
}
```



### Version 10

*December 2023*

`SELECT *` now emits an `all_columns` call instead of plain star (`*`).  

```
>>> from mo_sql_parsing import parse
>>> parse("SELECT * FROM table")
{'select': {'all_columns': {}}, 'from': 'table'}
```

This works better with the `except` clause, and is more explicit when selecting all child properties.

``` 
>>> parse("SELECT a.* EXCEPT b FROM table")
>>> {"select": {"all_columns": "a", "except": "b"}, "from": "table"}
```

You may get the original behaviour by staying with version 9, or by using `all_columns="*"`:

```
>>> parse("SELECT * FROM table", all_columns="*")
{'select': "*", 'from': 'table'}
```


### Version 9

*November 2022*

Output for `COUNT(DISTINCT x)` has changed from function composition

    {"count": {"distinct": x}}

to named parameters

    {"count": x, "distinct": true}
     
This was part of a bug fix [issue142](https://github.com/klahnakoski/mo-sql-parsing/issues/142) - realizing `distinct` is just one parameter of many in an aggregate function. Specifically, using the `calls=normal_op` for clarity:
    
    >>> from mo_sql_parsing import parse, normal_op
    >>> parse("select count(distinct x)", calls=normal_op)
    
    {'select': {'value': {
        'op': 'count', 
        'args': [x], 
        'kwargs': {'distinct': True}
    }}}

### Version 8.200+

*September 2022*

* Added `ALTER TABLE` and `COPY` command parsing for Snowflake 


### Version 8
 
*November 2021*

* Prefer BigQuery `[]` (create array) over SQLServer `[]` (identity) 
* Added basic DML (`INSERT`/`UPDATE`/`DELETE`)              
* flatter `CREATE TABLE` structures. The `option` list in column definition has been flattened:<br>
    **Old column format**
    
        {"create table": {
            "columns": {
                "name": "name",
                "type": {"decimal": [2, 3]},
                "option": [
                    "not null",
                    "check": {"lt": [{"length": "name"}, 10]}
                ]
            }
        }}
        
    **New column format**
                
        {"create table": {
            "columns": {
                "name": "name", 
                "type": {"decimal": [2, 3]}
                "nullable": False,
                "check": {"lt": [{"length": "name"}, 10]} 
            }
        }}

### Version 7 

*October 2021*

* changed error reporting; still terrible
* upgraded mo-parsing library which forced version change

### Version 6 

*October 2021*

* fixed `SELECT DISTINCT` parsing
* added `DISTINCT ON` parsing

### Version 5 

*August 2021*

* remove inline module `mo-parsing`
* support `CREATE TABLE`, add SQL "flavours" emit `{null:{}}` for None

### Version 4

*November 2021*

* changed parse result of `SELECT DISTINCT`
* simpler `ORDER BY` clause in window functions
