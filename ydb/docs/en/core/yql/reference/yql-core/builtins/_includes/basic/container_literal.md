## Container literals {#containerliteral}

Some containers support operator notation for their literal values:

* Tuple: `(value1, value2...)`;
* Structure: `<|name1: value1, name2: value2...|>`;
* List: `[value1, value2,...]`;
* Dictionary: `{key1: value1, key2: value2...}`;
* Set: `{key1, key2...}`.

In every case, you can use an insignificant trailing comma. For a tuple with one element, this comma is required: ```(value1,)```.
For field names in the structure literal, you can use an expression that can be calculated at evaluation time, for example, string literals or identifiers (including those enclosed in backticks).

For nested lists, use [AsList](#aslist), for nested dictionaries, use [AsDict](#asdict), for nested sets, use [AsSet](#asset), for nested tuples, use [AsTuple](#astuple), for nested structures, use [AsStruct](#asstruct).

**Examples**

```yql
$name = "computed " || "member name";
SELECT
  (1, 2, "3") AS `tuple`,
  <|
    `complex member name`: 2.3,
    b: 2,
    $name: "3",
    "inline " || "computed member name": false
  |> AS `struct`,
  [1, 2, 3] AS `list`,
  {
    "a": 1,
    "b": 2,
    "c": 3,
  } AS `dict`,
  {1, 2, 3} AS `set`
```

