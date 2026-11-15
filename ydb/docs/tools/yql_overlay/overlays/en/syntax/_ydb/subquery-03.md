### Examples

```yql
DEFINE SUBQUERY $sub($i) as
    SELECT $i as x;
END DEFINE;

$s = SubqueryExtendFor([1,2,3],$sub);
PROCESS $s();
```

## Adding sorting to the SubqueryOrderBy template or indicating the presence of this SubqueryAssumeOrderBy

The functions take the following arguments:

* A subquery template without parameters.
* A list of pairs (string indicating the column name and Boolean value: True for sorting in ascending order or False for sorting in descending order).

And they build a new query template without parameters where sorting is performed or a comment on the use of sorting is added to the result. To use the resulting query template, call the `PROCESS` function, since, when using a `SELECT`, sorting is ignored.

### Examples
