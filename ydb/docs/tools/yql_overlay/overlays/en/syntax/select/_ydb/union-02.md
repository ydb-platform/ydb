Two `UNION ALL` modes are supported: by column names (the default mode) and by column positions (corresponds to the ANSI SQL standard and is enabled by the [PRAGMA](../../pragma.md#positionalunionall)).

In the "by name" mode, the output of the resulting data schema uses the following rules:

* The resulting table includes all columns that were found in at least one of the input tables.
* If a column wasn't present in all the input tables, then it's automatically assigned the [optional data type](../../../types/optional.md) (that can accept `NULL`).
* If a column in different input tables had different types, then the shared type (the broadest one) is output.
* If a column in different input tables had a heterogeneous type, for example, string and numeric, an error is raised.



The order of output columns in this mode is equal to the largest common prefix of the order of inputs, followed by all other columns in the alphabetic order.
If the largest common prefix is empty (for example, if the order isn't specified for one of the inputs), then the output order is undefined.

In the "by position" mode, the output of the resulting data schema uses the following rules:

* All inputs must have equal number of columns
* The order of columns must be defined for all inputs
* The names of the resulting columns must match the names of columns in the first table
* The type of the resulting columns is output as a common (widest) type of input column types having the same positions

The order of the output columns in this mode is the same as the order of columns in the first input.

### Examples
