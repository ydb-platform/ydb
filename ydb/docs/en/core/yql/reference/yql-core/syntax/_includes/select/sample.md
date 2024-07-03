# TABLESAMPLE and SAMPLE

{% include [olap_warning_note](../../../../../_includes/not_allow_for_olap_note.md) %}

Building a random sample from the data source specified in `FROM`.

`TABLESAMPLE` is part of the SQL standard and works as follows:

* The operating mode is specified:
  * `BERNOULLI` means "slowly, straightforwardly going through all the data, but in a truly random way".
  * `SYSTEM` uses knowledge about the physical data storage of data to avoid full data scans, but somewhat sacrificing randomness of the sample.
The data is split into sufficiently large blocks, and the whole data blocks are sampled. For applied calculations on sufficiently large tables, the result may well be consistent.
* The size of the random sample is indicated as a percentage after the operating mode, in parentheses.
* To manage the block size in the `SYSTEM` mode, use the `yt.SamplingIoBlockSize` pragma.
* Optionally, it can be followed by the `REPEATABLE` keyword and an integer in parentheses to be used as a seed for a pseudorandom number generator.

`SAMPLE` is a shorter alias without sophisticated settings and sample size specified as a fraction. It currently corresponds to the `BERNOULLI` mode.

{% note info %}

In the `BERNOULLI` mode, if the `REPEATABLE` keyword is added, the seed is mixed with the chunk ID for each chunk in the table. That's why sampling from different tables with the same content might produce different results.

{% endnote %}

**Examples:**

```yql
SELECT *
FROM my_table
TABLESAMPLE BERNOULLI(1.0) REPEATABLE(123); -- one percent of the table
```

```yql
SELECT *
FROM my_table
TABLESAMPLE SYSTEM(1.0); -- about one percent of the table
```

```yql
SELECT *
FROM my_table
SAMPLE 1.0 / 3; -- one-third of the table
```

