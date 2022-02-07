# Changelog

All versions are tagged by the major Postgres version, plus an individual semver for this library itself.

## 13-2.1.0   2021-10-12

* Normalize: add funcname error object [#121](https://github.com/pganalyze/libpg_query/pull/121)
* Normalize: Match GROUP BY against target list and re-use param refs [#124](https://github.com/pganalyze/libpg_query/pull/124)
* PL/pgSQL: Setup namespace items for parameters, support RECORD types [#123](https://github.com/pganalyze/libpg_query/pull/123)
  - This significantly improves parsing for PL/pgSQL functions, to the extent
    that most functions should now parse successfully


## 13-2.0.7   2021-07-16

* Normalize: Don't modify constants in TypeName typmods/arrayBounds fields (#118)
  - This matches how pg_stat_statement behaves, and avoids causing parsing
    errors on the normalized statement
* Don't fail builds on systems that have strchrnul support (FreeBSD)


## 13-2.0.6   2021-06-29

* Normalize: Don't touch "ORDER BY 1" expressions, keep original text [#115](https://github.com/pganalyze/libpg_query/pull/115)
  - This avoids obscuring the semantic meaning of integers in the ORDER BY
    clause, which is to reference a particular column in the target list.


## 13-2.0.5   2021-06-24

* Update to Postgres 13.3 patch release [#114](https://github.com/pganalyze/libpg_query/pull/114)
* Add optional Makefile target to build as shared library [#100](https://github.com/pganalyze/libpg_query/pull/100)
* Normalize: Don't touch "GROUP BY 1" type statements, keep original text [#113](https://github.com/pganalyze/libpg_query/pull/113)
  -  This avoids obscuring the semantic meaning of integers in the GROUP BY
    clause, which is to reference a particular column in the target list.
* Fingerprint: Cache list item hashes to fingerprint complex queries faster [#112](https://github.com/pganalyze/libpg_query/pull/112)
  - This was exhibiting quite bad runtime behaviour before, causing both an
    explosion in memory, as well as very high CPU runtime for complex queries.
  - The new approach ensures we don't calculate the hashes for a particular
    list more than once, which ensures that we roughly have quadratic runtime
    instead of exponential runtime.
* Deparser: Emit the RangeVar catalogname if present [#105](https://github.com/pganalyze/libpg_query/pull/105)
* Fix crash in pg_scan function when encountering backslash escapes [#109](https://github.com/pganalyze/libpg_query/pull/109)
* Integrate oss-fuzz fuzzer [#106](https://github.com/pganalyze/libpg_query/pull/106)


## 13-2.0.4   2021-04-05

* Deparser: Fix crash in CopyStmt with HEADER or FREEZE inside WITH parens
  - The parse tree does not contain an explicit argument in those cases,
    but does when specified in the legacy mode without the wrapping WITH.
  - With this change we only output the "1" argument when the original tree
    also had this, to ensure parse tree comparisons match. Note the intent
    here is technically the same, which is to enable these options.


## 13-2.0.3   2021-04-02

* Normalize: Fix handling of two subsequent DefElem elements [#96](https://github.com/pganalyze/libpg_query/pull/91)
  - We were incorrectly adding too many DefElem locations to the recorded
    constant values, causing a crash when more than a single DefElem
    is present in a utility statement.


## 13-2.0.2   2021-03-30

* srcdata/nodetypes.json: Avoid bogus values accidentally parsed from inside comments


## 13-2.0.1   2021-03-30

* Fix ARM builds: Avoid dependency on cpuid.h header
* Simplify deparser of TableLikeClause [#91](https://github.com/pganalyze/libpg_query/pull/91) [Lele Gaifax](https://github.com/lelit)
* Fix asprintf warnings by ensuring _GNU_SOURCE is set early enough


## 13-2.0.0   2021-03-18

* Update to PostgreSQL 13 parser (13.2 release)
* Changes to JSON output format
  - WARNING: These JSON format changes are incompatible with prior releases.
  - New top-level result object that contains the Postgres version number the
    parser is based on
  - Node type names are only output when the field is a generic field (Node*),
    but not when the field always has the same type. This matches how the
    Postgres source looks like, and ensures the JSON and (new) Protobuf format
    match in their structure. You can utilize the `srcdata/struct_defs.json`
    file as needed to get the necessary context on field types.
  - Whitespace between control characters in JSON is no longer added
  - "<" and ">" characters are escaped to avoid browser HTML injections
  - Enum values are output with the value's name, instead of the integer value
* Introduce new Protobuf parse tree output format
  - Up until now, this library relied on JSON to pass the parse result back
    to the caller, which has a number of downsides, most importantly that
    we don't have a readily available parser for JSON thats not tied to a
    running Postgres server. That in turn makes it hard to provide
    cross-language features such as deparsing directly in this library
    (which would require reading back a parse tree that gets passed in).
  - Protobuf isn't perfect, but its straightforward enough to generate the
    schema definitions for the parse tree nodes, and output the tree using
    a bundled C protobuf library, which has a small enough SLOC count (~3k)
    to not be noticeable in the big picture.
* Add support for returning Postgres scanner result
  - This allows utilizing pg_query for use cases that need the raw token
    information, instead of a parse tree. Due to additional modifications
    to the Postgres source, this also contains information about comments
    in the query string, and their location.
* Add deparsing functionality that turns parse tree back into a SQL query
  - This is based on the deparser that was written over multiple years for
    the pg_query Ruby library, and is now accessible for all bindings through
    this new API and implementation.
* Fingerprinting: Introduce v3 version and 64-bit XXH3 hash
  - See full details in the wiki page here: https://github.com/pganalyze/libpg_query/wiki/Fingerprinting#version-30-based-on-postgresql-13
* Add new pg_query_split_with_scanner/pg_query_split_with_parser functions to
  split up multi-statement strings
  - Naively one could assume that splitting a string by ";" is sufficient,
    but it becomes tricky once one takes into consideration that this
    character can also show up in identifier, constants or comments.
  - We provide both a parser-based split function and a scanner-based split
    function. Most importantly when splitting statements in a file that may
    contain syntax errors that cause a parser error, but are accepted by the
    scanner. Otherwise the parser-based split function is recommended
    due to better accuracy.
* Add experimental Protobuf C++ outfuncs, converge JSON output to match Protobuf
  mapped output
* Extract source with USE_ASSERT_CHECKING enabled
  - This ensures we have the necessary functions to compile an
    assert-enabled build if necessary. Note that this doesn't mean that
    asserts are enabled by default (they are not, you need to explicitly
    use DEBUG=1).
* Ensure codebase has a clean Valgrind run
* PL/pgSQL: Output NEW/OLD variable numbers, record dno fields [Ethan Resnick](https://github.com/ethanresnick)
* Makefile: Allow passing in customized CFLAGS/PG_CONFIGURE_FLAGS/TEST_* [Ethan Resnick](https://github.com/ethanresnick)


## 10-1.0.5   2021-02-17

* Update to latest Postgres 10 patch release (10.16)
* Free Postgres top-level memory context on thread exit / with function
  - Previously there was no way to free the top-level Postgres memory context,
    causing threaded programs that churn through a lot of threads to leak
    memory with each newly initialized thread-local top-level memory context.
  - Instead, this uses a newly introduced cleanup method to free the memory
    when a pthread exits (note this causes a pthread dependency to be added
    to this library). In addition, primarily for memory testing purposes, add a
    new method "pg_query_exit" that performs the same cleanup on demand.
* Resolve correctness issues and possible memory leak in PL/pgSQL parser
* Add arch-ppc.h for PPC architectures [#80](https://github.com/pganalyze/libpg_query/pull/80) [@pkubaj](https://github.com/pkubaj)


## 10-1.0.4   2020-12-27

* Update to latest Postgres 10 patch release (10.15)
* PL/pgSQL parsing: Handle asprintf failures (and prevent compiler warning)


## 10-1.0.3   2020-11-07

* Update to latest Postgres 10 patch release (10.14)
* Add support for ARM builds by explicitly copying ARM header file
* Ignore return value of asprintf without compiler warnings [@herwinw](https://github.com/herwinw)
* Free stderr_buffer when parsing plpgsql [@herwinw](https://github.com/herwinw)


## 10-1.0.2   2018-05-18

* Avoid compiler warning due to unused result in pg_query_parse_plpgsql


## 10-1.0.1   2017-10-31

* Fix fingerprint tests


## 10-1.0.0   2017-10-30

* First release based on PostgreSQL 10.0
  - Parse tree output may have changed in backwards-incompatible ways!
  - Fingerprint base version bumped to "02" to reflect the change in parse trees


## 9.5-1.7.1    2017-10-26

* Allow "$1 FROM $2" to be parsed
  - This is new with Postgres 10 output of pg_stat_statements, so we should
    treat this the same as "? FROM ?" in earlier versions.


## 9.5-1.7.0    2017-10-20

* Update to Postgres 9.5.9
* Support gcc versions earlier than 4.6.0
* Export version information in pg_query.h directly


## 9.5-1.6.2    2017-07-30

* Fingerprinting Version 1.3
  * Attributes to be ignored:
    * RangeVar.relname (if node also has RangeVar.relpersistence = "t")
  * Special cases: List nodes where parent field name is valuesLists
    * Follow same logic described for fromClause/targetList/cols/rexpr


## 9.5-1.6.1    2017-07-29

* Fingerprinting Version 1.2
  * Ignore portal_name in DeclareCursorStmt, FetchStmt and ClosePortalStmt


## 9.5-1.6.0    2017-07-29

* Change normalization methods to output $1/$2 .. $N instead of ? characters
  * BREAKING CHANGE in pg_query_normalize(..) output
  * This matches the change in the upcoming Postgres 10, and makes it easier
    to migrate applications to the new normalization format


## 9.5-1.5.0    2017-05-26

* Update to Postgres 9.5.7
* Disable 128-bit integer support (they are not used), to support 32-bit archs [@herwinw](https://github.com/herwinw)
* Cleanup efforts [@herwinw](https://github.com/herwinw)
  * Improve concurrency tests
  * Make sure we have a valid proc_source
  * Normalized whitespace in pg_query_parse_plpgsql
  * Move inclusion of stdio.h in plpgsql parser


## 9.5-1.4.2    2016-12-06

* Cut off fingerprints at 100 nodes deep to avoid excessive runtimes/memory
* Fix warning on Linux due to missing asprintf include


## 9.5-1.4.1    2016-06-26

* Automatically call pg_query_init as needed to ease threaded usage


## 9.5-1.4.0    2016-06-26

* Clean up includes to avoid dependency on stdbool.h and xlocale.h
* Change PL/pgSQL input to be the full CREATE FUNCTION statement
  * This is necessary for parsing, since we need the argument and return types
* Fingerprinting Version 1.1
  * Only ignore ResTarget.name when parent field name is targetList *and*
    we have a SelectStmt as a parent node (fixes UpdateStmt fingerprinting)
* Normalize the password in ALTER ROLE ... PASSWORD '123' statements
* Make library thread-safe through thread-local storage [#13](https://github.com/pganalyze/libpg_query/issues/13)


## 9.5-1.3.0    2016-05-31

* Extract source code using LLVM instead of manually compiling the right objects
  * This speeds up build times considerably since we don't download the Postgres
    source anymore, instead shipping a partial copy created as part of a release.
* Experimental support for parsing PL/pgSQL source code (output format subject to change)


## 9.5-1.2.1    2016-05-17

* Make sure we encode special characters correctly in JSON output ([@zhm](https://github.com/zhm) [#11](https://github.com/pganalyze/libpg_query/pull/11))


## 9.5-1.2.0    2016-05-16

* Fix stack overflow when parsing CREATE FOREIGN TABLE ([#9](https://github.com/pganalyze/libpg_query/issues/9))
* Update to PostgreSQL 9.5.3


## 9.5-1.1.0    2016-04-17

* Add pg_query_fingerprint() method that uniquely identifies SQL queries,
  whilst ignoring formatting and individual constant values
* Update to PostgreSQL 9.5.2


## 9.5-1.0.0    2016-03-06

* First release based on PostgreSQL 9.5.1
* Make JSON_OUTPUT_V2 the default and remove outfuncs patch
  * NOTE: This is a backwards incompatible change in the output parsetree format!


## 9.4-1.0.0    2016-03-06

* First tagged release based on PostgreSQL 9.4.5
