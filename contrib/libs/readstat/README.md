[![GitHub CI build status](https://github.com/WizardMac/ReadStat/workflows/build/badge.svg)](https://github.com/WizardMac/ReadStat/actions)
[![Appveyor build status](https://ci.appveyor.com/api/projects/status/76ctatpy3grlrd9x/branch/master?svg=true)](https://ci.appveyor.com/project/evanmiller/readstat/branch/master)
[![codecov](https://codecov.io/gh/WizardMac/ReadStat/branch/master/graph/badge.svg)](https://codecov.io/gh/WizardMac/ReadStat)
[![Fuzzing Status](https://oss-fuzz-build-logs.storage.googleapis.com/badges/readstat.svg)](https://bugs.chromium.org/p/oss-fuzz/issues/list?sort=-opened&can=1&q=proj:readstat)

ReadStat: Read (and write) data sets from SAS, Stata, and SPSS
==

Originally developed for [Wizard](https://www.wizardmac.com/), ReadStat is a
command-line tool and MIT-licensed C library for reading files from popular
stats packages. Supported data formats include:

* SAS: SAS7BDAT (binary file) and XPORT (transport file)
* Stata: DTA (binary file) versions 104-119
* SPSS: POR (portable file), SAV (binary file), and ZSAV (compressed binary)

Supported metadata formats include:

* SAS: SAS7BCAT (catalog file) and .sas (command file)
* Stata: .dct (dictionary file)
* SPSS: .sps (command file)

There is also write support for all the data formats, but not the metadata
formats. *The produced SAS7BDAT files still cannot be read by SAS*, but feel
free to contribute your binary-format expertise here.

For reading in R data files, please see the related
[librdata](https://github.com/WizardMac/librdata) project.

Installation on Unix / macOS
--

Grab the latest [release](https://github.com/WizardMac/ReadStat/releases) and
then proceed as usual:

    ./configure
    make
    sudo make install

If you're cloning the repository, first make sure you have autotools installed,
and then run `./autogen.sh` to generate the configure file.

If you're on Mac and see errors about `AM_ICONV` when you run `./autogen.sh`,
you'll need to install [gettext](https://www.gnu.org/software/gettext/).

Installation on Windows
--

ReadStat now includes a Microsoft Visual Studio project file that includes
build targets for the library and tests. See the [VS17](./VS17) folder in
the downloaded release for a "one-click" Windows build.

Alternatively, you can build ReadStat on the command line using an
[msys2](https://msys2.github.io/) environment. After installing msys2,
download some other packages:

    pacman -S autoconf automake libtool make mingw-w64-x86_64-toolchain mingw-w64-x86_64-cmake mingw-w64-x86_64-libiconv

Then start a MINGW command line (not the msys2 prompt!) and follow the UNIX
install instructions above for this package.

Language Bindings
--

* Julia: [ReadStat.jl](https://github.com/queryverse/ReadStat.jl)
* Perl 6: [ReadStat.pm6](https://github.com/WizardMac/ReadStat.pm6)
* Python: [pyreadstat](https://github.com/Roche/pyreadstat)
* R: [haven](https://github.com/tidyverse/haven)


Docker
--

A dockerized version is available [here](https://github.com/jbn/readstat)



Command-line Usage
--

Standard usage:

    readstat [-f] <input file> <output file>

Where:

* `<input file>` ends with `.dta`, `.por`, `.sav`, `.sas7bdat`, or `.xpt`and
* `<output file>` ends with `.dta`, `.por`, `.sav`, `.sas7bdat`, `.xpt` or `.csv`

If [libxlsxwriter](http://libxlsxwriter.github.io) is found at compile-time, an
XLSX file (ending in `.xlsx`) can be written instead.

If zlib is found at compile-time, compressed SPSS files (`.zsav`) can be read
and written as well.

Use the `-f` option to overwrite an existing output file.

If you have a plain-text file described by a Stata dictionary file, a SAS
command file, or an SPSS command file, a second invocation style is supported:

    readstat <input file> <dictionary file> <output file>

Where:

* `<input file>` can be anything
* `<dictionary file>` ends with `.dct`, `.sas`, or `.sps`
* `<output file>` ends with `.dta`, `.por`, `.sav`, `.xpt`, or `.csv`

If you have a SAS catalog file containing the data set's value labels, you
can use the same invocation:

    readstat <input file> <catalog file> <output file>

Except where:

* `<input file>` ends with `.sas7bdat`
* `<catalog file>` ends with `.sas7bcat`
* `<output file>` ends with `.dta`, `.por`, `.sav`, `.xpt`, or `.csv`

If the file conversion succeeds, ReadStat will report the number of rows and
variables converted, e.g.

    Converted 111 variables and 160851 rows in 12.36 seconds

At the moment value labels are supported, but the finer nuances of converting
format strings (e.g. `%8.2g`) are not.

Command-line Usage with CSV input
--

A prerequisite for CSV input is that the [libcsv](https://github.com/rgamble/libcsv)
library is found at compile time.

CSV input is supported together with a metadata file describing the data:

    readstat <input file.csv> <input metadata.json> <output file>

The `<output file>` should end with `.dta`, `.sav`, or `.csv`.

The `<input file.csv>` is a regular CSV file.

The `<input metadata.json>` is a JSON file describing column types, value
labels and missing values. The easiest way to create such a metadata file is to
use the provided `extract_metadata` program on an existing file:

    $ extract_metadata <input file.(dta|sav|sas7bcat)>

The schema of this JSON file is fully described in
[variablemetadata_schema.json](variablemetadata_schema.json) using [JSON
Schema](http://json-schema.org/).

The following is an example of a valid metadata file:

    {
        "type": "SPSS",
        "variables": [
            {
                "type": "NUMERIC",
                "name": "citizenship",
                "label": "Citizenship of respondent",
                "categories": [
                    {
                        "code": 1,
                        "label": "Afghanistan"
                    },
                    {
                        "code": 2,
                        "label": "Albania"
                    },
                    {
                        "code": 98,
                        "label": "No answer"
                    },
                    {
                        "code": 99,
                        "label": "Not applicable"
                    }
                ],
                "missing": {
                    "type": "DISCRETE",
                    "values": [
                        98,
                        99
                    ]
                }
            }
        ]
    }

Here the column `citizenship` is a numeric column with four possible values `1`, `2`, `98`, and `99`.
`1` has the label `Afghanistan`, `2` has `Albania`, `98` has `No answer` and `99` has `Not applicable`.
`98` and `99` are defined as missing values.

Other column types are `STRING` and `DATE`. 
All values in `DATE` columns are expected to conform to [ISO 8601 date](https://en.wikipedia.org/wiki/ISO_8601).
Here is an example of `DATE` metadata:

    {
        "type": "SPSS",
        "variables": [
            {
                "type": "DATE",
                "name": "startdate",
                "label": "Start date",
                "categories": [
                    {
                        "code": "6666-01-01",
                        "label": "no date available"
                    }
                ],
                "missing": {
                    "type": "DISCRETE",
                    "values": [
                        "6666-01-01",
                        "9999-01-01"
                    ]
                }
            }
        ]
    }

Value labels are supported for `DATE`.

The last column type is `STRING`:

    {
        "type": "SPSS",
        "variables": [
            {
                "type": "STRING",
                "name": "somestring",
                "label": "Label of column",
                "missing": {
                    "type": "DISCRETE",
                    "values": [
                        "NA",
                        "N/A"
                    ]
                }
            }
        ]
    }

Value labels are not supported for `STRING`.

Library Usage: Reading Files
--

The ReadStat API is callback-based. It uses very little memory, and is suitable
for programs with progress bars.  ReadStat uses
[iconv](https://en.wikipedia.org/wiki/Iconv) to automatically transcode
text data into UTF-8, so you don't have to worry about character encodings. 

See src/readstat.h for the complete API. In general you'll provide a filename
and a set of optional callback functions for handling various information and
data found in the file. It's up to the user to store this information in an
appropriate data structure. If a context pointer is passed to the parse_* functions,
it will be made available to the various callback functions.

Callback functions should return `READSTAT_HANDLER_OK` (zero) on success.
Returning `READSTAT_HANDLER_ABORT` will abort the parsing process.

Example: Return the number of records in a DTA file.

```c
#include "readstat.h"

int handle_metadata(readstat_metadata_t *metadata, void *ctx) {
    int *my_count = (int *)ctx;

    *my_count = readstat_get_row_count(metadata);

    return READSTAT_HANDLER_OK;
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        printf("Usage: %s <filename>\n", argv[0]);
        return 1;
    }
    int my_count = 0;
    readstat_error_t error = READSTAT_OK;
    readstat_parser_t *parser = readstat_parser_init();
    readstat_set_metadata_handler(parser, &handle_metadata);

    error = readstat_parse_dta(parser, argv[1], &my_count);

    readstat_parser_free(parser);

    if (error != READSTAT_OK) {
        printf("Error processing %s: %d\n", argv[1], error);
        return 1;
    }
    printf("Found %d records\n", my_count);
    return 0;
}
```

Example: Convert a DTA to a tab-separated file.

```c
#include "readstat.h"

int handle_metadata(readstat_metadata_t *metadata, void *ctx) {
    int *my_var_count = (int *)ctx;
    
    *my_var_count = readstat_get_var_count(metadata);

    return READSTAT_HANDLER_OK;
}

int handle_variable(int index, readstat_variable_t *variable, 
    const char *val_labels, void *ctx) {
    int *my_var_count = (int *)ctx;

    printf("%s", readstat_variable_get_name(variable));
    if (index == *my_var_count - 1) {
        printf("\n");
    } else {
        printf("\t");
    }

    return READSTAT_HANDLER_OK;
}

int handle_value(int obs_index, readstat_variable_t *variable, readstat_value_t value, void *ctx) {
    int *my_var_count = (int *)ctx;
    int var_index = readstat_variable_get_index(variable);
    readstat_type_t type = readstat_value_type(value);
    if (!readstat_value_is_system_missing(value)) {
        if (type == READSTAT_TYPE_STRING) {
            printf("%s", readstat_string_value(value));
        } else if (type == READSTAT_TYPE_INT8) {
            printf("%hhd", readstat_int8_value(value));
        } else if (type == READSTAT_TYPE_INT16) {
            printf("%hd", readstat_int16_value(value));
        } else if (type == READSTAT_TYPE_INT32) {
            printf("%d", readstat_int32_value(value));
        } else if (type == READSTAT_TYPE_FLOAT) {
            printf("%f", readstat_float_value(value));
        } else if (type == READSTAT_TYPE_DOUBLE) {
            printf("%lf", readstat_double_value(value));
        }
    }
    if (var_index == *my_var_count - 1) {
        printf("\n");
    } else {
        printf("\t");
    }

    return READSTAT_HANDLER_OK;
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        printf("Usage: %s <filename>\n", argv[0]);
        return 1;
    }
    int my_var_count = 0;
    readstat_error_t error = READSTAT_OK;
    readstat_parser_t *parser = readstat_parser_init();
    readstat_set_metadata_handler(parser, &handle_metadata);
    readstat_set_variable_handler(parser, &handle_variable);
    readstat_set_value_handler(parser, &handle_value);

    error = readstat_parse_dta(parser, argv[1], &my_var_count);

    readstat_parser_free(parser);

    if (error != READSTAT_OK) {
        printf("Error processing %s: %d\n", argv[1], error);
        return 1;
    }
    return 0;
}
```

Library Usage: Writing Files
--

ReadStat can write data sets to a number of file formats, and uses largely the
same API for each of them. Files are written incrementally, with the header
written first, followed by individual rows of data, and ending with some kind
of trailer. (So the full data file never resides in memory.) Unlike like the
callback-based API for reading files, the writer API consists of function that
the developer must call in a particular order. The complete API can be found in
[readstat.h](./src/readstat.h).

Basic usage:

```c
#include "readstat.h"

/* A callback for writing bytes to your file descriptor of choice */
/* The ctx argument comes from the readstat_begin_writing_xxx function */
static ssize_t write_bytes(const void *data, size_t len, void *ctx) {
    int fd = *(int *)ctx;
    return write(fd, data, len);
}

int main(int argc, char *argv[]) {
    readstat_writer_t *writer = readstat_writer_init();
    readstat_set_data_writer(writer, &write_bytes);
    readstat_writer_set_file_label(writer, "My data set");

    int row_count = 1;

    readstat_variable_t *variable = readstat_add_variable(writer, "Var1", READSTAT_TYPE_DOUBLE, 0);
    readstat_variable_set_label(variable, "First variable");

    /* Call one of:
     *   readstat_begin_writing_dta
     *   readstat_begin_writing_por
     *   readstat_begin_writing_sas7bdat
     *   readstat_begin_writing_sav
     *   readstat_begin_writing_xport
     */

    int fd = open("something.dta", O_CREAT | O_WRONLY);
    readstat_begin_writing_dta(writer, &fd, row_count);

    int i;
    for (i=0; i<row_count; i++) {
        readstat_begin_row(writer);
        readstat_insert_double_value(writer, variable, 1.0 * i);
        readstat_end_row(writer);
    }

    readstat_end_writing(writer);
    readstat_writer_free(writer);
    close(fd);

    return 0;
}
```

Fuzz Testing
--

To assist in fuzz testing, ReadStat ships with target files designed to work
with [libFuzzer](http://llvm.org/docs/LibFuzzer.html). Clang 6 or later is
required.

1. `./configure --enable-fuzz-testing` turns on useful sanitizer and sanitizer-coverage flags
1. `make` will create a new binary called `generate_corpus`. Running this
   program will use the ReadStat test suite to create a corpus of test files in
   `corpus/`. There is a subdirectory for each sub-format (`dta104`, `dta105`,
   etc.). Currently a total of 468 files are created.
1. If fuzz-testing has been enabled, `make` will also create fourteen fuzzer
   targets, one for each of seven file formats, six for internally used
   grammars, and two fuzzers for testing the compression routines.
   * `fuzz_format_dta`
   * `fuzz_format_por`
   * `fuzz_format_sas7bcat`
   * `fuzz_format_sas7bdat`
   * `fuzz_format_sav`
   * `fuzz_format_xport`
   * `fuzz_format_stata_dictionary`
   * `fuzz_grammar_dta_timestamp`
   * `fuzz_grammar_por_double`
   * `fuzz_grammar_sav_date`
   * `fuzz_grammar_sav_time`
   * `fuzz_grammar_spss_format`
   * `fuzz_grammar_xport_format`
   * `fuzz_compression_sas_rle`
   * `fuzz_compression_sav`

For best results, each sub-directory of the corpus should be passed to the relevant fuzzer, e.g.:

* `./fuzz_format_dta corpus/dta104`
* `./fuzz_format_dta corpus/dta110`
* ...
* `./fuzz_format_sav corpus/sav`
* `./fuzz_format_sav corpus/zsav`
* `./fuzz_format_xport corpus/xpt5`
* `./fuzz_format_xport corpus/xpt8`

Finally, the compression fuzzers can be invoked without a corpus:

* `./fuzz_compression_sas_rle`
* `./fuzz_compression_sav`


