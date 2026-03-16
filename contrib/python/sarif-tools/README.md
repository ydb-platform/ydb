# SARIF Tools

A set of command line tools and Python library for working with SARIF files.

Read more about the SARIF format here:
[sarifweb.azurewebsites.net](https://sarifweb.azurewebsites.net/).

## Installation

### Prerequisites

You need Python 3.8 or later installed.  Get it from [python.org](https://www.python.org/downloads/).
This document assumes that the `python` command runs that version.

### Installing on Windows

Open a user command prompt and type:

```cmd
pip install sarif-tools
```

Check for a warning such as the following:

```log
WARNING: The script sarif.exe is installed in 'C:\tools\Python38\Scripts' which is not on PATH.
```

Go into Windows Settings and search for "env" (Edit environment variables for your account) and
add the missing path to your PATH variable.  You'll need to open a new terminal or reboot, and
then you can type `sarif --version` at the command prompt.

To install system-wide for all users, use an Administrator command prompt instead, if you are
comfortable with the security risks.

### Installing on Linux or Mac

```bash
pip install sarif-tools
```

Check for a warning such as the following:

```log
WARNING: The script sarif is installed in '/home/XYZ/.local/bin' which is not on PATH.
```

Add the missing path to your PATH.  How to do that varies by Linux flavour, but editing `~/.profile`
is often a good approach.  Then after opening a new terminal or running `source ~/.profile`, you
should be able to type `sarif --version` at the command prompt.

To install system-wide, use `sudo pip install`.  Be aware that this is discouraged from a
security perspective.

### Testing the installation

After installing using `pip`, you should then be able to run:

```bash
sarif --version
```

### Troubleshooting installation

This section has suggestions in case the `sarif` command is not available after installation.

A launcher called `sarif` or `sarif.exe` is created in Python's `Scripts` directory.  The `Scripts`
directory needs to be in the `PATH` environment variable for you to be able to type `sarif` at the
command prompt; this is most likely the case if `pip` is run as a super-user when installing (e.g.
Administrator Command Prompt on Windows, or using `sudo` on Linux).
If the SARIF tools are installed for the current user only, adding the user's Scripts directory to
the current user's PATH variable is the best approach.  Search online for how to do that on your
system.

If the `Scripts` directory is not in the `PATH`, then you can type `python -m sarif` instead of
`sarif` to run the tool.

Confusion can arise when the `python` and `pip` commands on the `PATH` are from different
installations, or the `python` installation on the super-user's `PATH` is different from the
`python` command on the normal user's path.  On Windows, you can use `where python` and `where pip`
in normal CMD and Admin CMD to see which installations are in use; on Linux, it's `which python` and
`which pip` with and without `sudo`.

## File format variations

SARIF is a standard format for the output of static analysis tools, but as always, different tools
use the standard in different ways.  The key parts of the spec that the SARIF Tools look at are:

- Severity: Only `error`, `warning`, `note` and `none` are allowed by the SARIF standard.
- Code and message: Issue types should have a short code and a short name, called "message" in the
  standard and usually called "description" in Sarif-Tools.
- Location: A specific location where the issue occurred, normally a file path and a line number.

Static analysis tools often have different ideas from the SARIF standard about how to represent
these pieces of data, and tool authors make different levels of effort to map their output to the
SARIF standard.  Examples we have seen are:

- Only using one SARIF severity level, and putting the "real" severity level into a custom property.
- Including the location information in the message, so that the same issue in different locations
  has a different message.
- Representing the location in different ways.

The Sarif-Tools code applies some minor fudging to produce decent results for mildly-divergent
tools.  For example, issue types are identified by a combination of code and truncated message,
either of which is allowed to be absent.  However, if Sarif-Tools is producing bad results for a
specific tool, you may need to implement custom pre-processing of the SARIF output, or ask the
tool authors to improve the quality of their tool's SARIF output to better align to the standard,
or [raise an issue against Sarif-Tools](https://github.com/microsoft/sarif-tools/issues).

## Command Line Usage

```plain
usage: sarif [-h] [--version] [--debug] [--check {error,warning,note}] {blame,codeclimate,copy,csv,diff,emacs,html,info,ls,summary,trend,usage,word} ...

Process sets of SARIF files

positional arguments:
  {blame,codeclimate,copy,csv,diff,emacs,html,info,ls,summary,trend,usage,word}
                        command

optional arguments:
  -h, --help            show this help message and exit
  --version, -v         show program's version number and exit
  --debug               Print information useful for debugging
  --check {error,warning,note}, -x {error,warning,note}
                        Exit with error code if there are any issues of the specified level (or for diff, an increase in issues at that level).

commands:
blame        Enhance SARIF file with information from `git blame`
codeclimate  Write a JSON representation in Code Climate format of SARIF file(s) for viewing as a Code Quality report in GitLab UI
copy         Write a new SARIF file containing optionally-filtered data from other SARIF file(s)
csv          Write a CSV file listing the issues from the SARIF files(s) specified
diff         Find the difference between two [sets of] SARIF files
emacs        Write a representation of SARIF file(s) for viewing in emacs
html         Write an HTML representation of SARIF file(s) for viewing in a web browser
info         Print information about SARIF file(s) structure
ls           List all SARIF files in the directories specified
summary      Write a text summary with the counts of issues from the SARIF files(s) specified
trend        Write a CSV file with time series data from SARIF files with "yyyymmddThhmmssZ" timestamps in their filenames
usage        (Command optional) - print usage and exit
word         Produce MS Word .docx summaries of the SARIF files specified
Run `sarif <COMMAND> --help` for command-specific help.
```

### Commands

The commands are illustrated below assuming input files in the following locations:

- `C:\temp\sarif_files` = a directory of SARIF files with arbitrary filenames.
- `C:\temp\sarif_with_date` = a directory of SARIF files with filenames including timestamps e.g. `C:\temp\sarif_with_date\myapp_devskim_output_20211001T012000Z.sarif`.
- `C:\temp\old_sarif_files` = a directory of SARIF files with arbitrary filenames from an older build.
- `C:\code\my_source_repo` = checkout directory of source code files from which SARIF results were obtained.

#### blame

```plain
usage: sarif blame [-h] [--output PATH] [--code PATH] [file_or_dir [file_or_dir ...]]

Enhance SARIF file with information from `git blame`

positional arguments:
  file_or_dir           A SARIF file or a directory containing SARIF files

optional arguments:
  -h, --help            show this help message and exit
  --output PATH, -o PATH
                        Output file or directory
  --code PATH, -c PATH  Path to git repository; if not specified, the current working directory is used
```

Augment SARIF files with `git blame` information, and write the augmented files to a specified location.

```shell
sarif blame -o "C:\temp\sarif_files_with_blame_info" -c "C:\code\my_source_repo" "C:\temp\sarif_files"
```

If the current working directory is the git repository, the `-c` argument can be omitted.

Blame information is added to the property bag of each `result` object for which it was successfully obtained.  The keys and values used are as in the [git blame porcelain format](https://git-scm.com/docs/git-blame#_the_porcelain_format).  E.g.:

```json
{
  "ruleId": "SM00702",
  ...
  "properties": {
    "blame": {
      "author": "aperson",
      "author-mail": "<aperson@acompany.com>",
      "author-time": "1350899798",
      "author-tz": "+0000",
      "committer": "aperson",
      "committer-mail": "<aperson@acompany.com>",
      "committer-time": "1350899798",
      "committer-tz": "+0000",
      "summary": "blah blah commit comment blah",
      "boundary": true,
      "filename": "src/net/myproject/mypackage/MyClass.java"
    }
  }
}
```

Note that the bare `boundary` key is given the automatic value `true`.

#### codeclimate

```plain
usage: sarif codeclimate [-h] [--output PATH] [--filter FILE] [--autotrim] [--trim PREFIX] [file_or_dir ...]

Write a JSON representation in Code Climate format of SARIF file(s) for viewing as a Code Quality report in GitLab UI

positional arguments:
  file_or_dir           A SARIF file or a directory containing SARIF files

optional arguments:
  -h, --help            show this help message and exit
  --output PATH, -o PATH
                        Output file or directory
  --filter FILE, -b FILE
                        Specify the filter file to apply. See README for format.
  --autotrim, -a        Strip off the common prefix of paths in the CSV output
  --trim PREFIX         Prefix to strip from issue paths, e.g. the checkout directory on the build agent
```

Write out a JSON file of Code Climate tool format from [a set of] SARIF files.
This can then be published as a Code Quality report artefact in a GitLab pipeline and shown in GitLab UI for merge requests.

The JSON output can also be filtered using the blame information; see
[Filtering](#filtering) below for how to use the `--filter` option.

#### copy

```plain
usage: sarif copy [-h] [--output FILE] [--filter FILE] [--timestamp] [file_or_dir [file_or_dir ...]]

Write a new SARIF file containing optionally-filtered data from other SARIF file(s)

positional arguments:
  file_or_dir           A SARIF file or a directory containing SARIF files

optional arguments:
  -h, --help            show this help message and exit
  --output FILE, -o FILE
                        Output file
  --filter FILE, -b FILE
                        Specify the filter file to apply. See README for format.
  --timestamp, -t       Append current timestamp to output filename in the "yyyymmddThhmmssZ" format used by the `sarif trend` command
```

Write a new SARIF file containing optionally-filtered data from an existing SARIF file or multiple
SARIF files.  The resulting file contains each run from the original SARIF files back-to-back.
The results can be filtered (see [Filtering](#filtering) below), in which case only
those results from the original SARIF files that meet the filter are included; the output file
contains no information about the excluded records.  If a run in the original file was empty,
or all its results are filtered out, the empty run is still included.

If no output filename is provided, a file called `out.sarif` in the current directory is written.
If the output file already exists and is also in the input file list, it is not included in the
inputs, to avoid duplication of results.  The output file is overwritten without warning.

The `file_or_dir` specifier can include wildcards e.g. `c:\temp\**\devskim*.sarif` (i.e.
a "glob").  This works for all commands, but it is particularly useful for `copy`.

One use for this is to combine a set of SARIF files from multiple static analysis tools run during
a build process into a single file that can be more easily stored and processed as a build asset.

#### csv

```plain
usage: sarif csv [-h] [--output PATH] [--filter FILE] [--autotrim] [--trim PREFIX] [file_or_dir [file_or_dir ...]]

Write a CSV file listing the issues from the SARIF files(s) specified

positional arguments:
  file_or_dir           A SARIF file or a directory containing SARIF files

optional arguments:
  -h, --help            show this help message and exit
  --output PATH, -o PATH
                        Output file or directory
  --filter FILE, -b FILE
                        Specify the filter file to apply. See README for format.
  --autotrim, -a        Strip off the common prefix of paths in the CSV output
  --trim PREFIX         Prefix to strip from issue paths, e.g. the checkout directory on the build agent
```

Write out a simple tabular list of issues from [a set of] SARIF files.  This can then be analysed, e.g. via Pivot Tables in Excel.

Use the `--trim` option to strip specific prefixes from the paths, to make the CSV less verbose.  Alternatively, use `--autotrim` to strip off the longest common prefix.

Generate a CSV summary of a single SARIF file with common file path prefix suppressed:

```shell
sarif csv "C:\temp\sarif_files\devskim_myapp.sarif"
```

Generate a CSV summary of a directory of SARIF files with path prefix `C:\code\my_source_repo` suppressed:

```shell
sarif csv --trim c:\code\my_source_repo "C:\temp\sarif_files"
```

If the SARIF file(s) contain blame information (as added by the `blame` command), then the CSV
includes an "Author" column indicating who last modified the line in question.

The CSV output can also be filtered using the same blame information; see
[Filtering](#filtering) below for how to use the `--filter` option.

#### diff

```plain
usage: sarif diff [-h] [--output FILE] [--filter FILE] old_file_or_dir new_file_or_dir

Find the difference between two [sets of] SARIF files

positional arguments:
  old_file_or_dir       An old SARIF file or a directory containing the old SARIF files
  new_file_or_dir       A new SARIF file or a directory containing the new SARIF files

optional arguments:
  -h, --help            show this help message and exit
  --output FILE, -o FILE
                        Output file
  --filter FILE, -b FILE
                        Specify the filter file to apply. See README for format.
```

Print the difference between two [sets of] SARIF files.

Difference between the issues in two SARIF files:

```shell
sarif diff "C:\temp\old_sarif_files\devskim_myapp.sarif" "C:\temp\sarif_files\devskim_myapp.sarif"
```

Difference between the issues in two directories of SARIF files:

```shell
sarif diff "C:\temp\old_sarif_files" "C:\temp\sarif_files"
```

Write output to JSON file instead of printing to stdout:

```shell
sarif diff -o mydiff.json "C:\temp\old_sarif_files\devskim_myapp.sarif" "C:\temp\sarif_files\devskim_myapp.sarif"
```

The JSON format is like this:

```json

{
    "all": {
        "+": 5,
        "-": 11
    },
    "error": {
        "+": 2,
        "-": 0,
        "codes": {
            "XYZ1234 Some Issue": {
                "<": 0,
                ">": 2,
                "+@": [
                    {
                        "Location": "C:\\code\\file1.py",
                        "Line": 119
                    },
                    {
                        "Location": "C:\\code\\file2.py",
                        "Line": 61
                    }
                ]
            },
        }
    },
    "warning": {
        "+": 3,
        "-": 11,
        "codes": {...}
    },
    "note": {
        "+": 3,
        "-": 11,
        "codes": {...}
    }
}
```

Where:

- "+" indicates new issue types at this severity, "error", "warning" or "note"
- "-" indicates resolved issue types at this severity (no occurrences remaining)
- "codes" lists each issue code where the number of occurrences has changed:
  - occurrences before indicated by "<"
  - occurrences after indicated by ">"
  - new locations indicated by "+@"

If the set of issue codes at a given severity has changed, diff will report this even if the total
number of issue types at that severity is unchanged.

When the number of occurrences of an issue code is unchanged, diff will not report this issue code,
although it is possible that an equal number of new occurrences of the specific issue have arisen as
have been resolved.  This is to avoid reporting line number changes.

The `diff` operation shows the location of new occurrences of each issue.  When writing to an
output JSON file, all new locations are written, but when writing output to the console, a maximum
of three locations are shown.  Note that there can be some false positives here, if line numbers
have changed.

See [Filtering](#filtering) below for how to use the `--filter` option.

#### emacs

```plain
usage: sarif emacs [-h] [--output PATH] [--filter FILE] [--no-autotrim] [--image IMAGE] [--trim PREFIX] [file_or_dir [file_or_dir ...]]

Write a representation of SARIF file(s) for viewing in emacs

positional arguments:
  file_or_dir           A SARIF file or a directory containing SARIF files

optional arguments:
  -h, --help            show this help message and exit
  --output PATH, -o PATH
                        Output file or directory
  --filter FILE, -b FILE
                        Specify the filter file to apply. See README for format.
  --no-autotrim, -n     Do not strip off the common prefix of paths in the output document
  --image IMAGE         Image to include at top of file - SARIF logo by default
  --trim PREFIX         Prefix to strip from issue paths, e.g. the checkout directory on the build agent
```

#### html

```plain
usage: sarif html [-h] [--output PATH] [--filter FILE] [--no-autotrim] [--image IMAGE] [--trim PREFIX] [file_or_dir [file_or_dir ...]]

Write an HTML representation of SARIF file(s) for viewing in a web browser

positional arguments:
  file_or_dir           A SARIF file or a directory containing SARIF files

optional arguments:
  -h, --help            show this help message and exit
  --output PATH, -o PATH
                        Output file or directory
  --filter FILE, -b FILE
                        Specify the filter file to apply. See README for format.
  --no-autotrim, -n     Do not strip off the common prefix of paths in the output document
  --image IMAGE         Image to include at top of file - SARIF logo by default
  --trim PREFIX         Prefix to strip from issue paths, e.g. the checkout directory on the build agent
```

Create an HTML file summarising SARIF results.

```shell
sarif html -o summary.html "C:\temp\sarif_files"
```

Use the `--trim` option to strip specific prefixes from the paths, to make the generated HTML page less verbose.  The longest common prefix of the paths will be trimmed unless `--no-autotrim` is specified.

Use the `--image` option to provide a header image for the top of the HTML page.  The image is embedded into the HTML, so the HTML document remains a portable standalone file.

See [Filtering](#filtering) below for how to use the `--filter` option.

#### info

```plain
usage: sarif info [-h] [--output FILE] [file_or_dir [file_or_dir ...]]

Print information about SARIF file(s) structure

positional arguments:
  file_or_dir           A SARIF file or a directory containing SARIF files

optional arguments:
  -h, --help            show this help message and exit
  --output FILE, -o FILE
                        Output file
```

Print information about the structure of a SARIF file or multiple files.  This is about the JSON
structure rather than any meaning of the results produced by the tool.  The summary includes the
full path of the file, its size and modified date, the number of runs, and for each run, the
tool that generated the run, the number of results, and the entries in the results' property bags.

```plain
c:\temp\sarif_files\ios_devskim_output.sarif
  1256241 bytes (1.2 MiB)
  modified: 2021-10-13 21:50:01.251544, accessed: 2022-01-09 18:23:00.060573, ctime: 2021-10-13 20:49:00
  1 run
    Tool: devskim
    1323 results
    All results have properties: tags, DevSkimSeverity
```

#### ls

```plain
usage: sarif ls [-h] [--output FILE] [file_or_dir [file_or_dir ...]]

List all SARIF files in the directories specified

positional arguments:
  file_or_dir           A SARIF file or a directory containing SARIF files

optional arguments:
  -h, --help            show this help message and exit
  --output FILE, -o FILE
                        Output file
```

List SARIF files in one or more directories.

```shell
sarif ls "C:\temp\sarif_files" "C:\temp\sarif_with_date"
```

#### summary

```plain
usage: sarif summary [-h] [--output PATH] [--filter FILE] [file_or_dir [file_or_dir ...]]

Write a text summary with the counts of issues from the SARIF files(s) specified

positional arguments:
  file_or_dir           A SARIF file or a directory containing SARIF files

optional arguments:
  -h, --help            show this help message and exit
  --output PATH, -o PATH
                        Output file or directory
  --filter FILE, -b FILE
                        Specify the filter file to apply. See README for format.
```

Print a summary of the issues in one or more SARIF file(s), grouped by severity and then ordered by number of occurrences.

When directories are provided as input and output, a summary is written for each input file, along with another file containing the totals.

```shell
sarif summary -o summaries "C:\temp\sarif_files"
```

When no output directory or file is specified, the overall summary is printed to the standard output.

```shell
sarif summary "C:\temp\sarif_files\devskim_myapp.sarif"
```

See [Filtering](#filtering) below for how to use the `--filter` option.

#### trend

```plain
usage: sarif trend [-h] [--output FILE] [--filter FILE] [--dateformat {dmy,mdy,ymd}] [file_or_dir [file_or_dir ...]]

Write a CSV file with time series data from SARIF files with "yyyymmddThhmmssZ" timestamps in their filenames

positional arguments:
  file_or_dir           A SARIF file or a directory containing SARIF files

optional arguments:
  -h, --help            show this help message and exit
  --output FILE, -o FILE
                        Output file
  --filter FILE, -b FILE
                        Specify the filter file to apply. See README for format.
  --dateformat {dmy,mdy,ymd}, -f {dmy,mdy,ymd}
                        Date component order to use in output CSV. Default is `dmy`
```

Generate a CSV showing a timeline of issues from a set of SARIF files in a directory.  The SARIF file names must contain a
timestamp in the specific format `yyyymmddThhhmmss` e.g. `20211012T110000Z`.

The CSV can be loaded in Microsoft Excel for graphing and trend analysis.

```shell
sarif trend -o timeline.csv "C:\temp\sarif_with_date" --dateformat dmy
```

See [Filtering](#filtering) below for how to use the `--filter` option.

#### upgrade-filter

```plain
usage: sarif upgrade-filter [-h] [--output PATH] [file [file ...]]

Upgrade a v1-style blame filter file to a v2-style filter YAML file

positional arguments:
  file                  A v1-style blame-filter file

optional arguments:
  -h, --help            show this help message and exit
  --output PATH, -o PATH
                        Output file or directory
```

#### usage

```plain
usage: sarif usage [-h] [--output FILE]

(Command optional) - print usage and exit

optional arguments:
  -h, --help            show this help message and exit
  --output FILE, -o FILE
                        Output file
```

Print usage and exit.

#### version

```plain
usage: sarif version [-h]

Print version and exit

optional arguments:
  -h, --help  show this help message and exit
```

Prints the version number of sarif-tools in a bare format.

#### word

```plain
usage: sarif word [-h] [--output PATH] [--filter FILE] [--no-autotrim] [--image IMAGE] [--trim PREFIX] [file_or_dir [file_or_dir ...]]

Produce MS Word .docx summaries of the SARIF files specified

positional arguments:
  file_or_dir           A SARIF file or a directory containing SARIF files

optional arguments:
  -h, --help            show this help message and exit
  --output PATH, -o PATH
                        Output file or directory
  --filter FILE, -b FILE
                        Specify the filter file to apply. See README for format.
  --no-autotrim, -n     Do not strip off the common prefix of paths in the output document
  --image IMAGE         Image to include at top of file - SARIF logo by default
  --trim PREFIX         Prefix to strip from issue paths, e.g. the checkout directory on the build agent
```

Create Word documents representing a SARIF file or multiple SARIF files.

If directories are provided for the `-o` option and the input, then a Word document is produced for each individual SARIF file
and for the full set of SARIF files.  Otherwise, a single Word document is created.

Create a Word document for each SARIF file and one for all of them together, in the `reports` directory (created if non-existent):

```shell
sarif word -o reports "C:\temp\sarif_files"
```

Create a Word document for a single SARIF file:

```shell
sarif word -o "reports\devskim_myapp.docx" "C:\temp\sarif_files\devskim_myapp.sarif"
```

Use the `--trim` option to strip specific prefixes from the paths, to make the generated documents less verbose.  The longest common prefix of the paths will be trimmed unless `--no-autotrim` is specified.

Use the `--image` option to provide a header image for the top of the Word document.

See [Filtering](#filtering) below for how to use the `--filter` option.

## Filtering

The data in each `result` object can then be used for filtering via the `--filter` option available for various commands.  This option requires a path to a filter-list YAML file, containing a list of patterns and substrings to match against data in a SARIF file.  The format of a filter-list file is as follows:

```yaml
# Lines beginning with # are interpreted as comments and ignored.
# Optional description for the filter.  If not specified, the filter file name is used.
description: Example filter from README.md

# Optional configuration section to override default values.
configuration:
  # This option controls whether to include results where a property to check is missing, default
  # value is true.
  default-include: false
  # This option only applies filter criteria if the line number is present and not equal to 1.
  # Some static analysis tools set the line number to 1 for whole file issues, but this does not
  # work with blame filtering, because who last changed line 1 is irrelevant.  Default value is
  # true.
  check-line-number: true

# Items in `include` list are interpreted as inclusion filtering rules.
# Items are treated with OR operator, the filtered results includes objects matching any rule.
# Each item can be one rule or a list of rules, in the latter case rules in the list are treated
# with AND operator - all rules must match.
include:
  # The following line includes issues whose author-mail property contains "@microsoft.com" AND
  # found in Java files.
  # Values with special characters `\:;_()$%^@,` must be enclosed in quotes (single or double):
  - author-mail: "@microsoft.com"
    locations[*].physicalLocation.artifactLocation.uri: "*.java"
  # Instead of a substring, a regular expression can be used, enclosed in "/" characters.
  # Issues whose committer-mail property includes a string matching the regular expression are included.
  # Use ^ and $ to match the whole committer-mail property.
  - committer-mail:
      value: "/^<myname.*\\.com>$/"
      # Configuration options can be overridden for any rule.
      default-include: true
      check-line-number: true
# Lines under `exclude` are interpreted as exclusion filtering rules.
exclude:
  # The following line excludes issues whose location is in test Java files with names starting with
  #  the "Test" prefix.
  - location: "Test*.java"
  # The value for the property can be empty, in this case only existence of the property is checked.
  - suppression:
```

Here's an example of a filter-file that includes issues on lines changed by an `@microsoft.com` email address or a `myname.SOMETHING.com` email address, but not if those email addresses end in `bot@microsoft.com` or contain a GUID.  It's the same as the above example, with comments stripped out.

```yaml
description: Example filter from README.md
configuration:
  default-include: true
  check-line-number: true
include:
  - author-mail: "@microsoft.com"
  - author-mail: "/myname\\..*\\.com/"
exclude:
  - author-mail: bot@microsoft.com
  - author-mail: '/[0-9A-F]{8}[-][0-9A-F]{4}[-][0-9A-F]{4}[-][0-9A-F]{4}[-][0-9A-F]{12}\@microsoft.com/'
```

Field names must be specified in [JSONPath notation](https://goessner.net/articles/JsonPath/)
accessing data in the [SARIF `result` object](https://docs.oasis-open.org/sarif/sarif/v2.1.0/cs01/sarif-v2.1.0-cs01.html#_Toc16012594).

For commonly used properties the following shortcuts are defined:
| Shortcut | Full JSONPath |
| -------- | -------- |
| author | properties.blame.author |
| author-mail | properties.blame.author-mail |
| committer | properties.blame.committer |
| committer-mail | properties.blame.committer-mail |
| location | locations[*].physicalLocation.artifactLocation.uri |
| rule | ruleId |
| suppression | suppressions[*].kind |

For the property `uri` (e.g. in `locations[*].physicalLocation.artifactLocation.uri`) file name wildcard characters can be used as it represents a file location:

- `?` - a single occurrence of any character in a directory or file name
- `*` - zero or more occurrences of any character in a directory or file name
- `**` - zero or more occurrences across multiple directory levels

E.g.

- `tests/Test???.js`
- `src/js/*.js`
- `src/js/**/*.js`

All matching is case insensitive, because email addresses are.  Whitespace at the start and end of lines is ignored, which also means that line ending characters don't matter.  The filter file must be UTF-8 encoded (including plain ASCII7).

If there are no inclusion patterns, all issues are included except for those matching the exclusion patterns.  If there are inclusion patterns, only issues matching the inclusion patterns are included.  If an issue matches one or more inclusion patterns and also at least one exclusion pattern, it is excluded.

## Usage as a Python library

Although not its primary purpose, you can use sarif-tools from a Python script or module to
load and summarise SARIF results.

### Basic usage pattern

After installation, use `sarif.loader` to load a SARIF file or files, and then use the operations
on the returned `SarifFile` or `SarifFileSet` objects to explore the data.

```python
from sarif import loader

sarif_data = loader.load_sarif_file(path_to_sarif_file)
report = sarif_data.get_report()
error_histogram = report.get_issue_type_histogram_for_severity("error")
```

### Files and file sets

The three classes defined in the `sarif_files` module, `SarifFileSet`, `SarifFile` and `SarifRun`,
provide similar APIs, which allows SARIF results to be handled similarly at multiple levels of
aggregation.  To explore the results, use `get_report()`.

### Results, records and reports

There are three levels of normalisation and aggregation of the results from the SARIF files:

1. Results, obtained from the `SarifFileSet`, `SarifFile` or `SarifRun` via the `get_results()`
   method.  These are dicts directly deserialised from the JSON Result objects in the SARIF data,
   as defined in the
   [SARIF standard section 3.27](https://docs.oasis-open.org/sarif/sarif/v2.1.0/os/sarif-v2.1.0-os.html#_Toc34317638).
2. Records, obtained from the `SarifFileSet`, `SarifFile` or `SarifRun` via the `get_records()`
   method.  Each record is a flat dict of key-value pairs where the keys are those in
   `sarif_file.BASIC_RECORD_ATTRIBUTES`:
    - `"Tool"` - the tool name for the run containing the result.
    - `"Severity"` - the SARIF severity for the record.  One of `error`, `warning` (the default if the
      record doesn't specify), `note` or `none`.
    - `"Code"` - the issue code from the result.
    - `"Description"` - the issue name from the result - corresponding to the Code.
    - `"Location"` - the location of the issue, typically the file containing the issue.  Format varies
      by tool.
    - `"Line"` - the line number in the file where the issue occurs.  Value is a string.  This defaults
      to `"1"` if the tool failed to identify the line.
   This flat list of records is useful for machine-readable output formats like CSV.
3. Reports are `IssuesReport` objects, as defined in the `issues_report.py`.  Reports group and
   sort records to provide a useful structure for human-readable output formats like Word and HTML.
   The report can be obtained from the `SarifFileSet`, `SarifFile` or `SarifRun` via the
   `get_report()` method.

### Key methods on Files and file sets

These methods exist across `SarifFileSet`, `SarifFile` and `SarifRun`.

- `get_distinct_tool_names()`
  - Returns a list of distinct tool names in a `SarifFile` or for all files in a `SarifFileSet`.
A `SarifRun` has a single tool name so the equivalent method is `get_tool_name()`.
- `get_results()`
  - Return the list of SARIF result objects as dicts - see above.
- `get_records()`
  - Return the list of SARIF records as flat dicts - see above.
- `get_report()`
  - Return the `IssuesReport` object that groups the records by issue type and sorts them by
    location.

### Key methods on `IssuesReport`

The `IssuesReport` provides methods that group issues in three levels:

1. Severity: SARIF defines severity levels `error`, `warning` and `note`, plus `none` which is
   sometimes used when the results in the SARIF file are not issues.
2. Issue Type: SARIF Results are identified by code, description or a combination of the two.
   The same issue can occur in multiple locations, so the `IssuesReport` groups these together
   per issue type, within a given severity level.  The issue type code key is formed by combining
   the issue code and issue description, with a space character inserted in-between, but truncated
   to be shorter than 120 characters.
3. Record: The individual records under an issue type represent the locations where that issue
   occurs.  They are sorted into a sensible location order.

The key methods on the `IssuesReport` are:

- `get_severities()`: Get the ordered list of severities, from worst to least bad.  To render a
  report, get the list of severities and then iterate through them calling other methods to get the
  summary of results at that severity level in the required form.
- `any_none_severities()`: `True` or `False` that there are any records with severity `none`.
  Severity `none` is not used by many tools, so it is only included as a heading if there are any
  matching records, to avoid a redundant and potentially-confusing `none` header otherwise.
  Severities `error`, `warning` and `note` are always included even if no records, as these levels
  are all relevant for static analysis issues.
- `get_issue_count_for_severity(severity)`: Get the total number of results at this severity.
- `get_issue_type_count_for_severity(severity)`:  Get the total number of issue types at this severity.
- `get_issues_grouped_by_type_for_severity(severity)`: Get a dict from issue type key (code + description)
  to the sorted list of results of that type.
- `get_issue_type_histogram_for_severity(severity)`: Get a dict from issue type key (code + description)
  to the number of results of that type.
- `get_issues_for_severity(severity)`: Get a flat list of results at the given severity.  This is
  like the result of `get_issues_grouped_by_type_for_severity` but flattened from a `dict[str, list]`
  to a single `list` in the same order as the original sequence of lists.

#### Disaggregation and filename access

These fields and methods allow access to the underlying information about the SARIF files.

- `SarifFileSet.subdirs` - a list of `SarifFileSet` objects corresponding to the subdirectories of
  the directory from which the `SarifFileSet` was created.
- `SarifFileSet.files` - a list of `SarifFile` objects corresponding to the SARIF files contained
  in the directory from which the `SarifFileSet` was created.
- `SarifFile.get_abs_file_path()` - get the absolute path to the SARIF file.
- `SarifFile.get_file_name()` - get the name of the SARIF file.
- `SarifFile.get_file_name_without_extension()` - get the name of the SARIF file without its
  extension.  Useful for constructing derived filenames.
- `SarifFile.get_filename_timestamp()` - extract the timestamp from the filename of a SARIF file,
  and return it as a string.  The timestamp must be in the format specified in the `sarif trend`
  command.
- `SarifFile.runs` - a list of `SarifRun` objects contained in the SARIF file.  Most SARIF files
  only contain a single run, but it is possible to aggregate runs from multiple tools into a
  single SARIF file.

#### Path shortening API

Call `init_path_prefix_stripping(autotrim, path_prefixes)` on a `SarifFileSet`, `SarifFile` or `SarifRun` object to set up path filtering, either automatically removing the longest common prefix (`autotrim=True`) or removing specific prefixes (`autotrim=False` and a list of strings in `path_prefixes`).

#### Filtering API

Call `init_general_filter(filter_description, include_filters, exclude_filters)` on a `SarifFileSet`, `SarifFile` or `SarifRun` object to set up filtering.  `filter_description` is a string and the other parameters are lists of inclusion and exclusion rules.  They correspond in an obvious way to the filter file contents described in [Filtering](#filtering) above.

Call `get_filter_stats()` to retrieve the filter stats after reading the results or records from sarif files.  It returns `None` if there is no filter, or otherwise a `sarif_file.FilterStats` object with integer fields `filtered_in_result_count`, `filtered_out_result_count`.  Call `to_string()` on the `FilterStats` object for a readable representation of these statistics, which also includes the filter file name or description (`filter_description` field).

## Suggested usage in CI pipelines

Using the `--check` option in combination with the `summary` command causes sarif-tools to exit
with a nonzero exit code if there are any issues of the specified level, or higher.  This can
be useful to fail a continuous integration (CI) pipeline in the case of SAST violation.

The SARIF issue levels are `error`, `warning` and `note`.  These are all valid options for the
`--check` option.

E.g. to fail if there are any errors or warnings:

```dos
sarif --check warning summary c:\temp\sarif_files
```

The `diff` command can check for any increase in issues of the specified level or above, relative
to a previous or baseline build.

E.g. to fail if there are any new issue codes at error level:

```dos
sarif --check error diff c:\temp\old_sarif_files c:\temp\sarif_files
```

You can also use sarif-tools to filter and consolidate the output from multiple tools.  E.g.

```bash
# First run your static analysis tools, configured to write SARIF output.  How to do that depends
# the tool.

# Now run the blame command to augment the output with blame information.
sarif blame -o with_blame/myapp_mytool_with_blame.sarif myapp_mytool.sarif

# Now combine all tools' output into a single file
sarif copy --timestamp -o artifacts/myapp_alltools_with_blame.sarif
```

Download the file `myapp_alltools_with_blame_TIMESTAMP.sarif` that is generated.  Then later you can
filter the results using the `--filter` argument, or generate graph of code quality over time
using `sarif trend`.

## Credits

sarif-tools was originally developed during the Microsoft Global Hackathon 2021 by Simon Abykov, Nick Brabbs, Anthony Hayward, Sivaji Kondapalli, Matt Parkes and Kathryn Pentland.

Thank you to everyone who has contributed
[pull requests](https://github.com/microsoft/sarif-tools/pulls?q=reason%3Acompleted)
since the initial release!
