"""ydb include sanitizer: report-only v1.

Top-level Python package. Sub-packages:

- compdb:    compile_commands.json generation
- analyze:   per-TU clang-include-cleaner runner
- aggregate: cross-TU verdict + graph builder
- report:    output formatters (graphs, CSV, diffs, summary)
"""

__version__ = "0.1.0"
