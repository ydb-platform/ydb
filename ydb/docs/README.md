# YDB Documentation

This folder contains the source code for YDB documentation built using the [Diplodoc](https://diplodoc.com/) documentation platform.

## Contributing to Documentation 📖

YDB follows the "Documentation as Code" approach. For comprehensive information about contributing to YDB documentation, including review process, style guide, and structure guidelines, visit:

- **[Contributing to YDB Documentation](https://ydb.tech/docs/en/contributor/documentation/?version=main)** — Overview
- **[Review process](https://ydb.tech/docs/en/contributor/documentation/review/?version=main)** - Documentation review workflow and requirements
- **[Style guide](https://ydb.tech/docs/en/contributor/documentation/style-guide/?version=main)** - Writing standards and conventions for YDB documentation
- **[Structure](https://ydb.tech/docs/en/contributor/documentation/structure/?version=main)** - Organization and hierarchy of documentation content
- **[Genres](https://ydb.tech/docs/en/contributor/documentation/genres/?version=main)** - Different types of documentation and their purposes

## Quick Start

This folder provides two scripts to help you work with the documentation locally:

### `build.sh` - Build Documentation Only

Builds the YDB documentation and outputs it to a specified directory.

```bash
# Build to a temporary directory
./build.sh

# Build to a specific directory
./build.sh /path/to/output
```

**Requirements:**
- [YFM builder](https://diplodoc.com/docs/en/tools/docs/) (`yfm` command)

**Returns:**
- Exit code 0 on successful build
- Exit code 1 on build failure

### `run.sh` - Build and Serve Documentation

Builds the documentation and starts a local HTTP server to preview the results.

```bash
# Build and serve from a temporary directory
./run.sh

# Build and serve from a specific directory
./run.sh /path/to/output
```

**Requirements:**
- [YFM builder](https://diplodoc.com/docs/en/tools/docs/) (`yfm` command)
- Python 3 (for the HTTP server)

**Access the documentation:**
- English: http://localhost:8888/en
- Russian: http://localhost:8888/ru

Press `Ctrl+C` to stop the server.

## File Structure

The documentation source files are organized as Markdown files with YAML configuration, following the Diplodoc documentation format. The built documentation includes both English (`en`) and Russian (`ru`) versions.