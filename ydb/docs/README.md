# YDB Documentation

This folder contains the source code for [YDB documentation](https://ydb.tech/docs/en/) built using the [Diplodoc](https://diplodoc.com/) documentation platform.

## Contributing to Documentation 📖

YDB follows the "Documentation as Code" approach. For comprehensive information about contributing to YDB documentation, including review process, style guide, and structure guidelines, visit:

- **[Contributing to YDB Documentation](https://ydb.tech/docs/en/contributor/documentation/?version=main)** — Overview
- **[Review process](https://ydb.tech/docs/en/contributor/documentation/review/?version=main)** - Documentation review workflow and requirements
- **[Style guide](https://ydb.tech/docs/en/contributor/documentation/style-guide/?version=main)** - Writing standards and conventions for YDB documentation
- **[Structure](https://ydb.tech/docs/en/contributor/documentation/structure/?version=main)** - Organization and hierarchy of documentation content
- **[Genres](https://ydb.tech/docs/en/contributor/documentation/genres/?version=main)** - Different types of documentation and their purposes

## AI Skills 🤖

YDB provides reusable AI skills for documentation work:

- **[YDB documentation](.ruler/skills/ydb-documentation/SKILL.md)** creates
  well-integrated documentation in Russian and English.
- **[Internal server release notes](.ruler/skills/generate-internal-changelog/SKILL.md)**
  prepares feature-only server release notes for a stable release line.

### Quick Start with the Skills

1. **Prepare the required input**:

   For product documentation, gather:

   - Description and purpose
   - Parameters, options, configurations
   - Code examples and usage patterns
   - Limitations and constraints
   - Links to PR/Issue where it was implemented

   For an internal release, provide a three-component release identifier, for
   example `26.3.1`.

2. **From `ydb/docs`, run `ruler apply --skills`** to generate agent
   configurations and distribute the skills:

```bash
cd ydb/docs
npm install -g @intellectronica/ruler
ruler apply --skills
```

3. **Ask the agent for the required workflow**.

   Examples:

   - "Document this YDB feature."
   - "Сделай внутренний релиз YDB 26.3.1."
   - "Prepare the internal release for 26.3.1."

The skill descriptions provide automatic routing. Requests containing
"внутренний релиз" or "internal release" activate
`generate-internal-changelog`.

### Key Resources

All documentation tools are in [`.ruler/`](.ruler/):

- **[ydb-documentation](.ruler/skills/ydb-documentation/SKILL.md)** -
  Documentation writing workflow.
- **[generate-internal-changelog](.ruler/skills/generate-internal-changelog/SKILL.md)** -
  Internal server release-notes workflow, coverage evaluator, and evals.
- **[DOCUMENTATION_RULES.md](.ruler/DOCUMENTATION_RULES.md)** - 15 content rules.
- **[FORMAT_RULES.md](.ruler/FORMAT_RULES.md)** - Markdown formatting standards.
- **[ruler.toml](.ruler/ruler.toml)** - Agent and skill distribution
  configuration.

## Quick Start

This folder provides two scripts to help you work with the documentation locally:

### Install diplodoc CLI

```bash
npm install -g @diplodoc/cli@latest
```

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

## LLM indexes (`llms.txt`)

Diplodoc generates per-locale indexes (`/docs/{en|ru}/llms.txt` and `llms-full.txt`). The hub [`llms.txt`](./llms.txt) links agents to those indexes for the **default stable** (no `?version=`; currently `v26.1`), the **`main` trunk**, and each **stable** (`?version=vX.Y`). Keep the stable list in the hub up to date when new release lines appear.

## File Structure

The documentation source files are organized as Markdown files with YAML configuration, following the Diplodoc documentation format. The built documentation includes both English (`en`) and Russian (`ru`) versions.
