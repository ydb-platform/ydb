# YDB Documentation

This folder contains the source code for [YDB documentation](https://ydb.tech/docs/en/) built using the [Diplodoc](https://diplodoc.com/) documentation platform.

## Contributing to Documentation 📖

YDB follows the "Documentation as Code" approach. For comprehensive information about contributing to YDB documentation, including review process, style guide, and structure guidelines, visit:

- **[Contributing to YDB Documentation](https://ydb.tech/docs/en/contributor/documentation/?version=main)** — Overview
- **[Review process](https://ydb.tech/docs/en/contributor/documentation/review/?version=main)** - Documentation review workflow and requirements
- **[Style guide](https://ydb.tech/docs/en/contributor/documentation/style-guide/?version=main)** - Writing standards and conventions for YDB documentation
- **[Structure](https://ydb.tech/docs/en/contributor/documentation/structure/?version=main)** - Organization and hierarchy of documentation content
- **[Genres](https://ydb.tech/docs/en/contributor/documentation/genres/?version=main)** - Different types of documentation and their purposes

## Documentation Writing Skill 🤖

YDB provides an AI-powered documentation writing skill that helps developers create well-integrated documentation automatically.

### Quick Start with the Skill

1. **Prepare your material first** - gather all information about what needs to be documented:
   - Description and purpose
   - Parameters, options, configurations
   - Code examples and usage patterns
   - Limitations and constraints
   - Links to PR/Issue where it was implemented

2. **Run `ruler apply`** to generate agent configurations:

```bash
npm install -g @intellectronica/ruler
ruler apply
```

3. **In any IDE** (Cursor, VSCode, JetBrains, Copilot, etc.), provide the information and ask the skill to create documentation

The skill will guide you through a 5-stage workflow to create fully-integrated documentation with glossary, reference, recipes, cross-links, and TOC updates — all in Russian and English.

**Important:** The skill cannot invent technical details. Provide complete information for best results.

### Key Resources

All documentation tools are in [`.ruler/`](.ruler/):
- **[DOCUMENTATION_SKILL.md](.ruler/DOCUMENTATION_SKILL.md)** — Complete 5-stage workflow
- **[DOCUMENTATION_RULES.md](.ruler/DOCUMENTATION_RULES.md)** — 15 content rules
- **[FORMAT_RULES.md](.ruler/FORMAT_RULES.md)** — Markdown formatting standards
- **[ruler.toml](.ruler/ruler.toml)** — Agent configuration

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