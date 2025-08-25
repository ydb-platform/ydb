# Project Agents.md Guide for AI Agents

This file provides comprehensive guidance for AI agents working with this codebase.

## More Information About the Project

The documentation is available at <https://ydb.tech/docs/en/>.

## Project Structure for AI Agents Navigation

Every directory is a project (a library or a program).

- `/ydb/core`: Core modules.
- `/ydb/library`: YDB libraries.
- `/ydb/docs`: YDB documentation.
- `/library`: Common libraries; never change them.
- `/util`: Common libraries; never change them.

## Building and Testing Requirements for AI Agents

Building and testing are performed using the Ya utility (`ya-tool`), which is located in the root of the codebase: `/ya`.

To build a project, an AI agent should run:

```bash
cd path/to/project
/ya make
```

To test a project, an AI agent should run:

```bash
cd path/to/project
/ya make -A
```

More information is available at <https://ydb.tech/docs/en/contributor/build-ya>.

## Coding Conventions for AI agents

### C++ Standards for AI Agents

- Use modern C++ (no later than C++20).

### Documentation Guidelines for AI Agents

- Follow the style guide: <https://ydb.tech/docs/en/contributor/documentation/style-guide>
- Keep the structure consistent: <https://ydb.tech/docs/en/contributor/documentation/structure>

## Pull Request Guidelines for AI Agents

- Pull request descriptions must include a changelog entry (a short summary of the changes), followed by a detailed description for reviewers.
- Specify exactly one of the following PR categories:
  - New Feature
  - Experimental Feature
  - Improvement
  - Performance Improvement
  - User Interface
  - Bugfix
  - Backward-Incompatible Change
  - Documentation (changelog entry is not required)
  - Not for Changelog (changelog entry is not required)
