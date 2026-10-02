# {{ ydb-short-name }} AI skills (ydb-ai-skills)

[ydb-ai-skills](https://github.com/ydb-platform/ydb-ai-skills) is a set of [skills](https://agentskills.io/) for AI coding agents that assist when working with {{ ydb-short-name }}: writing [YQL](../../concepts/glossary.md#yql) queries, designing the data schema, and reviewing application code for correct use of the {{ ydb-short-name }} SDK. Skills plug into your AI agent and activate automatically based on the request context.

{% note warning %}

The project is under active development. The set of skills and their content are expanding, so the composition and behavior may change. See the [project repository](https://github.com/ydb-platform/ydb-ai-skills) for up-to-date information.

{% endnote %}

## What's inside {#whats-inside}

| Skill         | Purpose                                                                                                       |
| ------------- | ------------------------------------------------------------------------------------------------------------ |
| **ydb-core**  | Baseline skill: {{ ydb-short-name }} overview, connection, authentication, schema basics, CLI. Installed automatically. |
| **ydb-table** | Auditing and reviewing application code that uses the {{ ydb-short-name }} SDK, schema design, writing YQL.   |

## Supported agents {#supported-agents}

Skills are distributed in a universal format and installed into the corresponding agent's directory.

| Agent          | Project directory   | Global directory           |
| -------------- | ------------------- | -------------------------- |
| Claude Code    | `.claude/skills/`   | `~/.claude/skills/`        |
| Cursor         | `.cursor/skills/`   | —                          |
| Windsurf       | `.windsurf/skills/` | —                          |
| GitHub Copilot | `.github/skills/`   | `~/.copilot/skills/`       |
| Codex CLI      | `.agents/skills/`   | `~/.codex/skills/`         |
| Roo Code       | `.roo/skills/`      | —                          |
| Gemini CLI     | `.gemini/skills/`   | `~/.gemini/skills/`        |
| Amp            | `.agents/skills/`   | `~/.config/agents/skills/` |
| Kiro           | `.kiro/skills/`     | —                          |
| Trae           | `.trae/skills/`     | —                          |
| Generic        | `.agents/skills/`   | `~/.agents/skills/`        |

## Installation {#installation}

```bash
git clone https://github.com/ydb-platform/ydb-ai-skills.git
cd ydb-ai-skills

# Auto-detect agents in the current project
./install.sh --detect

# Install for a specific agent
./install.sh --agent=claude

# Install only ydb-table (ydb-core is added as the baseline skill)
./install.sh --agent=claude --skills=ydb-table

# Install only ydb-table without the baseline skill
./install.sh --agent=claude --skills=ydb-table --no-core

# Dry run — show what would be done without making changes
./install.sh --agent=claude --dry-run
```

## Usage {#usage}

No separate action is required to start: the agent connects the appropriate skill itself when the task concerns {{ ydb-short-name }}. If the agent does not support auto-activation, specify the skill name (`ydb-core` or `ydb-table`) in the request explicitly.

## Code review {#code-review}

The `ydb-table` skill checks application code for common mistakes when working with the {{ ydb-short-name }} SDK and points to the violated rules. To run a review, point the agent at the file or the range of changes.

## Local run with Ollama {#ollama}

You can use the skills with an agent running on a local model via [Ollama](https://ollama.com/), without calling the cloud API. The `ollama launch` command is available starting from Ollama v0.15.

```bash
# 1. Pull a local model (tool calling support and a large context are required)
ollama pull gpt-oss:20b

# 2. Run Claude Code on that model — the skills activate as usual
ollama launch claude --model gpt-oss:20b
```

For agentic workflows, a context length of at least 64,000 tokens is recommended (configured in Ollama).

## See also {#see-also}

- [ydb-ai-skills repository](https://github.com/ydb-platform/ydb-ai-skills) — source code, documentation on authoring and testing skills.
- [{#T}](../../yql/reference/index.md)
- [{#T}](../ydb-sdk/index.md)
- [{#T}](../ydb-cli/index.md)
