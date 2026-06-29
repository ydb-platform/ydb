# AI-навыки {{ ydb-short-name }} (ydb-ai-skills)

[ydb-ai-skills](https://github.com/ydb-platform/ydb-ai-skills) — это набор навыков ([skills](https://agentskills.io/)) для AI-агентов разработки, помогающий при работе с {{ ydb-short-name }}: написании запросов на [YQL](../../concepts/glossary.md#yql), проектировании схемы данных и ревью кода приложений на корректность использования {{ ydb-short-name }} SDK. Навыки подключаются к AI-агенту и автоматически активируются в зависимости от контекста запроса.

{% note warning %}

Проект находится в стадии активной разработки. Набор навыков и их наполнение расширяются, поэтому состав и поведение могут меняться. Актуальную информацию смотрите в [репозитории проекта](https://github.com/ydb-platform/ydb-ai-skills).

{% endnote %}

## Что внутри {#whats-inside}

| Навык         | Назначение                                                                                                                |
| ------------- | ------------------------------------------------------------------------------------------------------------------------- |
| **ydb-core**  | Базовый навык: обзор {{ ydb-short-name }}, подключение, аутентификация, основы схемы, CLI. Устанавливается автоматически. |
| **ydb-table** | Аудит и ревью кода приложений на {{ ydb-short-name }} SDK, проектирование схемы, написание YQL.                           |

## Поддерживаемые агенты {#supported-agents}

Навыки распространяются в универсальном формате и устанавливаются в каталог соответствующего агента.

| Агент          | Каталог в проекте   | Глобальный каталог         |
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

## Установка {#installation}

```bash
git clone https://github.com/ydb-platform/ydb-ai-skills.git
cd ydb-ai-skills

# Автоопределение агентов в текущем проекте
./install.sh --detect

# Установка для конкретного агента
./install.sh --agent=claude

# Установить только ydb-table (ydb-core добавится как базовый навык)
./install.sh --agent=claude --skills=ydb-table

# Установить только ydb-table без базового навыка
./install.sh --agent=claude --skills=ydb-table --no-core

# Пробный запуск — показать, что будет сделано, без изменений
./install.sh --agent=claude --dry-run
```

## Использование {#usage}

Отдельных действий для запуска не требуется: агент сам подключает подходящий навык, когда задача относится к {{ ydb-short-name }}. Если агент не поддерживает автоподключение, укажите имя навыка (`ydb-core` или `ydb-table`) в запросе явно.

## Ревью кода {#code-review}

Навык `ydb-table` проверяет код приложения на типичные ошибки работы с {{ ydb-short-name }} SDK и указывает нарушенные правила. Для запуска ревью укажите агенту файл или диапазон изменений.

## Локальный запуск через Ollama {#ollama}

Навыки можно использовать с агентом, работающим на локальной модели через [Ollama](https://ollama.com/), без обращения к облачному API. Команда `ollama launch` доступна начиная с Ollama v0.15.

```bash
# 1. Загрузить локальную модель (нужны поддержка tool calling и большой контекст)
ollama pull gpt-oss:20b

# 2. Запустить Claude Code на этой модели — навыки активируются как обычно
ollama launch claude --model gpt-oss:20b
```

Для агентных сценариев рекомендуется контекст не меньше 64 000 токенов (настраивается в Ollama).

## Дополнительные материалы {#see-also}

- [Репозиторий ydb-ai-skills](https://github.com/ydb-platform/ydb-ai-skills) — исходный код, документация по написанию навыков и тестированию.
- [{#T}](../../yql/reference/index.md)
- [{#T}](../ydb-sdk/index.md)
- [{#T}](../ydb-cli/index.md)
