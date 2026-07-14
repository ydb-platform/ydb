# Documenting new CLI commands

This article describes the process of creating documentation for new {{ ydb-short-name }} CLI commands. It contains a template and recommendations based on existing articles about CLI commands.

## General principles

Before you start working on the documentation for a new command:

1. Familiarize yourself with [{#T}](style-guide.md) to understand the general documentation style rules.
2. Make sure the command is implemented and available in the CLI with the `--help` option.

## Article structure

Each article about a CLI command should follow the following structure:

### 1. First-level heading

The heading should briefly describe the command's action. Use a wording that starts with a noun or a gerund.

**Examples:**

- `# Creating a topic`
- `# Deleting a table`
- `# Getting the status of a background operation`
- `# Reading from a topic`

### 2. Brief description

The first 1–3 sentences describe the purpose of the command. If the command is related to other commands, add links to them.

**Example:**


```markdown
С помощью подкоманды `topic create` вы можете создать новый топик.
```


**Example with a link:**


```markdown
С помощью команды `topic consumer add` вы можете добавить читателя для [созданного ранее](topic-create.md) топика.
```


### 3. Notes (optional)

If the command has important features, limitations, or warnings, add them right after the description using a `{% note %}` block.

**Example:**


```markdown
{% note info %}

При удалении топика также будут удалены все добавленные для него читатели.

{% endnote %}
```


### 4. Command overview

Show the command syntax using the `{{ ydb-cli }}` template and proper formatting.

**Basic template:**


```markdown
Общий вид команды:

{{ ydb-cli }} [global options...] <command> [options...] [arguments...]

* `global options` — [глобальные параметры](commands/global-options.md).
* `options` — [параметры подкоманды](#options).
* `arguments` — аргументы команды (опишите каждый).
```


**Examples:**

For a command without options:


```markdown
{{ ydb-cli }} [global options...] topic drop <topic-path>
```


For a command with options:


```markdown
{{ ydb-cli }} [global options...] topic create [options...] <topic-path>
```


For a complex command:


```markdown
{{ ydb-cli }} [connection options] topic read <topic-path> [--consumer STR] \
  [--format STR] [--wait] [--limit INT] \
  [--transform STR] [--file STR] [--commit BOOL] \
  [дополнительные параметры...]
```


### 5. Help reference

Always add the command to get help:


```markdown
Посмотрите описание команды:

{{ ydb-cli }} <command> --help
```


### 6. Subcommand parameters

If the command has parameters, create a `## Subcommand parameters {#options}` section.

Format the parameters as a table:


```markdown
# # Subcommand parameters {#options}

Имя | Описание
|---|---
`--parameter-name VAL` | Описание параметра.
```


**Recommendations for describing parameters:**

- Specify the value type if it matters (for example, `VAL`, `STR`, `INT`, `BOOL`).
- Specify the default value if there is one.
- List the possible values for enumerations.
- Add links to concepts if the parameter is related to them.
- Use HTML tags for formatting inside table cells (for example, `<br/>`, `<ul>`, `<li>`).

**Example of a simple parameter:**


```markdown
`--consumer VAL` | Имя читателя, которого нужно добавить.
```


**Example of a parameter with a description:**


```markdown
`--retention-period` | Время хранения данных в топике. Положительное число с указанием единицы измерения.<br/>Поддерживаются следующие единицы:<ul><li>`s` – секунды;</li><li>`m` – минуты;</li><li>`h` – часы;</li><li>`d` – дни.</li></ul>Значение по умолчанию — `18h`.
```


**Example of a parameter with a link:**


```markdown
`--partitions-count`| Количество [партиций](../../concepts/datamodel/topic.md#partitioning) топика.<br/>Значение по умолчанию — `1`.
```


**Grouping parameters:**

For commands with many parameters, you can split them into groups:


```markdown
# # Parameters {#options}

# ## Required parameters

`<topic-path>`: Путь топика

# ## Basic optional parameters

`-c VAL`, `--consumer VAL`: Имя читателя топика.

# ## Other optional parameters

| Имя | Описание |
|-----|----------|
| `--idle-timeout VAL` | Таймаут принятия решения о том что топик пуст. |
```


### 7. Examples

Always add a section with examples of using the command.

**Template for the examples section:**


```markdown
# # Examples {#examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

[Описание примера]:
```


**Recommendations for examples:**

- Start each example with a brief description of what it demonstrates.
- Use the `quickstart` profile for examples: `{{ ydb-cli }} -p quickstart ...`
- Show the results of running the command if they are important for understanding.
- Group related examples.
- Add links to related commands or articles at the end of the section, if appropriate.

**Example of simple usage:**


```markdown
Создайте читателя с именем `my-consumer` для [созданного ранее](topic-create.md) топика `my-topic`:

{{ ydb-cli }} -p quickstart topic consumer add \
  --consumer my-consumer \
  my-topic
```


**Example with output:**


```markdown
Убедитесь, что читатель создан:

{{ ydb-cli }} -p quickstart scheme describe my-topic

Результат:

    RetentionPeriod: 2h
    PartitionsCount: 2
    SupportedCodecs: RAW, GZIP

    Consumers:
    ┌──────────────┬─────────────────┬───────────────────────────────┬───────────┐
    || ConsumerName | SupportedCodecs | ReadFrom                      | Important |
    ├──────────────┼─────────────────┼───────────────────────────────┼───────────┤
    || my-consumer  | RAW, GZIP       | Mon, 15 Aug 2022 16:00:00 MSK | 0         |
    └──────────────┴─────────────────┴───────────────────────────────┴───────────┘
```


**Example with multiple use cases:**


```markdown
* Чтение одного сообщения с выводом в терминал:

{{ ydb-cli }} -p quickstart topic read topic1 -c c1

* Ожидание появления и чтение одного сообщения с записью его в файл:

{{ ydb-cli }} -p quickstart topic read topic1 -c c1 -w -f message.bin
```


## Full article template

The full article template is available in a separate file: [template](https://github.com/ydb-platform/ydb/tree/main/ydb/docs/en/core/contributor/documentation/_templates/cli-command-template.md). Use it as a basis when creating documentation for a new CLI command.

## Special cases

### Commands without parameters

If the command has no parameters (except global ones), the "Subcommand parameters" section can be omitted.

**Example:**


```markdown
# Deleting a topic

С помощью подкоманды `topic drop` вы можете удалить [созданный ранее](topic-create.md) топик.

Общий вид команды:

{{ ydb-cli }} [global options...] topic drop <topic-path>

* `global options` — [глобальные параметры](commands/global-options.md).
* `topic-path` — путь топика.

Посмотрите описание команды удаления топика:

{{ ydb-cli }} topic drop --help

# # Examples {#examples}
...
```


### Commands with operation modes

If the command supports multiple operation modes, describe them before the parameters section.

**Example:**


```markdown
Поддерживается три режима работы команды:

1. **Одно сообщение**. Из топика считывается не более одного сообщения.
2. **Пакетный режим**. Сообщения из топика считываются до того момента, пока в топике не закончатся сообщения для обработки.
3. **Потоковый режим**. Сообщения из топика считываются по мере их появления.
```


### Deprecated command warnings

If the command is deprecated, add a warning at the beginning of the article:


```markdown
{% include notitle [warning](../../reference/ydb-cli/_includes/deprecated_command_warning.md) %}
```


## File naming

- Use hyphens instead of underscores: `topic-create.md`, not `topic_create.md`.
- The file name should match the command structure: `topic-consumer-add.md` for the `topic consumer add` command.
- For simple commands, use short names: `version.md`, `table-drop.md`.

## Integration into documentation

After creating the article:

1. **Add a link to `_includes/commands.md`** — the general list of all CLI commands.
2. **Update the table of contents** — add the article to the corresponding `toc_i.yaml` or `toc_p.yaml`.
3. **Add cross-references** — update related articles by adding links to the new command.
4. **Check the links** — make sure all internal links are correct and use relative paths with the `.md` suffix.

## Pre-publication check

Before submitting a pull request, make sure that:

- [ ] The article follows the structure described above.
- [ ] All examples have been tested and work correctly.
- [ ] The correct variables are used: `{{ ydb-cli }}`, `{{ ydb-short-name }}`.
- [ ] All links are correct and use relative paths with `.md`.
- [ ] The article is added to the table of contents and the command list.
- [ ] The text follows the [style guide](style-guide.md).
- [ ] There are no typos or grammatical errors.

## See also

- [{#T}](style-guide.md) — general documentation style guide
- [{#T}](structure.md) — documentation structure
- [{#T}](review.md) — documentation review process
- [Examples of existing articles](../../reference/ydb-cli/index.md) — study similar commands for inspiration
