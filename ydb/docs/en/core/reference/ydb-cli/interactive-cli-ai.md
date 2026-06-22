# AI assistant mode

The {{ ydb-short-name }} CLI [interactive query execution mode](interactive-cli.md) includes an AI assistant mode — a conversational agent powered by a large language model (LLM). The assistant understands natural-language requests and helps you work with the database: explore its structure, write and run YQL queries, explain query plans, search the documentation, and invoke {{ ydb-short-name }} CLI commands.

{% note warning %}

The assistant calls an external large language model API that you specify yourself. The conversation contents, including the schema structure and query results that the assistant chooses to pass to the model, are sent to that API. Do not use this mode with sensitive data if your policies prohibit it. The {{ ydb-short-name }} CLI does not provide a language model or an access key — you must obtain them from the provider of your choice.

{% endnote %}

## Switching to AI mode {#enter}

The interactive mode runs in one of two submodes:

* **YQL** — entering and running YQL queries (used by default on the first launch);
* **AI** — a dialog with the AI assistant.

Start the [interactive mode](interactive-cli.md) by running the `{{ ydb-cli }}` command without a subcommand and with the required [connection parameters](connect.md):

```bash
{{ ydb-cli }} [global options...]
```

The active submode is shown in the command-line prompt (`YQL>` or `AI>`). To switch between submodes, press `Ctrl + T` or use the `/switch` command. The {{ ydb-short-name }} CLI remembers the last used submode and opens it on the next launch.

## Setting up an AI profile {#profile}

The assistant requires an AI profile — a set of connection parameters for the language model API. The first time you switch to AI mode, if no profiles exist yet, the {{ ydb-short-name }} CLI offers to create one and guides you through the setup step by step:

Parameter | Description
--- | ---
**API endpoint** | The URL of the language model API. It must start with `http://` or `https://` (for example, `https://api.openai.com/v1/`). The {{ ydb-short-name }} CLI verifies the connection to the specified address.
**API type** | The API type: <ul><li>**OpenAI-compatible** — for models with an OpenAI-compatible API;</li><li>**Anthropic** — for the Anthropic API.</li></ul>
**API token** | The API access token. It is stored in the [configuration file](#config-file). You can leave the field empty and pass the token via the [`YDB_CLI_AI_TOKEN`](#env) environment variable.
**Model name** | The model name. If the API allows it, the {{ ydb-short-name }} CLI requests the list of available models; the name can also be entered manually. An empty value means the model name is not passed in requests.
**Profile display name** | The display name under which the profile is saved and shown during selection.

After setup, the profile is saved to the [configuration file](#config-file) and becomes active. To switch the active profile or create a new one, use the [`/model`](#commands) command.

## Working with the assistant {#usage}

In AI mode, type a natural-language message and press `Enter` to send it (`Ctrl + Enter` inserts a newline). The assistant analyzes your request and, when needed, calls tools to work with the database:

Capability | Modifies data | Confirmation
--- | --- | ---
Listing schema objects under a given path | No | Not required
Describing a schema object (table, topic, and so on) | No | Not required
Getting a YQL query plan (`EXPLAIN`) | No | Not required
Searching the {{ ydb-short-name }} documentation | No | Not required
Getting help on {{ ydb-short-name }} CLI commands | No | Not required
Running a YQL query | Yes | Required
Running a shell command | Yes | Required

The assistant keeps the conversation context: subsequent messages take previous ones into account. You can clear the context with the [`/config`](#commands) command.

### Action confirmation {#approve}

Before performing an action that may modify data — running a YQL query or a shell command — the assistant asks for confirmation and offers the following options:

* **Approve execution** — run the query or command as is;
* **Edit query** / **Edit command** — edit the query or command text before running it;
* **Abort operation** — cancel the execution (and continue working in the same context).

## Commands {#commands}

The following commands are available in AI mode:

Command | Description
--- | ---
`/help` | Show help for AI mode and the list of hotkeys.
`/model` | Select another AI profile or create a new one.
`/config` | Change session settings: clear the conversation context, switch or edit the active profile, remove a profile.
`/switch` | Switch to YQL mode (equivalent to `Ctrl + T`).

## Configuration file {#config-file}

AI profiles and AI mode settings are stored in a YAML file. By default it is `~/.config/ydb/ai_profiles.yaml`; the path can be overridden with the [`YDB_CLI_AI_PROFILE_FILE`](#env) environment variable. The file contains the list of profiles, the ID of the active profile, and the current interactive submode. You usually do not need to edit it manually — manage profiles via the [`/model`](#commands) and [`/config`](#commands) commands.

{% note info %}

A token entered directly during profile setup is stored in the configuration file. The {{ ydb-short-name }} CLI restricts the file's permissions so that only the owner — the user who runs the CLI — can read and modify it (on Unix systems, mode `0600`). If you prefer not to store the token on disk, leave the token field empty and pass it via the [`YDB_CLI_AI_TOKEN`](#env) environment variable.

{% endnote %}

## Environment variables {#env}

Variable | Description
--- | ---
`YDB_CLI_AI_TOKEN` | The access token for the language model API. Used if the token is not set in the active profile.
`YDB_CLI_AI_PROFILE_FILE` | The path to the AI profiles [configuration file](#config-file). Defaults to `~/.config/ydb/ai_profiles.yaml`. Lets you use a separate profile set or a non-default file location, for example in automated scenarios.
`YDB_CLI_AI_DISABLE_HISTORY` | If set, the AI mode message history is not stored on disk between launches. Use it when the conversation with the assistant should not be persisted to disk.
