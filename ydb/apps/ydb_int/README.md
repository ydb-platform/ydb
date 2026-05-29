# YDB CLI — Experimental Build (`ydb_int`)

`ydb_int` is an internal build of the YDB command-line client that hosts
**experimental** commands. It is intended for YDB developers and early adopters
who need to validate functionality that is not yet ready to be exposed in the
official client ([`ydb/apps/ydb`](../ydb)).

The binary is **not** distributed as an official release artifact, has no
versioning guarantees, and its commands and flags may change or be removed at
any time without prior notice.

## Relationship with the official `ydb` client

| Aspect                      | `ydb/apps/ydb`                  | `ydb/apps/ydb_int`                |
|-----------------------------|---------------------------------|-----------------------------------|
| Audience                    | End users                       | YDB developers, contributors      |
| Distribution                | Official releases, packages     | Built locally from source         |
| Stability of commands       | Backwards-compatible guarantees | No guarantees                     |
| Experimental commands       | Hidden / unavailable            | Available under `experimental`    |

`ydb_int` is built on top of the same client infrastructure as the official
`ydb` client ([`ydb/public/lib/ydb_cli`](../../public/lib/ydb_cli)) and adds
the `experimental` (alias `exp`) command tree on top of it. The rest of the
behaviour (authentication options, profiles, output formats, etc.) largely
matches the official `ydb` client.

## Building

```bash
./ya make --build relwithdebinfo ydb/apps/ydb_int
```

The resulting binary is placed at `ydb/apps/ydb_int/ydb_int`.

## Running

The invocation mirrors the official `ydb` client; the only difference is the
extra `experimental` command tree:

```bash
./ydb/apps/ydb_int/ydb_int experimental --help
```

## Adding a new experimental command

The recipe below describes the minimum set of steps required to introduce a
new command. The `experimental` command tree itself is defined in
[`commands/ydb_service_experimental.cpp`](commands/ydb_service_experimental.cpp);
that file is the single entry point for registering new commands.

### 1. Declare the command class

Choose a base class according to what the command needs:

| Base class            | Use when                                                                |
|-----------------------|-------------------------------------------------------------------------|
| `TClientCommand`      | The command does not need a YDB connection (e.g. local utility).        |
| `TYdbCommand`         | The command needs a YDB driver (most common case).                      |
| `TTableCommand`       | The command operates on tables via the Table service SDK.               |
| `TClientCommandTree`  | The command is a subtree that groups other commands.                    |

Add the declaration to
[`commands/ydb_service_experimental.h`](commands/ydb_service_experimental.h)
(or to a dedicated header if the command is large):

```cpp
class TCommandMyFeature : public TYdbCommand {
public:
    TCommandMyFeature();
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    TString InputFile;
    bool Force = false;
};
```

### 2. Implement the command

In the corresponding `.cpp` file inside `commands/`:

```cpp
TCommandMyFeature::TCommandMyFeature()
    : TYdbCommand("my-feature", {"mf"}, "Short, imperative description of the command")
{}

void TCommandMyFeature::Config(TConfig& config) {
    TYdbCommand::Config(config);

    config.Opts->AddLongOption('f', "file", "Path to the input file")
        .RequiredArgument("PATH").StoreResult(&InputFile);
    config.Opts->AddLongOption("force", "Proceed even if a conflict is detected")
        .NoArgument().StoreTrue(&Force);

    config.SetFreeArgsNum(0);
}

void TCommandMyFeature::Parse(TConfig& config) {
    TYdbCommand::Parse(config);

    if (InputFile.empty()) {
        throw TMisuseException() << "Option --file is required";
    }
}

int TCommandMyFeature::Run(TConfig& config) {
    auto driver = CreateDriver(config);
    // ... perform the operation ...
    return EXIT_SUCCESS;
}
```

### 3. Register the command

Add the command to the `experimental` command tree by appending an
`AddCommand` call to the constructor of `TCommandExperimental` in
[`commands/ydb_service_experimental.cpp`](commands/ydb_service_experimental.cpp):

```cpp
TCommandExperimental::TCommandExperimental()
    : TClientCommandTree("experimental", {"exp"}, "Experimental operations")
{
    // ... existing commands ...
    AddCommand(std::make_unique<TCommandMyFeature>());
}
```

### 4. Update the build description

If a new `.cpp` file was introduced, list it in
[`commands/ya.make`](commands/ya.make) under `SRCS`. If the command depends on
a library that is not already pulled in transitively, add it to `PEERDIR`.

### 5. Build and verify

```bash
./ya make --build relwithdebinfo ydb/apps/ydb_int
./ydb/apps/ydb_int/ydb_int experimental my-feature --help
```

## Requirements and conventions

The following rules apply to every command added to `ydb_int`. They exist to
keep the experimental binary usable for the developers around you and to ease
the eventual promotion of a command to the official client.

* **Naming.** Use short, kebab-case names for commands and long options (e.g.
  `my-feature`, `--input-file`). Provide a meaningful one-letter short option
  only when it is unambiguous and will not collide with existing global flags.
* **Help text.** Every command and option must have a concise, single-sentence
  description. Mention default values explicitly when they are not obvious.
* **Validation in `Parse`.** Reject invalid combinations of options as early as
  possible by throwing `TMisuseException` from `Parse`; do not defer such
  checks to `Run`.
* **Error handling.** Surface SDK errors via
  `NStatusHelpers::ThrowOnErrorOrPrintIssues` (or `ThrowOnError` inside polling
  loops, to avoid repeating the same issue list on every iteration). Return
  `EXIT_FAILURE` or throw for terminal failures; never silently ignore
  non-`OK` statuses (this also applies to Arrow / protobuf builder APIs).

## Promoting a command to the official client

`ydb_int` is the proving ground for new functionality, not its permanent home.
Once a command has stabilised, its options have been agreed upon, and it
provides value to end users, move the corresponding sources to
[`ydb/public/lib/ydb_cli/commands`](../../public/lib/ydb_cli/commands), register
it in [`ydb/apps/ydb`](../ydb), and remove the experimental version from
`ydb_int`.
