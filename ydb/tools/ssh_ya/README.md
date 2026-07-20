`ssh_ya` syncs the current branch to a remote checkout and runs commands there.

On first use it asks for the remote host and saves it to `~/.config/ssh_ya/config.json`.
Later runs use the saved host automatically. `--host` overrides and updates the saved value.

Build as a normal Yandex-style Python binary:

```bash
./ya make ydb/tools/ssh_ya
```

Or install it into a user bin directory:

```bash
ydb/tools/ssh_ya/install.sh
ydb/tools/ssh_ya/install.sh --dir ~/.local/bin
```

The tool keeps the local branch untouched:
- it creates a temporary commit object from the current working tree
- it includes tracked changes and untracked files
- it does not create a local commit or modify the local index

Remote checkout behavior:
- default path is `~/ydb`
- if the checkout does not exist, the tool runs `git init`
- the remote branch is updated via `git push` only when the target commit changed
- `receive.denyCurrentBranch=updateInstead` keeps the worktree in sync
- by default the tool is quiet; use `--verbose` to print ssh/git commands

Examples:

```bash
python3 ydb/tools/ssh_ya/__main__.py sync

python3 ydb/tools/ssh_ya/__main__.py make ydb/core/blobstorage

python3 ydb/tools/ssh_ya/__main__.py test ydb/core/blobstorage -F '*SomeTest*'

python3 ydb/tools/ssh_ya/__main__.py execute git status

python3 ydb/tools/ssh_ya/__main__.py execute --skip-sync 'uname -a'

python3 ydb/tools/ssh_ya/__main__.py --host other-builder sync
```

Useful options:
- `--host <name>`: override saved host and write it to config
- `--branch <name>`: override branch name, useful for detached HEAD
- `--repo-path <path>`: remote checkout path, default `~/ydb`
- `--ssh-arg <arg>`: pass extra raw `ssh` arguments, repeat as needed
- `--dry-run`: print commands without executing them
- `--verbose`: print underlying ssh/git commands during normal execution
