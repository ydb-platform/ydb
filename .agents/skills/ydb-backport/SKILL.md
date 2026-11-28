---
name: ydb-backport
description: "Use when asked to backport a given list of commits and file the YDBBACKPORT ticket for those backports."
---

# Backport commits

Backport the given commits onto the two newest minor branches. Then file the ticket and attach the pull requests.

List paths with `git log -1 --name-only --format= <sha>`. When every path is under ydb/docs/, stop and follow ydb/docs/.agents/skills/ydb-release-publication/SKILL.md. A documentation backport must not contain conflict markers.

## Prerequisite

The Tracker server is the MCP server started by `ya tool mcp connect tracker`.

1. Confirm these tools are connected: `find_fields`, `list_queue_forms`, `list_field_values`, `create_issue`, `get_issue`, and `list_issue_links`.
2. When that server is missing or needs authentication, stop. Ask the user to configure `ya tool mcp connect tracker`, and wait until they say it is ready.
3. Do not call `GetIssue`, `GetIssues`, or `GetQueueLocalFields`. Read queue `YDBBACKPORT` and create the ticket only with the tools named here.

## Inputs

Take the sources from the user, in the order given. Each item is a commit SHA, a pull request number, or a pull request URL on `github.com/ydb-platform/ydb`. Ask for the list when it is missing.

A commit SHA is used as given. A pull request contributes only its final commit, the one that landed on the base branch.

```bash
gh pr view <number> --repo ydb-platform/ydb --json mergeCommit,state,title,url
```

`mergeCommit` is an object. Use its `oid` when `state` is `MERGED`. When `state` is not `MERGED`, or `oid` is absent, stop and ask the user which single commit to cherry-pick. Show the JSON keys when `oid` is absent. When `gh` is not installed, stop and ask the user to install GitHub CLI.

## Branches

The target branches are the two newest minor branches on the upstream remote. Do not copy names from the `Target branches` field. Its options are stale. Do not copy a name from an older ticket.

Pick the upstream remote from `git remote -v`. Its fetch URL, with a trailing `.git` removed, is `https://github.com/ydb-platform/ydb`.

```bash
git ls-remote --heads <upstream> 'refs/heads/stable-*'
```

Keep a name only when it matches `^stable-[0-9]+-[0-9]+-[0-9]+$`. That is a minor branch. Drop major branches, hotfix branches, and every other name. Sort by the first number, then the second, then the third, highest first. Take the first two. When fewer than two names match, stop and ask the user.

When the user names a different list, use that list. Show the branch list before creating branches.

## Cherry-pick

Do not dispatch the Cherry-pick_v2 workflow. It stores log conflicts as conflict markers.

Repeat these steps for each target branch. Create the ticket only after every branch has a pull request or a reported failure.

In the main worktree, remember the repository root with `git rev-parse --show-toplevel`. The resolver is `.agents/skills/ydb-backport/scripts/resolve_log_conflicts.py` under that root. It rewrites each resolved file in place. How a hunk is rewritten is in references/log-conflicts.md.

Pick the upstream remote from `git remote -v`. Its fetch URL, with a trailing `.git` removed, is `https://github.com/ydb-platform/ydb`.

Fetch each commit in the main worktree and count its parents before creating a branch:

```bash
git fetch <upstream> <sha>
git rev-list --parents --max-count=1 <sha>
```

When that line has three or more fields, the commit is a merge. Stop and ask the user which single commit to cherry-pick.

```bash
git fetch <upstream> <branch>
git worktree add -b backport/<branch>-<utc> <path> <upstream>/<branch>
```

`<utc>` is the output of `date -u +%Y%m%dT%H%M%SZ`. `<path>` is a directory that does not exist yet. `git worktree add` creates it. Run the later git commands with `git -C <path>`. The main worktree stays put.

For each commit:

```bash
git -C <path> fetch <upstream> <sha>
git -C <path> cherry-pick <sha>
```

When the output contains the sentence `The previous cherry-pick is now empty`, run `git -C <path> cherry-pick --skip`.

On a conflict, list unmerged paths. The path is the text after the tab. Run the resolver on each path once.

```bash
git -C <path> ls-files -u
python3 <root>/.agents/skills/ydb-backport/scripts/resolve_log_conflicts.py <path>/<file>
```

The script keeps the cherry-picked side of every hunk and rewrites the file in place. The rules are in references/log-conflicts.md.

When the script exits with code 2, do not commit. Stop this branch and report the script's line.

When every hunk is `resolved` or the file is `clean`, stage the files and continue:

```bash
git -C <path> add <files>
git -C <path> cherry-pick --continue --no-edit
```

Read the push URL with `git remote get-url --push origin`. The owner is the path segment after `github.com/`. Strip a trailing `.git`.

```bash
git -C <path> push --set-upstream origin HEAD
```

When the push fails, stop and ask the user which remote to push to.

When the owner is `ydb-platform`, the head is `<branch-name>`. When the owner is another account, the head is `<owner>:<branch-name>`.

```bash
gh pr create --repo ydb-platform/ydb --base <branch> --head <head> --title "<title>" --body "<body>"
```

The title of one pull request is its `title` field. The title of one commit is `git log -1 --format=%s <sha>`. For several sources, join those titles with `, `. The pull request title is `[Backport <branch>] <titles>`.

The body lists the source SHAs and the upstream pull request URLs.

From the main worktree, after the pull request exists:

```bash
git worktree remove <path>
```

## Ticket

Create one issue with `create_issue`, unless the form description says to create one issue per branch.

1. Call `list_field_values` with `target` `issue`, `field` `type`, and `queue` `YDBBACKPORT`. Pass the returned `key` as `type`.
2. Set `queue` to `YDBBACKPORT` and `summary` from the source titles. Put the commits, the target branches, and every pull request URL in `description`.
3. Call `find_fields` with `target` `issue` and `queue` `YDBBACKPORT`. Pass `fields` using those ids. The `Commits` field gets the commit list. The text field `Target branches for the merge` gets the branch list. Do not set the enum field `Target branches`.
4. Do not set `priority` unless the user names one. Then call `list_field_values` for `priority` and pass a returned key.
5. Call `list_issue_links` on one recent `YDBBACKPORT` issue. When it has a GitHub remote link, copy `remoteLinks[].object.application.id` into `create_remote_link` as `origin`, and pass each pull request URL as `key`. When it has none, leave the URLs in the description.
6. Read the issue back with `get_issue`. Check the queue, the summary, and that the description contains each pull request URL. Leave the issue open.

## Report

List each target branch and its pull request URL. Give the YDBBACKPORT issue key and URL. Name any branch or commit you skipped, and why.
