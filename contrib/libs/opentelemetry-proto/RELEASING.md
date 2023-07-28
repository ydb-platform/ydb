# How to Create a Release of OpenTelemetry Proto (for Maintainers Only)

## Tagging the Release

Our release branches follow the naming convention of `v<major>.<minor>.x`, while
the tags include the patch version `v<major>.<minor>.<patch>`. For example, the
same branch `v0.3.x` would be used to create all `v0.3` tags (e.g. `v0.3.0`,
`v0.3.1`).

In this section upstream repository refers to the main opentelemetry-proto
github repository.

Before any push to the upstream repository you need to create a [personal access
token](https://help.github.com/articles/creating-a-personal-access-token-for-the-command-line/).

1. Create the release branch and push it to GitHub:

    ```bash
    MAJOR=0 MINOR=3 PATCH=0 # Set appropriately for new release
    git checkout -b v$MAJOR.$MINOR.x main
    git push upstream v$MAJOR.$MINOR.x
    ```

2. Enable branch protection for the new branch, if you have admin access.
   Otherwise, let someone with admin access know that there is a new release
   branch.

    - Open the branch protection settings for the new branch, by following
      [Github's instructions](https://help.github.com/articles/configuring-protected-branches/).
    - Copy the settings from a previous branch, i.e., check
      - `Protect this branch`
      - `Require pull request reviews before merging`
      - `Require status checks to pass before merging`
      - `Include administrators`

      Enable the following required status checks:
      - `cla/linuxfoundation`
      - `ci/circleci: build`
    - Uncheck everything else.
    - Click "Save changes".

3. For `vMajor.Minor.x` branch:

    - Create and push a tag:

    ```bash
    git checkout v$MAJOR.$MINOR.x
    git pull upstream v$MAJOR.$MINOR.x
    git tag -a v$MAJOR.$MINOR.$PATCH -m "Version $MAJOR.$MINOR.$PATCH"
    git push upstream v$MAJOR.$MINOR.$PATCH
    ```

## Patch Release

All patch releases should include only bug-fixes, and must avoid
adding/modifying the public APIs. To cherry-pick one commit use the following
instructions:

- Create and push a tag:

```bash
COMMIT=1224f0a # Set the right commit hash.
git checkout -b cherrypick v$MAJOR.$MINOR.x
git cherry-pick -x $COMMIT
git commit -a -m "Cherry-pick commit $COMMIT"
```

- Go through PR review and merge it to GitHub v$MAJOR.$MINOR.x branch.

- Tag a new patch release when all commits are merged.

## Announcement

Once deployment is done, go to Github [release
page](https://github.com/open-telemetry/opentelemetry-proto/releases), press
`Draft a new release` to write release notes about the new release.

You can use `git log upstream/v$MAJOR.$((MINOR-1)).x..upstream/v$MAJOR.$MINOR.x --graph --first-parent`
or the Github [compare tool](https://github.com/open-telemetry/opentelemetry-proto/compare/)
to view a summary of all commits since last release as a reference.

In addition, you can refer to [CHANGELOG.md](CHANGELOG.md)
for a list of major changes since last release.

## Update release versions in documentations and CHANGELOG files

After releasing is done, you need to update [README.md](README.md) and [CHANGELOG.md](CHANGELOG.md).

Create a PR to mark the new release in [CHANGELOG.md](CHANGELOG.md) on main branch.
