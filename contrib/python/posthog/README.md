# PostHog Python

<p align="center">
  <img alt="posthoglogo" src="https://user-images.githubusercontent.com/65415371/205059737-c8a4f836-4889-4654-902e-f302b187b6a0.png">
</p>
<p align="center">
   <a href="https://pypi.org/project/posthog/"><img alt="pypi installs" src="https://img.shields.io/pypi/v/posthog"/></a>
   <img alt="GitHub contributors" src="https://img.shields.io/github/contributors/posthog/posthog-python">
  <img alt="GitHub commit activity" src="https://img.shields.io/github/commit-activity/m/posthog/posthog-python"/>
  <img alt="GitHub closed issues" src="https://img.shields.io/github/issues-closed/posthog/posthog-python"/>
</p>

Please see the [Python integration docs](https://posthog.com/docs/integrations/python-integration) for details.

## Python Version Support

| SDK Version   | Python Versions Supported    | Notes                      |
| ------------- | ---------------------------- | -------------------------- |
| 7.3.1+        | 3.10, 3.11, 3.12, 3.13, 3.14 | Added Python 3.14 support  |
| 7.0.0 - 7.0.1 | 3.10, 3.11, 3.12, 3.13       | Dropped Python 3.9 support |
| 4.0.1 - 6.x   | 3.9, 3.10, 3.11, 3.12, 3.13  | Python 3.9+ required       |

## Development

### Testing Locally

We recommend using [uv](https://docs.astral.sh/uv/). It's super fast.

1. Run `uv venv env` (creates virtual environment called "env")
   - or `python3 -m venv env`
2. Run `source env/bin/activate` (activates the virtual environment)
3. Run `uv sync --extra dev --extra test` (installs the package in develop mode, along with test dependencies)
   - or `pip install -e ".[dev,test]"`
4. you have to run `pre-commit install` to have auto linting pre commit
5. Run `make test`
6. To run a specific test do `pytest -k test_no_api_key`

## PostHog recommends `uv` so...

```bash
uv python install 3.12
uv python pin 3.12
uv venv
source env/bin/activate
uv sync --extra dev --extra test
pre-commit install
make test
```

### Running Locally

Assuming you have a [local version of PostHog](https://posthog.com/docs/developing-locally) running, you can run `python3 example.py` to see the library in action.

### Testing changes locally with the PostHog app

You can run `make prep_local`, and it'll create a new folder alongside the SDK repo one called `posthog-python-local`, which you can then import into the posthog project by changing pyproject.toml to look like this:

```toml
dependencies = [
    ...
    "posthoganalytics" #NOTE: no version number
    ...
]
...
[tools.uv.sources]
posthoganalytics = { path = "../posthog-python-local" }
```

This'll let you build and test SDK changes fully locally, incorporating them into your local posthog app stack. It mainly takes care of the `posthog -> posthoganalytics` module renaming. You'll need to re-run `make prep_local` each time you make a change, and re-run `uv sync --active` in the posthog app project.

## Releasing

This repository uses [Sampo](https://github.com/bruits/sampo) for versioning, changelogs, and publishing to crates.io.

1. When making changes, include a changeset: `sampo add`
2. Create a PR with your changes and the changeset file
3. Add the `release` label and merge to `main`
4. Approve the release in Slack when prompted — this triggers version bump, crates.io publish, git tag, and GitHub Release

You can also trigger a release manually via the workflow's `workflow_dispatch` trigger (still requires pending changesets).
