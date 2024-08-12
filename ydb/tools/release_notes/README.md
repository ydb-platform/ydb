## How To Generate Changelog

Generate github token:
* https://github.com/settings/tokens?type=beta - generate new token and keep all checkboxes unchecked, no scopes need to be enabled.

Dependencies:
```
sudo apt-get update
sudo apt-get install git python3 python3-fuzzywuzzy python3-github
python3 changelog.py -h
```

Usage example:

Note: The working directory is ydb/tools/release_notes

```bash
export GITHUB_TOKEN="<your token>"

git fetch --tags # changelog.py depends on having the tags available, this will fetch them.
                 # If you are working from a branch in your personal fork, then you may need `git fetch --all`

python3 changelog.py --output=changelog-v24.1.14.md 24.1.14 --gh-user-or-token="$GITHUB_TOKEN" --from 23.4.11
```
