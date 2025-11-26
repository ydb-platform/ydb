# Auto Cherry-pick for YDB

This directory contains the automated cherry-pick functionality for YDB PRs.

## Overview

The auto cherry-pick feature provides a convenient way to cherry-pick merged PRs to stable branches using simple GitHub comments.

## Components

### Workflows

1. **auto_cherry_pick_suggest.yml** - Automatically suggests cherry-pick after PR merge
2. **auto_cherry_pick_comment.yml** - Handles `/cherry-pick` commands in PR comments

### Scripts

1. **auto_cherry_pick_suggest.py** - Posts suggestion comments with available stable branches
2. **auto_cherry_pick_comment.py** - Processes cherry-pick commands and creates PRs

## How it works

1. **After PR merge**: The suggestion workflow automatically posts a comment with available stable branches
2. **Command usage**: Users comment `/cherry-pick stable-25-1 stable-24-4` to trigger cherry-pick
3. **Automated process**: The system creates new branches, applies changes, and creates PRs
4. **Error handling**: Reports conflicts and failures with detailed information

## Usage Examples

```bash
# Cherry-pick to multiple branches
/cherry-pick stable-25-1 stable-24-4 stable-23-3

# Cherry-pick to single branch  
/cherry-pick stable-25-1

# Case insensitive
/CHERRY-PICK stable-25-1
```

## Permissions

- PR authors can always trigger cherry-pick for their own PRs
- Repository collaborators with write access can trigger cherry-pick for any PR

## Error Handling

- Validates branch existence
- Detects merge conflicts
- Reports detailed error messages
- Provides fallback to manual cherry-pick process

## Configuration

The scripts automatically detect stable branches using the pattern `stable-*` and present the 5 most recent ones for selection.

## Dependencies

- PyGithub==2.5.0
- Git
- GitHub Actions environment with YDBOT_TOKEN