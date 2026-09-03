# Development process: working on a change for {{ ydb-short-name }}

This section provides a step-by-step guide that will help you perform the necessary configuration steps and learn how to make changes to the {{ ydb-short-name }} project. You don't have to strictly follow this guide; you can develop your own approach based on the provided information.

## Set up the environment {#envsetup}

### GitHub account {#GitHub_login}

You need to have a GitHub account to suggest any changes to the {{ ydb-short-name }} source code. Register at [GitHub](https://github.com/) if haven't done it yet.

### SSH key pair {#ssh_key_pair}

* To connect to GitHub, you can use: ssh/token/ssh from yubikey/password, etc. The recommended method is ssh keys.
* If you don't have already created keys (or yubikey), then just create new keys. Full instructions are on [this GitHub page](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent#generating-a-new-ssh-key).
* If you have your own private keys and use skotty as an ssh-agent:

  * Add keys to skotty with command [ssh-add](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent#adding-your-ssh-key-to-the-ssh-agent)
  * Edit `~/.skotty/config.yaml` file by adding a section:


    ```yaml
    keys_order:
        - added
        - insecure
        - legacy
        - secure
    ```

* If you have a yubikey, you can use the legacy key from the yubikey:

  * Suppose you already have a configured yubikey (or you configured yubikey locally)
  * On your laptop: `skotty ssh keys`
  * Upload `legacy@yubikey` ssh key to GitHub ([via UI](https://github.com/settings/keys))
  * test connection on laptop: `ssh -T git@github.com`

#### Remote development

If you are developing on a remote dev host you can use the key from your laptop (generated keys or keys from yubikey). You need to configure key forwarding. (Full instructions are on  [this GitHub page](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/using-ssh-agent-forwarding) ).

Suppose that your remote computer is dev123456.search.yandex.net.

* on your laptop add ssh forwarding (`~/.ssh/config`):


```text
Host dev123456.search.yandex.net
    ForwardAgent yes
```


* on remote dev host add to `~/.bashrc`:


```bash
if [[ -S "$SSH_AUTH_SOCK" && ! -h "$SSH_AUTH_SOCK" ]]; then
    ln -sf "$SSH_AUTH_SOCK" ~/.ssh/ssh_auth_sock;
fi
export SSH_AUTH_SOCK=~/.ssh/ssh_auth_sock;
```


* test connection: `ssh -T git@github.com`

### Git CLI {#git_cli}

You must have the git command-line utility installed to run commands from the console. Visit the [Downloads](https://git-scm.com/downloads) page of the official website for installation instructions.

To install it on Linux/Ubuntu, run:


```bash
sudo apt-get update
sudo apt-get install git
```


### Build dependencies {#build_dependencies}

Some libraries must be installed on the developer's computer.

To install them on Linux/Ubuntu, run:


```bash
sudo apt-get update
sudo apt-get install libidn11-dev libaio-dev libc6-dev
```


### GitHub CLI (optional) {#gh_cli}

Using GitHub CLI allows you to create pull requests and manage the repository from the command line. You can also use GitHub UI for such actions.

Install GitHub CLI as described [at the home page](https://cli.github.com/). For Linux Ubuntu, you can go directly to [the installation instructions](https://github.com/cli/cli/blob/trunk/docs/install_linux.md#debian-ubuntu-linux-raspberry-pi-os-apt).

Configure authentication:


```bash
gh auth login
```


You will be asked several questions in interactive mode; answer them as follows:

| Question | Answer |
| --- | --- |
| What account do you want to log into? | **GitHub.com** |
| What is your preferred protocol for Git operations? | **SSH** |
| Upload your SSH public key to your GitHub account? | Select the file with the public key (extension `.pub`) from those created in the step ["Create an SSH key pair"](#ssh_key_pair), for example **/home/user/.ssh/id_ed25519.pub** |
| Title for your SSH key | **GitHub CLI** (leave the default value) |
| How would you like to authenticate GitHub CLI | **Paste your authentication token** |

After the last answer, you will be prompted to enter a token that you can generate in the GitHub user interface:


```text
Tip: you can generate a Personal Access Token here https://github.com/settings/tokens
The minimum required scopes are 'repo', 'read:org', 'admin:public_key'.
? Paste your authentication token:
```


Open the [https://github.com/settings/tokens](https://github.com/settings/tokens), click on "Generate new token" / "Classic", tick FOUR boxes:

* **Box `workflow`**
* Three others, as indicated in the hint: "repo", "admin:public_key" and "read:org" (in the "admin:org" section)

And paste the displayed token to complete the GitHub CLI setup.

### Fork and clone repository {#fork_create}

The official {{ ydb-short-name }} repository is [https://github.com/ydb-platform/ydb](https://github.com/ydb-platform/ydb), located under the YDB organization account `ydb-platform`.

To work on the {{ ydb-short-name }} code changes, you need to create a fork repository under your GitHub account. Create a fork by pressing the `Fork` button on the [official {{ ydb-short-name }} repository page](https://github.com/ydb-platform/ydb).

After your fork is created, create a local git repository with two remotes:

- `official`: official {{ ydb-short-name }} repository, for main and stable branches
- `fork`: your {{ ydb-short-name }} repository fork, for your development branches


```bash
mkdir -p ~/ydbwork
cd ~/ydbwork
git clone -o official git@github.com:ydb-platform/ydb.git
```


```bash
cd ydb
git remote add fork git@github.com:{your_github_user_name}/ydb.git
```


Once completed, you have a {{ ydb-short-name }} Git repository set up in `~/ydbwork/ydb`.

Forking a repository is an instant action, but cloning it to a local computer takes some time to transfer about 650 MB of repository data over the network.

Next, let's configure the default `git push` behavior:


```bash
git config push.default current
git config push.autoSetupRemote true
```


This way, `git push {remote}` command will automatically set upstream for the current branch to the `{remote}` and consecutive `git push` commands will only push current branch.

If you intend to use GitHub CLI, then set `ydb-platform/ydb` as a default repository for GitHub CLI:


```bash
gh repo set-default ydb-platform/ydb
```


### Configure commit authorship {#author}

Run the following command to specify your name and email address for commits sent using Git (replace the username and email with yours):


```bash
git config --global user.name "Marco Polo"
git config --global user.email "marco@ydb.tech"
```


## Working on a feature {#feature}

To start working on a feature, ensure the steps specified in the [Setup the environment](#envsetup) section above are completed.

### Refresh trunk {#fork_sync}

Usually you need a fresh revision to branch from. Sync your local `main` branch by running the following command in the repository:

If your current local branch is `main`:


```bash
git pull --ff-only official main
```


If your current local branch is not `main`:


```bash
cd ~/ydbwork/ydb
git fetch official main:main
```


This command updates your local `main` branch without checking it out.

### Create a development branch {#create_devbranch}

Create a development branch using Git (replace "feature42" with the name of your new branch):


```bash
git checkout -b feature42
```


### Make changes and commits {#commit}

Edit files locally, use standard Git commands to add files, check status, make commits, and push changes to your fork of the repository:


```bash
git add .
git status
```


```bash
git commit -m "Implemented feature 42"
git push fork
```


Subsequent pushes do not require upstream or a branch name:


```bash
git push
```


### Create a pull request to the official repository {#create_pr}

When the changes are completed and locally tested (see [Ya Build and Test](build-ya.md)), create Pull Request.

{% list tabs %}

- GitHub UI

  Visit your branch's page on GitHub.com (`https://github.com/{your_github_user_name}/ydb/tree/{branch_name}`), press `Contribute` and then `Open Pull Request`.
  You can also use the link in the `git push` output to open a Pull Request:


  ```text
  ...
  remote: Resolving deltas: 100% (1/1), completed with 1 local object.
  remote:
  remote: Create a pull request for '{branch_name}' on GitHub by visiting:
  remote:      https://github.com/{your_github_user_name}/test/pull/new/{branch_name}
  ...
  ```

- GitHub CLI

  Install and configure [GitHub CLI](https://cli.github.com/).


  ```bash
  cd ~/ydbwork/ydb
  ```


  ```bash
  gh pr create --title "Feature 42 implemented"
  ```


  After answering some questions, the Pull Request will be created, and you will receive a link to its page on GitHub.com.

{% endlist %}

### Fill in the Pull Request description {#create_pr_desc}

When creating a Pull Request, the description will be filled with the text from the template, which you need to edit:

1. **Changelog Entry.** In this block, you should add a description of the change for end users of the system (see [requirements](#changelog_entry_req)). The contents of this block will be published in the [list of changes](../changelog-server.md) if the PR is merged.
2. **Description for reviewers.** You can add a link to the task and any additional information that will be useful for reviewing your change to this block. The contents of this block will not be included in the list of changes.

#### Requirements for Changelog Entry {#changelog_entry_req}

The message in the Changelog Entry must meet the following requirements:

- Must be written in English.
- Rely on the terms used in the [glossary](../concepts/glossary.md).
- Describe what changed in the system's operation for the end user.

### Precommit checks {#precommit_checks}

Before merging changes, pre-commit checks of the Pull Request are run.

For {{ ydb-short-name }} code changes, pre-commit checks build artifacts and run the tests described in the `ya.make` files. The build and tests are run on a special merge commit that merges your changes with the current `main` branch.

You can see the status of checks on the Pull Request page. Also, key information about the progress of the {{ ydb-short-name }} build and tests and the current status is published in PR comments.

If you are not a member of the {{ ydb-short-name }} team, build/test checks do not run until a team member reviews your changes and approves the PR for tests by assigning a label `ok-to-test`.

Checks are restarted every time new changes are pushed; the previous check is interrupted if it has not yet completed. Each iteration of checks creates its own comment on the PR page, so the check history is preserved there.

If you are a member of the {{ ydb-short-name }} team, you can also restart checks on a new merge commit without pushing. To do so, add label `rebase-and-check` to the PR.

### Test results {#test-results}

You can click on the number of tests in different sections of the test results comment to go to a simple HTML test report. In this report, you can see which tests passed or failed and get access to their logs.

### Test history {#test_history}

Each time when tests are run by the {{ ydb-short-name }} CI, their results are uploaded to the [test history application](https://nebius.testmo.net/projects/view/1). There's a link "Test history" in the comment with test results heading to the page with the relevant run in this application.

In "Test history", {{ ydb-short-name }} team members can view test runs, search for tests, view logs, and compare them across different test runs. If a test fails on a pre-commit check, its history shows whether the failure was caused by this change or the test was already broken.

### Review and merge {#review}

A Pull Request can be merged after receiving approval from a {{ ydb-short-name }} team member. Comments are used for communication. After approval, a {{ ydb-short-name }} team member clicks the "Merge" button.

### Update changes {#update}

If a Pull Request is open in your repository for a development branch, it will be updated every time you push to that branch, and checks will be restarted.

### Rebase changes {#rebase}

If you have conflicts on the Pull Request, you may rebase your changes on top of the actual trunk from the official repository. To do so, [refresh main](#fork_sync) branch state on your local machine, and run the rebase command:


```bash
# It is assumed that your active branch is your development branch.
git fetch official main:main
git rebase main
```


### Cherry-picking fixes to the stable branch {#cherry_pick_stable}

When you need to move a patch to a stable branch, branch off from the stable branch:


```bash
git fetch official
git checkout -b "cherry-pick-fix42" official/stable-24-1
```


Then use cherry-pick to move the patch and push the branch to your fork:


```bash
git cherry-pick {fixes_commit_hash}
git push fork
```


And then create a PR from your branch with the cherry-picked fix to the stable branch. It is done similarly to opening a PR to `main`, but make sure to double-check the target branch.

If you are using GitHub CLI, pass `-B` argument to specify the target branch:


```bash
gh pr create --title "Title" -B stable-24-1
```


{% include [career](./_includes/career.md) %}
