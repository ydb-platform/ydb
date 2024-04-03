# Development process: working on a change for YDB

This section contains a step-by-step scenario which helps you complete necessary configuration steps, and learn how to bring a change to the YDB project. This scenario does not have to be strictly followed, you may develop your own approach based on the provided information.

## Set up the environment {#envsetup}

### GitHub account {#github_login}

You need to have a GitHub account to suggest any changes to the YDB source code. Register at [github.com](https://github.com/) if haven't done it yet.

### SSH key pair {#ssh_key_pair}

* In general to connect to github you can use: ssh/token/ssh from yubikey/password etc. Recommended method is ssh keys.
* If you don't have already created keys (or yubikey), then just create new keys. Full instructions are on [this GitHub page](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent#generating-a-new-ssh-key).
* If you have a yubikey, you can use the legacy key from the yubikey:
  * Let's assume you have already configured yubikey (or configure yubikey locally)
  * On your laptop: `skotty ssh keys`
  * Upload `legacy@yubikey` ssh key to github (over ui: https://github.com/settings/keys) 
  * test connection on laptop: `ssh -T git@github.com`

#### Remote development 
If you are developing on a remote dev host you can use the key from your laptop (generated keys or keys from yubikey). You need to configure key forwarding. (Full instructions are on  [this GitHub page](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/using-ssh-agent-forwarding) ).

Suppose your remote machine is dev123456.search.yandex.net.

* on your laptop add ssh forwarding (`~/.ssh/config`):
```
Host dev123456.search.yandex.net
    ForwardAgent yes
```
* on remote dev host add to `~/.bashrc`:
```
if [[ -S "$SSH_AUTH_SOCK" && ! -h "$SSH_AUTH_SOCK" ]]; then
    ln -sf "$SSH_AUTH_SOCK" ~/.ssh/ssh_auth_sock;
fi
export SSH_AUTH_SOCK=~/.ssh/ssh_auth_sock;
```
* test connection: `ssh -T git@github.com`

### Git CLI {#git_cli}

You need to have the `git` command-line utility installed to run commands from the console. Visit the [Downloads](https://git-scm.com/downloads) page of the official website for installation instructions.

To install it under Linux/Ubuntu run:

```
sudo apt-get update
sudo apt-get install git
```

### Build dependencies {#build_dependencies}

You need to have some libraries installed on the development machine.

To install it under Linux/Ubuntu run:

```
sudo apt-get update
sudo apt-get install libidn11-dev libaio-dev libc6-dev
```

### GitHub CLI (optional) {#gh_cli}

Using GitHub CLI enables you to create Pull Requests and manage repositories from a command line. You can also use GitHub UI for such actions.

Install GitHub CLI as described [at the home page](https://cli.github.com/). For Linux Ubuntu, you can go directly to [https://github.com/cli/cli/blob/trunk/docs/install_linux.md#debian-ubuntu-linux-raspberry-pi-os-apt](https://github.com/cli/cli/blob/trunk/docs/install_linux.md#debian-ubuntu-linux-raspberry-pi-os-apt).

Run authentication configuration:

```
gh auth login
```
You will be asked several questions interactively, answer them as follows:

|Question|Answer|
|--|--|
|What account do you want to log into?|**GitHub.com**|
|What is your preferred protocol for Git operations?|**SSH**|
|Upload your SSH public key to your GitHub account?|Choose a file with a public key (extention `.pub`) of those created on the ["Create SSH key pair"](#ssh_key_pair) step, for instance **/home/user/.ssh/id_ed25519.pub**|
|Title for your SSH key|**GitHub CLI** (leave default)|
|How would you like to authenticate GitHub CLI|**Paste your authentication token**|

After the last answer, you will be asked for a token which you can generate in the GitHub UI:

```
Tip: you can generate a Personal Access Token here https://github.com/settings/tokens
The minimum required scopes are 'repo', 'read:org', 'admin:public_key'.
? Paste your authentication token:
```

Open the [https://github.com/settings/tokens](https://github.com/settings/tokens), click on "Generate new token" / "Classic", tick FOUR boxes:
* **Box `workflow`**
* Three others as adivised in the tip: "repo", "admin:public_key" and "read:org" (under "admin:org")

And copy-paste the shown token to complete the GitHub CLI configuration.

### Fork and clone repository {#fork_create}

YDB official repository is [https://github.com/ydb-platform/ydb](https://github.com/ydb-platform/ydb), located under the YDB organization account `ydb-platform`.

To work on the {{ ydb-short-name }} code changes, you need to create a fork repository under your GitHub account. Create a fork by pressing the `Fork` button on the [official {{ ydb-short-name }} repository page](https://github.com/ydb-platform/ydb).

After your fork is set up, create a local git repository with two remotes:
- `official`: official {{ ydb-short-name }} repository, for main and stable branches
- `fork`: your {{ ydb-short-name }} repository fork, for your development branches

```
mkdir -p ~/ydbwork
cd ~/ydbwork
git clone -o official git@github.com:ydb-platform/ydb.git
```

```
cd ydb
git remote add fork git@github.com:{your_github_user_name}/ydb.git
```

Once completed, you have a {{ ydb-short-name }} Git repository set up in `~/ydbwork/ydb`.

Forking a repository is an instant action, however cloning to the local machine takes some time to transfer about 650 MB of repository data over the network.

Next, let's configure the default `git push` behavior:

```
git config push.default current
git config push.autoSetupRemote true
```

This way, `git push {remote}` command will automatically set upstream for the current branch to the `{remote}` and consecutive `git push` commands will only push current branch.

If you intend to use GitHub CLI, then set `ydb-platform/ydb` as a default repository for GitHub CLI:
```
gh repo set-default ydb-platform/ydb
```

### Configure commit authorship {#author}

Run the following command to set up your name and email for commits pushed using Git (replace example name and email with your real ones):

```
git config --global user.name "Marco Polo"
git config --global user.email "marco@ydb.tech"
```

## Working on a feature {#feature}

To start working on a feature, ensure the steps specified in the [Setup the environment](#envsetup) section above are completed.

### Refresh trunk {#fork_sync}

Usually you need a fresh revision to branch from. Sync your local `main` branch by running the following command in the repository:

If your current local branch is `main`:

```
git pull --ff-only official main
```

If your current local branch is not `main`:

```
cd ~/ydbwork/ydb
git fetch official main:main
```

This command updates your local `main` branch without checking it out.

### Create a development branch {#create_devbranch}

Create a development branch using Git (replace "feature42" with a name for your new branch):

```
git checkout -b feature42
```

### Make changes and commits {#commit}

Edit files locally, use standard Git commands to add files, verify status, make commits, and push changes to your fork repository:

```
git add .
git status
```

```
git commit -m "Implemented feature 42"
git push fork
```

Consecutive pushes do not require an upstream or a branch name:
```
git push
```

### Create a pull request to the official repository {#create_pr}

When the changes are completed and locally tested (see [Ya Build and Test](build-ya.md)), create Pull Request.

{% list tabs %}

- GitHub UI

  Visit your branch's page on GitHub.com (https://github.com/{your_github_user_name}/ydb/tree/{branch_name}), press `Contribute` and then `Open Pull Request`.
  You can also use the link in the `git push` output to open a Pull Request:
  
  ```
  ...
  remote: Resolving deltas: 100% (1/1), completed with 1 local object.
  remote: 
  remote: Create a pull request for '{branch_name}' on GitHub by visiting:
  remote:      https://github.com/{your_github_user_name}/test/pull/new/{branch_name}
  ... 
  ```

- GitHub CLI

  Install and configure [GitHub CLI](https://cli.github.com/).
  ```
  cd ~/ydbwork/ydb
  ```
  
  ```
  gh pr create --title "Feature 42 implemented"
  ```
  
  After answering some questions, the Pull Request will be created and you will get a link to its page on GitHub.com.

{% endlist %}

### Precommit checks {#precommit_checks}

Prior to merging, the precommit checks are run for the Pull Request.

For changes in the {{ ydb-short-name }} code, precommit checks build {{ ydb-short-name }} artifacts, and run tests as described in `ya.make` files. Build and test run on a specific commit which merges your changes to the current `main` branch. If there are merge conflicts, build/test checks cannot be run, and you need to rebase your changes as described [below](#rebase).

You can see the checks status on the Pull Request page. Also, key information for {{ ydb-short-name }} build/test checks progress and status is published to the comments of the Pull Ruquest.

If you are not a member of the YDB team, build/test checks do not run until a team member reviews your changes and approves the PR for tests by assigning a label `ok-to-test`.

Checks are restarted every time you push changes to the pull request, cancelling the previous run if it's still in progress. Each iteration of checks produces its own comment on the pull request page, so you can see the history of checks.

If you are a member of the YDB team, you can also restart checks on a new merge commit without pushing. To do so, add label `rebase-and-check` to the PR.

### Test results {#test-results}

You can click on the test amounts in different sections of the test results comment to get to the simple HTML test report. In this report you can see which tests have been failed/passed, and get to their logs.

### Test history {#test_history}

Each time when tests are run by the YDB CI, their results are uploaded to the [test history application](https://nebius.testmo.net/projects/view/1). There's a link "Test history" in the comment with test results heading to the page with the relevant run in this application.

In the "Test History" YDB team members can browse test runs, search for tests, see the logs, and compare them between different test runs. If some test is failed in a particular precommit check, it can be seen in its history if this failure had been introduced by the change, or the test had been broken/flaky earlier.

### Review and merge {#review}

The Pull Request can be merged after obtaining an approval from the YDB team member. Comments are used for communication. Finally a reviewer from the YDB team clicks on the 'Merge' button.

### Update changes {#update}

If there's a Pull Request opened for some development branch in your repository, it will update every time you push to that branch, restarting the checks.

### Rebase changes {#rebase}

If you have conflicts on the Pull Request, you may rebase your changes on top of the actual trunk from the official repository. To do so, [refresh main](#fork_sync) branch state on your local machine, and run the rebase command:

```
# Assuming your active branch is your development branch
git fetch official main:main
git rebase main
```

### Cherry-picking fixes to the stable branch {#cherry_pick_stable}

When required to cherry-pick a fix to the stable branch, first branch off of the stable branch:

```
git fetch official
git checkout -b "cherry-pick-fix42" official/stable-24-1
```

Then cherry-pick the fix and push the branch to your fork:

```
git cherry-pick {fixes_commit_hash}
git push fork
```

And then create a PR from your branch with the cherry-picked fix to the stable branch. It is done similarly to opening a PR to `main`, but make sure to double-check the target branch.

If you are using GitHub CLI, pass `-B` argument to specify the target branch:

```
gh pr create --title "Title" -B stable-24-1
```
