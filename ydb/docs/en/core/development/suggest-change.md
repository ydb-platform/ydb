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

Create working dir:
```
mkdir -p ~/ydbwork
cd ~/ydbwork
```

To work on the YDB code changes, you need to create a fork repository under your GitHub account, and clone it locally. 

{% list tabs %}

- GitHub CLI:

There's a single GitHub CLI command which does all of that together:

```
gh repo fork ydb-platform/ydb --default-branch-only --clone
```

Once completed, you have a YDB Git repository fork cloned to `~/ydbwork/ydb`.

- git

On https://github.com/ydb-platform/ydb press Fork (Fork your own copy of ydb-platform/ydb).
```
git clone git@github.com:{your_name}/ydb.git
```
{% endlist %}

Forking a repository is an instant action, however cloning to the local machine takes some time to transfer about 650 MB of repository data over the network.

### Configure commit authorship {#author}

Run the following command from your repository directory to set up your name and email for commits pushed using Git:

```
cd ~/ydbwork/ydb
```
```
git config --global user.name "Marco Polo"
git config --global user.email "marco@ydb.tech"
```

## Working on a feature {#feature}

To start working on a feature, ensure the steps specified in the [Setup the environment](#envsetup) section above are completed.

### Refresh trunk {#fork_sync}

Usually you need a fresh trunk revision to branch from. Sync fork to obtain it, running the following command:

```
gh repo sync your_github_login/ydb -s ydb-platform/ydb
```

This statement performs sync remotely on GitHub. Pull the changes to the local repository then:

```
cd ~/ydbwork/ydb
```
```
git checkout main
git pull
```

### Create a development branch {#create_devbranch}

Create a development branch using Git (replace "feature42" with your branch name), and assign upstream for it:

```
git checkout -b feature42
git push --set-upstream origin feature42
```

### Make changes and commits {#commit}

Edit files locally, use standard Git commands to add files, verify status, make commits, and push changes to your fork repository:

```
git add .
```

```
git status
```

```
git commit -m "Implemented feature 42"
```

```
git push
```

### Create a pull request to the official repository {#create_pr}

When the changes are completed and locally tested (see [Ya Build and Test](build-ya.md)), run the following command from your repository root to submit a Pull Request to the YDB official repository:

```
cd ~/ydbwork/ydb
```
```
gh pr create --title "Feature 42 implemented"
```

After answering some questions, the Pull Request will be created and you will get a link to its page on GitHub.com.

### Precommit checks {#precommit_checks}

Prior to merging, the precommit checks are run for the Pull Request. You can see its status on the Pull Request page.

As part of the precommit checks, the YDB CI builds artifacts and runs all the tests, providing the results as a comment to the Pull Request.

If you are not a member of the YDB team, build/test checks do not run until a team member reviews your changes and approves the PR for tests by assigning a label 'Ok-to-test'.

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

If you have conflicts on the Pull Request, you may rebase your changes on top of the actual trunk from the official repository. To do so, [refresh trunk](#fork_sync) in your fork, pull the `main` branch state to the local machine, and run the rebase command:

```
# Assuming your active branch is your development branch
gh repo sync your_github_login/ydb -s ydb-platform/ydb
git fetch origin main:main
git rebase main
```
