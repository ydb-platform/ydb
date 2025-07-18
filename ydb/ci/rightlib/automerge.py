#!/usr/bin/env python3
import os
import datetime
import logging
import subprocess
import argparse
from typing import Optional
from github import Github
from github.PullRequest import PullRequest

automerge_pr_label = "automerge"
pr_label_fail = "automerge-blocked"
check_name = "checks_integrated"
failed_comment_mark = "<!--SyncFailed-->"


class PrAutomerger:
    def __init__(self, repo, token):
        self.repo_name = repo
        self.token = token
        self.gh = Github(login_or_token=self.token)
        self.repo = self.gh.get_repo(self.repo_name)
        self.dtm = datetime.datetime.now().strftime("%y%m%d-%H%M")
        self.logger = logging.getLogger("sync")
        self.workflow_url = None
        self.detect_env()

    def detect_env(self):
        if "GITHUB_RUN_ID" in os.environ:
            self.workflow_url = (
                f"{os.environ['GITHUB_SERVER_URL']}/{self.repo_name}/actions/runs/{os.environ['GITHUB_RUN_ID']}"
            )

    def get_latest_open_prs(self) -> Optional[PullRequest]:
        query = f"label:{automerge_pr_label} repo:{self.repo_name} is:pr state:open sort:created-desc"
        result = self.gh.search_issues(query).get_page(0)
        if result:
            return result
        return None

    def get_commit_check_status(self, sha):
        checks = self.repo.get_commit(sha).get_combined_status().statuses

        for c in checks:
            if c.context == check_name:
                return c
        return None

    def check_opened_pr(self, pr: PullRequest):
        pr_labels = [l.name for l in pr.labels]

        self.logger.info("check opened pr %r (labels %s)", pr, pr_labels)

        if pr_label_fail in pr_labels:
            self.logger.info("pr has %s label, exit", pr_label_fail)
            return

        check = self.get_commit_check_status(pr.head.sha)

        if check is None:
            self.logger.info("no %r checks found", check_name)
            return

        self.logger.info("check result %s", check)

        if check.state == "failure":
            self.logger.info("check failed")
            self.add_failed_comment(pr, f"Check `{check_name}` failed.")
            self.add_pr_failed_label(pr)
            return

        elif check.state == "success":
            self.logger.info("check success, going to merge")
            self.merge_pr(pr)
        else:
            self.logger.info("wait for success")

    def add_pr_failed_label(self, pr: PullRequest):
        pr.add_to_labels(pr_label_fail)

    def git_merge_pr(self, pr: PullRequest):
        self.git_run("clone", f"https://{self.token}@github.com/{self.repo_name}.git", "merge-repo")
        os.chdir("merge-repo")
        self.git_run("fetch", "origin", f"pull/{pr.number}/head:PR")
        self.git_run("checkout", pr.base.ref)

        commit_msg = f"Merge pull request #{pr.number} from {pr.head.user.login}/{pr.head.ref}"
        try:
            self.git_run("merge", "PR", "-m", commit_msg)
        except subprocess.CalledProcessError:
            self.add_failed_comment(pr, "Unable to merge PR.")
            self.add_pr_failed_label(pr)
            return False

        try:
            self.git_run("push")
        except subprocess.CalledProcessError:
            self.add_failed_comment(pr, "Unable to push merged revision.")
            self.add_pr_failed_label(pr)
            return False

    def merge_pr(self, pr: PullRequest):
        self.logger.info("start merge %s into %s", pr, pr.base.ref)
        if not self.git_merge_pr(pr):
            self.logger.info("unable to merge PR")
            return
        self.logger.info("deleting ref %r", pr.head.ref)
        self.repo.get_git_ref(f"heads/{pr.head.ref}").delete()
        body = f"The PR was successfully merged into {pr.base.ref} using workflow"
        pr.create_issue_comment(body=body)

    def add_failed_comment(self, pr: PullRequest, text: str):
        text += f" All future check are suspended, please remove the `{pr_label_fail}` label to enable checks."
        if self.workflow_url:
            text += f" Sync workflow logs can be found [here]({self.workflow_url})."
        pr.create_issue_comment(f"{failed_comment_mark}\n{text}")

    def git_run(self, *args):
        args = ["git"] + list(args)

        self.logger.info("run: %r", args)
        try:
            output = subprocess.check_output(args).decode()
        except subprocess.CalledProcessError as e:
            self.logger.error(e.output.decode())
            raise
        else:
            self.logger.info("output:\n%s", output)
        return output

    def git_revparse_head(self):
        return self.git_run("rev-parse", "HEAD").strip()

    def cmd_check_pr(self):
        prs = self.get_latest_open_prs()

        if prs is not None:
            for pr in prs:
                self.check_opened_pr(pr.as_pull_request())
        else:
            self.logger.info("No open PRs found")


def main():
    log_fmt = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    logging.basicConfig(format=log_fmt, level=logging.DEBUG)
    repo = os.environ["REPO"]
    token = os.environ["TOKEN"]

    syncer = PrAutomerger(repo, token)

    syncer.cmd_check_pr()


if __name__ == "__main__":
    main()
