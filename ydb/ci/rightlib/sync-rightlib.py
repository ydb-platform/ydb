#!/usr/bin/env python3
import os
import datetime
import logging
import subprocess
from typing import Optional
from github import Github
from github.PullRequest import PullRequest


class RightlibSync:
    pr_label_rightlib = "rightlib"
    pr_label_fail = "rightlib-fail"
    rightlib_sha_file = "ydb/ci/rightlib.txt"
    check_name = "checks_integrated"
    failed_comment_mark = "<!--RightLibSyncFailed-->"
    rightlib_check_status_name = "rightlib-merge"

    def __init__(self, repo, base_branch, head_branch,  token):
        self.repo_name = repo
        self.base_branch = base_branch
        self.head_branch = head_branch
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

    def rightlib_latest_repo_sha(self):
        return self.repo.get_branch(self.head_branch).commit.sha

    def rightlib_sha_file_contents(self, ref):
        return self.repo.get_contents(self.rightlib_sha_file, ref=ref).decoded_content.decode().strip()

    def rightlib_latest_sync_commit(self):
        return self.rightlib_sha_file_contents(ref=self.base_branch)

    def get_latest_open_pr(self) -> Optional[PullRequest]:
        query = f"label:{self.pr_label_rightlib} repo:{self.repo_name} base:{self.base_branch} is:pr state:open sort:created-desc"
        result = self.gh.search_issues(query).get_page(0)
        if result:
            return result[0].as_pull_request()
        return None

    def get_commit_check_status(self, sha):
        checks = self.repo.get_commit(sha).get_combined_status().statuses

        for c in checks:
            if c.context == self.check_name:
                return c
        return None

    def check_opened_pr(self, pr: PullRequest):
        pr_labels = [l.name for l in pr.labels]

        self.logger.info("check opened pr %r (labels %s)", pr, pr_labels)

        if self.pr_label_fail in pr_labels:
            self.logger.info("pr has %s label, exit", self.pr_label_fail)
            return

        check = self.get_commit_check_status(pr.head.sha)

        if check is None:
            self.logger.info("no %r checks found", self.check_name)
            return

        self.logger.info("check result %s", check)

        if check.state == "failure":
            self.logger.info("check failed")
            self.add_failed_comment(pr, f"Check `{self.check_name}` failed.")
            self.add_pr_failed_label(pr)
            return

        elif check.state == "success":
            self.logger.info("check success, going to merge")
            self.merge_pr(pr)
        else:
            self.logger.info("wait for success")

    def add_pr_failed_label(self, pr: PullRequest):
        pr.add_to_labels(self.pr_label_fail)

    def git_merge_pr(self, pr: PullRequest):
        self.git_run("clone", f"https://{self.token}@github.com/{self.repo_name}.git", "merge-repo")
        os.chdir("merge-repo")
        self.git_run("fetch", "origin", f"pull/{pr.number}/head:PR")
        self.git_run("checkout", self.base_branch)

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
        self.logger.info("start merge %s into main", pr)
        if not self.git_merge_pr(pr):
            self.logger.info("unable to merge PR")
            return
        self.logger.info("deleting ref %r", pr.head.ref)
        self.repo.get_git_ref(f"heads/{pr.head.ref}").delete()
        body = f"The PR was successfully merged into main using workflow"
        pr.create_issue_comment(body=body)

    def add_failed_comment(self, pr: PullRequest, text: str):
        text += f" All future check are suspended, please remove the `{self.pr_label_fail}` label to enable checks."
        if self.workflow_url:
            text += f" Rightlib sync workflow logs can be found [here]({self.workflow_url})."
        pr.create_issue_comment(f"{self.failed_comment_mark}\n{text}")

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

    def create_new_pr(self):
        dev_branch_name = f"merge-libs-{self.dtm}"
        commit_msg = f"Import libraries {self.dtm}"
        pr_title = f"Library import {self.dtm}"

        self.git_run("clone", f"https://{self.token}@github.com/{self.repo_name}.git", "ydb-new-pr")
        os.chdir("ydb-new-pr")
        self.git_run("checkout", self.head_branch)
        rightlib_sha = self.git_revparse_head()

        self.logger.info(f"{rightlib_sha=}")

        self.git_run("checkout", self.base_branch)
        self.git_run(f"checkout", "-b", dev_branch_name)

        prev_sha = self.git_revparse_head()

        self.git_run("merge", self.head_branch, "--no-edit")

        cur_sha = self.git_revparse_head()

        if prev_sha == cur_sha:
            logging.info("Merge did not bring any changes, exiting")
            return

        with open(self.rightlib_sha_file, "w") as fp:
            fp.write(f"{rightlib_sha}\n")

        self.git_run("add", ".")
        self.git_run("commit", "-m", commit_msg)
        self.git_run("push", "--set-upstream", "origin", dev_branch_name)

        if self.workflow_url:
            pr_body = f"PR was created by rightlib sync workflow [run]({self.workflow_url})"
        else:
            pr_body = f"PR was created by rightlib sync script"

        pr = self.repo.create_pull(self.base_branch, dev_branch_name, title=pr_title, body=pr_body)
        pr.add_to_labels(self.pr_label_rightlib)

    def sync(self):
        pr = self.get_latest_open_pr()

        if pr:
            self.check_opened_pr(pr)
        else:
            cur_sha = self.rightlib_latest_repo_sha()
            latest_sha = self.rightlib_latest_sync_commit()
            self.logger.info("cur_sha=%s", cur_sha)
            self.logger.info("latest_sha=%s", latest_sha)

            if cur_sha != latest_sha:
                self.create_new_pr()


def main():
    log_fmt = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    logging.basicConfig(format=log_fmt, level=logging.DEBUG)
    repo = os.environ["REPO"]
    token = os.environ["TOKEN"]
    syncer = RightlibSync(repo, "main", "rightlib", token)
    syncer.sync()


if __name__ == "__main__":
    main()
