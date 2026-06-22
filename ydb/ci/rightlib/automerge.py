#!/usr/bin/env python3
import os
import shutil
import datetime
import logging
import subprocess
import time
from typing import Optional
from github import Github
from github.GithubException import GithubException
from github.PullRequest import PullRequest

automerge_pr_label = "automerge"
pr_label_fail = "automerge-blocked"
pr_label_retry = "automerge-retry"
check_name = "checks_integrated"
failed_comment_mark = "<!--SyncFailed-->"
retry_comment_mark = "<!--SyncRetry-->"
max_push_retries_in_run = 5
max_cross_run_push_retries = 5
push_race_backoff_sec = (0, 5, 10, 15, 20)


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
            self.clear_retry_label(pr)
            return

        elif check.state == "success":
            if pr_label_retry in pr_labels:
                self.logger.info("retrying merge after previous push failure (%s)", pr_label_retry)
            self.logger.info("check success, going to merge")
            self.merge_pr(pr)
        else:
            self.logger.info("wait for success")

    def add_pr_failed_label(self, pr: PullRequest):
        pr.add_to_labels(pr_label_fail)

    def clear_retry_label(self, pr: PullRequest):
        if pr_label_retry not in [l.name for l in pr.labels]:
            return
        try:
            pr.remove_from_labels(pr_label_retry)
        except Exception:
            self.logger.warning("failed to remove %s label", pr_label_retry, exc_info=True)

    def count_cross_run_push_retries(self, pr: PullRequest) -> int:
        return sum(
            1
            for comment in pr.get_issue_comments()
            if retry_comment_mark in comment.body
        )

    def add_retry_comment(self, pr: PullRequest, text: str):
        if self.workflow_url:
            text += f" Sync workflow logs can be found [here]({self.workflow_url})."
        pr.create_issue_comment(f"{retry_comment_mark}\n{text}")

    def mark_push_retry(self, pr: PullRequest, base_ref: str):
        cross_run_retries = self.count_cross_run_push_retries(pr)
        if cross_run_retries + 1 >= max_cross_run_push_retries:
            self.logger.warning(
                "push to %s rejected after %s cross-run retries, blocking automerge",
                base_ref,
                cross_run_retries,
            )
            self.add_failed_comment(
                pr,
                f"Unable to push merged revision (failed {max_cross_run_push_retries} workflow runs due to push race).",
            )
            self.add_pr_failed_label(pr)
            self.clear_retry_label(pr)
            return

        text = (
            f"Unable to push merged revision to `{base_ref}` "
            "(probably a race with another push), will retry automatically."
        )
        try:
            pr.add_to_labels(pr_label_retry)
            self.add_retry_comment(pr, text)
        except Exception:
            self.logger.warning("failed to mark push retry on %r", pr, exc_info=True)
            try:
                pr.remove_from_labels(pr_label_retry)
            except Exception:
                pass
            self.add_failed_comment(pr, "Unable to push merged revision (failed to schedule retry).")
            self.add_pr_failed_label(pr)

    @staticmethod
    def is_push_race_error(output: str) -> bool:
        lower = output.lower()
        return (
            "non-fast-forward" in lower
            or "fetch first" in lower
            or "failed to push some refs" in lower
        )

    def refresh_base_and_merge(self, pr: PullRequest, commit_msg: str) -> bool:
        base_ref = pr.base.ref
        self.git_run("fetch", "origin", base_ref)
        self.git_run("fetch", "origin", f"pull/{pr.number}/head:PR")
        self.git_run("checkout", base_ref)
        self.git_run("reset", "--hard", f"origin/{base_ref}")
        try:
            self.git_run("merge", "PR", "-m", commit_msg)
        except subprocess.CalledProcessError:
            return False
        return True

    def git_merge_pr(self, pr: PullRequest):
        shutil.rmtree("merge-repo", ignore_errors=True)
        original_dir = os.getcwd()
        try:
            self.git_run(
                "clone",
                "--filter=blob:none",
                "--single-branch",
                "--branch",
                pr.base.ref,
                f"https://{self.token}@github.com/{self.repo_name}.git",
                "merge-repo",
            )
            os.chdir("merge-repo")

            commit_msg = f"Merge pull request #{pr.number} from {pr.head.user.login}/{pr.head.ref}"

            for attempt in range(max_push_retries_in_run):
                if attempt > 0:
                    backoff = push_race_backoff_sec[min(attempt, len(push_race_backoff_sec) - 1)]
                    self.logger.info(
                        "push race retry %s/%s for %s, sleeping %ss",
                        attempt + 1,
                        max_push_retries_in_run,
                        pr.base.ref,
                        backoff,
                    )
                    time.sleep(backoff)

                if not self.refresh_base_and_merge(pr, commit_msg):
                    self.add_failed_comment(pr, "Unable to merge PR.")
                    self.add_pr_failed_label(pr)
                    self.clear_retry_label(pr)
                    return False

                try:
                    self.git_run("push")
                    return True
                except subprocess.CalledProcessError as e:
                    output = (e.output or b"").decode()
                    if not self.is_push_race_error(output):
                        self.logger.warning("push to %s rejected with non-transient error", pr.base.ref)
                        self.add_failed_comment(pr, "Unable to push merged revision.")
                        self.add_pr_failed_label(pr)
                        self.clear_retry_label(pr)
                        return False
                    self.logger.warning(
                        "push to %s rejected (race), attempt %s/%s in this run",
                        pr.base.ref,
                        attempt + 1,
                        max_push_retries_in_run,
                    )

            self.logger.warning(
                "push to %s rejected after %s in-run retries, scheduling cross-run retry",
                pr.base.ref,
                max_push_retries_in_run,
            )
            self.mark_push_retry(pr, pr.base.ref)
            return False
        finally:
            os.chdir(original_dir)
            shutil.rmtree("merge-repo", ignore_errors=True)

    def delete_head_ref(self, pr: PullRequest):
        if pr.head.repo.full_name != self.repo_name:
            self.logger.info("skip deleting head ref %r (fork PR)", pr.head.ref)
            return
        self.logger.info("deleting ref %r", pr.head.ref)
        try:
            self.repo.get_git_ref(f"heads/{pr.head.ref}").delete()
        except GithubException as e:
            if e.status == 404:
                # GitHub may delete the branch when it detects the merge
                # (auto-merge, "delete head branch" setting, etc.).
                self.logger.info("head ref %r already gone", pr.head.ref)
                return
            raise

    def merge_pr(self, pr: PullRequest):
        self.logger.info("start merge %s into %s", pr, pr.base.ref)
        if not self.git_merge_pr(pr):
            self.logger.info("unable to merge PR")
            return
        self.clear_retry_label(pr)
        self.delete_head_ref(pr)
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
            output = subprocess.check_output(args, stderr=subprocess.STDOUT).decode()
        except subprocess.CalledProcessError as e:
            output = (e.output or b"").decode()
            self.logger.error(output)
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
