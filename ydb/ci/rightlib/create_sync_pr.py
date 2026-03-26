#!/usr/bin/env python3
import os
import datetime
import logging
import subprocess
import argparse
from typing import Optional
from github import Github
from github.PullRequest import PullRequest
import automerge


class PrSyncCreator:
    rightlib_sha_file = "ydb/ci/rightlib.txt"
    check_name = "checks_integrated"
    failed_comment_mark = "<!--SyncFailed-->"

    def __init__(self, repo, base_branch, head_branch, token, pr_label, pr_label_failed, ours_on_conflict, theirs_on_conflict):
        self.repo_name = repo
        self.base_branch = base_branch
        self.head_branch = head_branch
        self.ours_on_conflict = ours_on_conflict
        self.theirs_on_conflict = theirs_on_conflict
        self.token = token
        self.pr_label = pr_label
        self.pr_label_fail = pr_label_failed
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

    def is_commit_present_on_branch(self, sha, branch):
        try:
            command = ["git", "merge-base", "--is-ancestor", sha, 'origin/{}'.format(branch)]

            result = subprocess.run(
                command,
                capture_output=True,
                text=True
            )

            if result.returncode == 0:
                return True
            elif result.returncode == 1:
                return False
            else:
                self.logger.warning(f"Command '{' '.join(command)}' finished with error:")
                self.logger.warning(f"Exit code: {result.returncode}")
                self.logger.warning(f"Stderr: {result.stderr.strip()}")
                return None
        except Exception as e:
            self.logger.error(f"Exception occured while git merge-base: {e}")
            return None

    def get_latest_open_pr(self) -> Optional[PullRequest]:
        query = f"label:{self.pr_label} repo:{self.repo_name} base:{self.base_branch} label:{automerge.automerge_pr_label} is:pr state:open sort:created-desc"
        result = self.gh.search_issues(query).get_page(0)
        if result:
            return result[0].as_pull_request()
        return None

    def git_run(self, *args, fail=True):
        args = ["git"] + list(args)

        self.logger.info("run: %r", args)
        try:
            result = subprocess.run(args, check=fail, capture_output=True)
        except subprocess.CalledProcessError as e:
            self.logger.info("stdout:\n%s", e.stdout.decode())
            if e.stderr:
                self.logger.info("stderr:\n%s", e.stderr.decode())
            raise
        else:
            self.logger.info("output:\n%s", result.stdout.decode())
        return result

    def git_get_conflict_files(self):
        conflicting_files = set()

        for line in self.git_run("ls-files", "-u").stdout.decode().strip().splitlines():
            parts = line.split('\t')
            if len(parts) > 1:
                filename = parts[-1]
                conflicting_files.add(filename)

        return conflicting_files

    def find_conflict_lines(self, file):
        start_line_number = 1
        end_line_number = 1
        if os.path.exists(file):
            try:
                with open(file, 'r', encoding='utf-8', errors='ignore') as f:
                    for i, line in enumerate(f, 1):
                        if line.startswith('<<<<<<<'):
                            start_line_number = i
                        if line.startswith('>>>>>>>'):
                            end_line_number = i
                            break
            except Exception as e:
                self.logger.warning("Failed to find conflict lines in %s: %s", file, e)
        return (start_line_number, end_line_number)

    def git_revparse_head(self):
        return self.git_run("rev-parse", "HEAD").stdout.decode().strip()

    def create_new_pr(self):
        dev_branch_name = f"merge-{self.head_branch}-{self.dtm}"
        commit_msg = f"Sync branches {self.dtm}"
        pr_title = f"Sync branches {self.dtm}: {self.head_branch} to {self.base_branch}"

        self.git_run("clone", f"https://{self.token}@github.com/{self.repo_name}.git", "ydb-new-pr")
        os.chdir("ydb-new-pr")
        self.git_run("checkout", self.head_branch)
        rightlib_sha = self.git_revparse_head()

        self.logger.info(f"{rightlib_sha=}")

        self.git_run("checkout", self.base_branch)
        self.git_run("checkout", "-b", dev_branch_name)
        merge_result = self.git_run("merge", self.head_branch, "-m", commit_msg, fail=False)
        merge_output = merge_result.stdout.decode()
        merge_failed = merge_result.returncode != 0
        conflict_files = ''
        if merge_failed and "Automatic merge failed; fix conflicts and then commit the result." in merge_output:
            conflict_files = self.git_run("ls-files", "-u").stdout.decode()
            should_commit = False
            for ours_file in self.ours_on_conflict:
                if ours_file in conflict_files:
                    self.logger.warning(f"Conflicts while merging. Attempting to resolve for {ours_file} with --ours")
                    self.git_run("checkout", "--ours", ours_file)
                    self.git_run("add", ours_file)
                    should_commit = True
            for theirs_file in self.theirs_on_conflict:
                if theirs_file in conflict_files:
                    self.logger.warning(f"Conflicts while merging. Attempting to resolve only for {theirs_file} with --theirs")
                    self.git_run("checkout", "--theirs", theirs_file)
                    self.git_run("add", theirs_file)
                    should_commit = True
            conflict_files = self.git_get_conflict_files()
            if len(conflict_files) > 0:
                self.logger.info(f"Resolved conflicts while merging. Other conflicts should be fixed manually: {conflict_files}")
                self.git_run("add", *list(conflict_files))
                should_commit = True
            if should_commit:
                self.git_run("commit", "-m", commit_msg)
        elif merge_failed:
            raise Exception(f"Unexpected error during merge {merge_output}")
        self.git_run("push", "--set-upstream", "origin", dev_branch_name)

        if self.workflow_url:
            pr_body = f"PR was created by rightlib sync workflow [run]({self.workflow_url})"
        else:
            pr_body = "PR was created by rightlib sync script"

        if merge_failed and len(conflict_files) > 0:
            pr_body += "\n\n### Merge failed\n\nFound some unresolved conflicts:\n"
            for conflict_file in conflict_files:
                lines = self.find_conflict_lines(conflict_file)
                pr_body += f"- [{conflict_file}]({os.environ['GITHUB_SERVER_URL']}/{self.repo_name}/blob/{self.git_revparse_head()}/{conflict_file}#L{lines[0]}-L{lines[1]})\n"

        pr = self.repo.create_pull(
            self.base_branch, dev_branch_name, title=pr_title, body=pr_body, maintainer_can_modify=True
        )
        pr.add_to_labels(self.pr_label)
        pr.add_to_labels(automerge.automerge_pr_label)
        if merge_failed and len(conflict_files) > 0:
            pr.add_to_labels(automerge.pr_label_fail)

    def cmd_create_pr(self):
        pr = self.get_latest_open_pr()

        if not pr:
            cur_sha = self.rightlib_latest_repo_sha()
            self.logger.info("cur_sha=%s", cur_sha)

            if self.is_commit_present_on_branch(cur_sha, self.base_branch) is False:
                self.create_new_pr()
            else:
                self.logger.info("Skipping create-pr because base branch is up-to-date")
        else:
            self.logger.info("Skipping create-pr because an open PR was found")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-branch", help="Branch to merge into")
    parser.add_argument("--head-branch", help="Branch to be merged")
    parser.add_argument("--process-label", help="Label to filter PRs")
    parser.add_argument("--merge-ours", action="extend", nargs="+", default=[], type=str, help='Files that will be merged with --ours upon conflict')
    parser.add_argument("--merge-theirs", action="extend", nargs="+", default=[], type=str, help='Files that will be merged with --theirs upon conflict')
    args = parser.parse_args()

    log_fmt = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    logging.basicConfig(format=log_fmt, level=logging.DEBUG)
    repo = os.environ["REPO"]
    token = os.environ["TOKEN"]

    syncer = PrSyncCreator(repo, args.base_branch, args.head_branch, token, args.process_label, f'{args.process_label}-fail', args.merge_ours, args.merge_theirs)

    syncer.cmd_create_pr()


if __name__ == "__main__":
    main()
