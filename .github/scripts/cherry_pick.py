#!/usr/bin/env python3
import os
import datetime
import logging
import subprocess
import argparse
from github import Github, GithubException, GithubObject, Commit


class CherryPickCreator:
    def __init__(self, args):
        def __split(s: str, seps: str = ', '):
            if not s:
                return []
            if not seps:
                return [s]
            result = []
            for part in s.split(seps[0]):
                result += __split(part, seps[1:])
            return result

        self.repo_name = os.environ["REPO"]
        self.target_branches = __split(args.target_branches)
        self.token = os.environ["TOKEN"]
        self.gh = Github(login_or_token=self.token)
        self.repo = self.gh.get_repo(self.repo_name)
        self.commit_shas: list[str] = []
        self.pr_title_list: list[str] = []
        self.pr_body_list: list[str] = []
        for c in __split(args.commits):
            commit = self.repo.get_commit(c)
            self.pr_title_list.append(commit.sha)
            self.pr_body_list.append(commit.html_url)
            self.commit_shas.append(commit.sha)
        for p in __split(args.pulls):
            pull = self.repo.get_pull(int(p))
            self.pr_title_list.append(f'PR {pull.number}')
            self.pr_body_list.append(pull.html_url)
            self.commit_shas.append(pull.merge_commit_sha)
        self.dtm = datetime.datetime.now().strftime("%y%m%d-%H%M")
        self.logger = logging.getLogger("cherry-pick")
        self.workflow_url = None
        self.__detect_env()

    def __detect_env(self):
        if "GITHUB_RUN_ID" in os.environ:
            self.workflow_url = (
                f"{os.environ['GITHUB_SERVER_URL']}/{self.repo_name}/actions/runs/{os.environ['GITHUB_RUN_ID']}"
            )

    def add_summary(self, msg):
        logging.info(msg)
        summary_path = os.getenv('GITHUB_STEP_SUMMARY')
        if summary_path:
            with open(summary_path, 'a') as summary:
                summary.write(msg)

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

    def create_pr_for_branch(self, target_branch):
        dev_branch_name = f"cherry-pick-{target_branch}-{self.dtm}"
        self.git_run("reset", "--hard")
        self.git_run("checkout", target_branch)
        self.git_run("checkout", "-b", dev_branch_name)
        self.git_run("cherry-pick", "--allow-empty", *self.commit_shas)
        self.git_run("push", "--set-upstream", "origin", dev_branch_name)

        pr_title = f"Cherry-pick {', '.join(self.pr_title_list)} to {target_branch}"
        pr_body = f"Cherry-pick {', '.join(self.pr_body_list)} to {target_branch}\n\n"
        if self.workflow_url:
            pr_body += f"PR was created by cherry-pick workflow [run]({self.workflow_url})"
        else:
            pr_body += "PR was created by cherry-pick script"

        pr = self.repo.create_pull(
            target_branch, dev_branch_name, title=pr_title, body=pr_body, maintainer_can_modify=True
        )
        self.add_summary(f'{target_branch}: PR {pr.html_url} created\n')

    def process(self):
        if len(self.commit_shas) == 0 or len(self.target_branches) == 0:
            self.add_summary("Noting to cherry-pick or no targets branches, my life is meaningless.")
            return
        self.git_run("clone", f"https://{self.token}@github.com/{self.repo_name}.git", "-c", "protocol.version=2", f"ydb-new-pr")
        os.chdir(f"ydb-new-pr")
        for target in self.target_branches:
            try:
                self.create_pr_for_branch(target)
            except GithubException as e:
                self.add_summary(f'{target} error {type(e)}\n```\n{e}\n```\n')
            except BaseException as e:
                self.add_summary(f'{target} error {type(e)}\n```\n{e}\n```\n')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--commits", help="Comma or space separated list of commit SHAs")
    parser.add_argument("--pulls", help="Comma or space separated list of PR numbers")
    parser.add_argument("--target-branches", help="Comma or space separated list of branchs to cherry-pick")
    args = parser.parse_args()

    log_fmt = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    logging.basicConfig(format=log_fmt, level=logging.DEBUG)
    CherryPickCreator(args).process()


if __name__ == "__main__":
    main()
