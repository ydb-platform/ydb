#!/usr/bin/env python3
import os
import datetime
import logging
import subprocess
import argparse
from github import Github, GithubException, GithubObject, Commit


class CherryPickCreator:
    def __init__(self, args):
        def __split(s: str, seps: str = ', \n'):
            if not s:
                return []
            if not seps:
                return [s]
            result = []
            for part in s.split(seps[0]):
                result += __split(part, seps[1:])
            return result

        def __add_commit(c: str, single: bool):
            commit = self.repo.get_commit(c)
            pulls = commit.get_pulls()
            if pulls.totalCount > 0:
                title = f"{pulls.get_page(0)[0].title} (#{pulls.get_page(0)[0].number})"
                body = f"{pulls.get_page(0)[0].title} ({pulls.get_page(0)[0].html_url})"
            else:
                title = ''
                body = ""

            if single:
                self.pr_title_list.append(f"commit {commit.sha}: '{title}'")
            else:
                self.pr_title_list.append(commit.sha)
            self.pr_body_list.append(f"commit {commit.html_url}: '{body}'")
            self.commit_shas.append(commit.sha)

        def __add_pull(p: int, single: bool):
            pull = self.repo.get_pull(p)
            if single:
                self.pr_title_list.append(f"PR {pull.number} '{pull.title}'")
            else:
                self.pr_title_list.append(f'PR {pull.number}')
            self.pr_body_list.append(f"PR {pull.html_url}: '{pull.title}'")
            self.commit_shas.append(pull.merge_commit_sha)

        self.repo_name = os.environ["REPO"]
        self.target_branches = __split(args.target_branches)
        self.token = os.environ["TOKEN"]
        self.gh = Github(login_or_token=self.token)
        self.repo = self.gh.get_repo(self.repo_name)
        self.commit_shas: list[str] = []
        self.pr_title_list: list[str] = []
        self.pr_body_list: list[str] = []
        commits = __split(args.commits)
        for c in commits:
            id = c.split('/')[-1]
            try:
                __add_pull(int(id), len(commits) == 1)
            except ValueError:
                __add_commit(id, len(commits) == 1)

        self.dtm = datetime.datetime.now().strftime("%y%m%d-%H%M")
        self.logger = logging.getLogger("cherry-pick")
        try:
            self.workflow_url = self.repo.get_workflow_run(int(os.getenv('GITHUB_RUN_ID', 0))).html_url
        except:
            self.workflow_url = None

    def pr_title(self, target_branch) -> str:
        return f"[{target_branch}] Cherry-pick {', '.join(self.pr_title_list)}"
    
    def pr_body(self, with_wf: bool) -> str:
        commits = '\n'.join(self.pr_body_list)
        pr_body = f"Cherry-pick:\n{commits}\n"
        if with_wf:
            if self.workflow_url:
                pr_body += f"\nPR was created by cherry-pick workflow [run]({self.workflow_url})\n"
            else:
                pr_body += "\nPR was created by cherry-pick script\n"
        return pr_body
    
    def add_summary(self, msg):
        self.logger.info(msg)
        summary_path = os.getenv('GITHUB_STEP_SUMMARY')
        if summary_path:
            with open(summary_path, 'a') as summary:
                summary.write(f'{msg}\n')

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

        pr = self.repo.create_pull(
            target_branch, dev_branch_name, title=self.pr_title(target_branch), body=self.pr_body(True), maintainer_can_modify=True
        )
        self.add_summary(f'{target_branch}: PR {pr.html_url} created')

    def process(self):
        br = ', '.join(self.target_branches)
        self.logger.info(self.pr_title(br))
        self.add_summary(f"{self.pr_body(False)}to {br}")
        if len(self.commit_shas) == 0 or len(self.target_branches) == 0:
            self.add_summary("Noting to cherry-pick or no targets branches, my life is meaningless.")
            return
        self.git_run(
            "clone", f"https://{self.token}@github.com/{self.repo_name}.git", "-c", "protocol.version=2", f"ydb-new-pr"
        )
        os.chdir(f"ydb-new-pr")
        for target in self.target_branches:
            try:
                self.create_pr_for_branch(target)
            except GithubException as e:
                self.add_summary(f'{target} error {type(e)}\n```\n{e}\n```')
            except BaseException as e:
                self.add_summary(f'{target} error {type(e)}\n```\n{e}\n```')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--commits",
        help="List of commits to cherry-pick. Can be represented as full or short commit SHA, PR number or URL to commit or PR. Separated by space, comma or line end.",
    )
    parser.add_argument(
        "--target-branches", help="List of branchs to cherry-pick. Separated by space, comma or line end."
    )
    args = parser.parse_args()

    log_fmt = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    logging.basicConfig(format=log_fmt, level=logging.DEBUG)
    CherryPickCreator(args).process()


if __name__ == "__main__":
    main()
