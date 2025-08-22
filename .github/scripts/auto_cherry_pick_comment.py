#!/usr/bin/env python3
import os
import re
import datetime
import logging
import subprocess
import argparse
from github import Github, GithubException


class CherryPickCommandHandler:
    def __init__(self, args):
        self.repo_name = os.environ["REPO"]
        self.token = os.environ["TOKEN"]
        self.pr_number = int(args.pr_number)
        self.comment_body = args.comment_body
        self.comment_author = args.comment_author
        self.comment_id = int(args.comment_id)
        
        self.gh = Github(login_or_token=self.token)
        self.repo = self.gh.get_repo(self.repo_name)
        self.dtm = datetime.datetime.now().strftime("%y%m%d-%H%M")
        self.logger = logging.getLogger("cherry-pick-comment")
        
        try:
            self.workflow_url = self.repo.get_workflow_run(int(os.getenv('GITHUB_RUN_ID', 0))).html_url
        except:
            self.workflow_url = None

    def parse_cherry_pick_command(self):
        """Parse cherry-pick command from comment body"""
        # Look for /cherry-pick command followed by branch names
        pattern = r'/cherry-pick\s+([\w\s\-\.]+)'
        match = re.search(pattern, self.comment_body, re.IGNORECASE)
        
        if not match:
            return []
        
        # Split branch names by whitespace and filter empty strings
        branches = [branch.strip() for branch in match.group(1).split() if branch.strip()]
        return branches

    def validate_branches(self, branches):
        """Validate that all specified branches exist"""
        valid_branches = []
        invalid_branches = []
        
        for branch in branches:
            try:
                self.repo.get_branch(branch)
                valid_branches.append(branch)
            except GithubException:
                invalid_branches.append(branch)
        
        return valid_branches, invalid_branches

    def check_permissions(self):
        """Check if comment author has permissions to trigger cherry-pick"""
        try:
            # Check if user is a collaborator or has write access
            try:
                permission = self.repo.get_collaborator_permission(self.comment_author)
                return permission in ['admin', 'write']
            except GithubException:
                # If not a collaborator, check if they are the PR author
                pr = self.repo.get_pull(self.pr_number)
                return pr.user.login == self.comment_author
        except Exception as e:
            self.logger.error(f"Error checking permissions: {e}")
            return False

    def get_pr_info(self):
        """Get PR information needed for cherry-pick"""
        pr = self.repo.get_pull(self.pr_number)
        return {
            'number': pr.number,
            'title': pr.title,
            'merge_commit_sha': pr.merge_commit_sha,
            'html_url': pr.html_url,
            'author': pr.user.login
        }

    def add_summary(self, msg):
        """Add message to workflow summary and logs"""
        self.logger.info(msg)
        summary_path = os.getenv('GITHUB_STEP_SUMMARY')
        if summary_path:
            with open(summary_path, 'a') as summary:
                summary.write(f'{msg}\n')

    def git_run(self, *args):
        """Run git command with error handling"""
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

    def create_pr_for_branch(self, target_branch, pr_info):
        """Create a cherry-pick PR for a specific target branch"""
        dev_branch_name = f"auto-cherry-pick-{self.pr_number}-{target_branch}-{self.dtm}"
        
        self.git_run("reset", "--hard")
        self.git_run("checkout", target_branch)
        self.git_run("checkout", "-b", dev_branch_name)
        
        try:
            self.git_run("cherry-pick", "--allow-empty", pr_info['merge_commit_sha'])
        except subprocess.CalledProcessError as e:
            # Cherry-pick failed, likely due to conflicts
            raise Exception(f"Cherry-pick failed with conflicts. Manual resolution needed.")
        
        self.git_run("push", "--set-upstream", "origin", dev_branch_name)

        # Create PR title and body
        pr_title = f"{target_branch}: {pr_info['title']}"
        pr_body = f"""Automatic cherry-pick of PR #{pr_info['number']} to `{target_branch}` branch

Original PR: {pr_info['html_url']}
Original Author: @{pr_info['author']}
Cherry-pick requested by: @{self.comment_author}

---
{pr_info['title']}"""

        if self.workflow_url:
            pr_body += f"\n\nCherry-pick created by [automated workflow]({self.workflow_url})"

        new_pr = self.repo.create_pull(
            target_branch, dev_branch_name, 
            title=pr_title, 
            body=pr_body, 
            maintainer_can_modify=True
        )
        
        return new_pr

    def post_status_comment(self, results):
        """Post a comment with cherry-pick results"""
        pr = self.repo.get_pull(self.pr_number)
        
        success_count = len([r for r in results if r['status'] == 'success'])
        failure_count = len([r for r in results if r['status'] == 'failed'])
        
        comment_body = f"## 🍒 Cherry-pick Results\n\n"
        comment_body += f"**Summary:** {success_count} successful, {failure_count} failed\n\n"
        
        if success_count > 0:
            comment_body += "### ✅ Successful Cherry-picks\n\n"
            for result in results:
                if result['status'] == 'success':
                    comment_body += f"- **{result['branch']}**: {result['pr_url']}\n"
            comment_body += "\n"
        
        if failure_count > 0:
            comment_body += "### ❌ Failed Cherry-picks\n\n"
            for result in results:
                if result['status'] == 'failed':
                    comment_body += f"- **{result['branch']}**: {result['error']}\n"
            comment_body += "\n"
        
        comment_body += f"---\n*In response to [comment]({pr.html_url}#issuecomment-{self.comment_id}) by @{self.comment_author}*"
        
        if self.workflow_url:
            comment_body += f" | [Workflow logs]({self.workflow_url})"
        
        try:
            pr.create_issue_comment(comment_body)
            self.logger.info("Posted cherry-pick results comment")
        except Exception as e:
            self.logger.error(f"Failed to post results comment: {e}")

    def process(self):
        """Main processing function"""
        self.logger.info(f"Processing cherry-pick command from @{self.comment_author} on PR #{self.pr_number}")
        
        # Check permissions
        if not self.check_permissions():
            self.logger.warning(f"User @{self.comment_author} does not have permission to trigger cherry-pick")
            return
        
        # Parse command
        target_branches = self.parse_cherry_pick_command()
        if not target_branches:
            self.logger.info("No valid cherry-pick command found in comment")
            return
        
        self.logger.info(f"Cherry-pick requested for branches: {target_branches}")
        
        # Validate branches
        valid_branches, invalid_branches = self.validate_branches(target_branches)
        
        if invalid_branches:
            self.logger.warning(f"Invalid branches specified: {invalid_branches}")
        
        if not valid_branches:
            self.logger.error("No valid branches specified")
            return
        
        # Get PR info
        pr_info = self.get_pr_info()
        if not pr_info['merge_commit_sha']:
            self.logger.error("PR is not merged, cannot cherry-pick")
            return
        
        self.add_summary(f"Cherry-picking PR #{pr_info['number']} to branches: {', '.join(valid_branches)}")
        
        # Clone repository and perform cherry-picks
        results = []
        
        try:
            self.git_run(
                "clone", f"https://{self.token}@github.com/{self.repo_name}.git", 
                "-c", "protocol.version=2", f"ydb-cherry-pick"
            )
            os.chdir(f"ydb-cherry-pick")
            
            for branch in valid_branches:
                try:
                    new_pr = self.create_pr_for_branch(branch, pr_info)
                    results.append({
                        'branch': branch,
                        'status': 'success',
                        'pr_url': new_pr.html_url
                    })
                    self.add_summary(f"✅ {branch}: Cherry-pick PR created: {new_pr.html_url}")
                except Exception as e:
                    results.append({
                        'branch': branch,
                        'status': 'failed',
                        'error': str(e)
                    })
                    self.add_summary(f"❌ {branch}: Cherry-pick failed: {str(e)}")
            
        except Exception as e:
            self.logger.error(f"Failed to set up git repository: {e}")
            return
        
        # Post results comment
        self.post_status_comment(results)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pr-number", required=True, help="Pull request number")
    parser.add_argument("--comment-body", required=True, help="Comment body text")
    parser.add_argument("--comment-author", required=True, help="Comment author")
    parser.add_argument("--comment-id", required=True, help="Comment ID")
    args = parser.parse_args()

    log_fmt = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    logging.basicConfig(format=log_fmt, level=logging.DEBUG)
    
    CherryPickCommandHandler(args).process()


if __name__ == "__main__":
    main()