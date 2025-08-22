#!/usr/bin/env python3
import os
import re
import logging
import argparse
from github import Github


class CherryPickSuggester:
    def __init__(self, args):
        self.repo_name = os.environ["REPO"]
        self.token = os.environ["TOKEN"]
        self.pr_number = int(args.pr_number)
        self.pr_title = args.pr_title
        self.pr_author = args.pr_author
        self.merge_commit_sha = args.merge_commit_sha
        
        self.gh = Github(login_or_token=self.token)
        self.repo = self.gh.get_repo(self.repo_name)
        self.logger = logging.getLogger("cherry-pick-suggest")

    def get_stable_branches(self):
        """Get list of stable branches from the repository"""
        branches = []
        try:
            # Get all branches and filter for stable-* pattern
            repo_branches = self.repo.get_branches()
            for branch in repo_branches:
                if re.match(r'^stable-\d+', branch.name):
                    branches.append(branch.name)
            
            # Sort branches by version number for better presentation
            branches.sort(key=lambda x: [int(v) for v in re.findall(r'\d+', x)], reverse=True)
            
        except Exception as e:
            self.logger.error(f"Error getting stable branches: {e}")
            # Fallback to known stable branches if API fails
            branches = ['stable-25-1', 'stable-24-4']
        
        return branches[:5]  # Limit to 5 most recent stable branches

    def should_suggest_cherry_pick(self):
        """Determine if we should suggest cherry-pick for this PR"""
        # Skip if PR is labeled as not for cherry-pick
        pr = self.repo.get_pull(self.pr_number)
        labels = [label.name for label in pr.labels]
        
        skip_labels = ['not-for-cherry-pick', 'not-for-changelog', 'experimental-feature']
        if any(label in labels for label in skip_labels):
            self.logger.info(f"Skipping cherry-pick suggestion due to labels: {labels}")
            return False
        
        # Skip if PR description contains skip markers
        if pr.body and ('cherry-pick' in pr.body.lower() and 'skip' in pr.body.lower()):
            self.logger.info("Skipping cherry-pick suggestion due to skip marker in description")
            return False
            
        return True

    def post_suggestion_comment(self, stable_branches):
        """Post a comment suggesting cherry-pick to stable branches"""
        pr = self.repo.get_pull(self.pr_number)
        
        branches_list = ", ".join([f"`{branch}`" for branch in stable_branches])
        
        comment_body = f"""## 🍒 Cherry-pick Suggestion

Thank you for your contribution, @{self.pr_author}! 

This PR has been successfully merged to `main`. If you would like to cherry-pick this change to stable branches, you can use the following command:

```
/cherry-pick {' '.join(stable_branches)}
```

Available stable branches: {branches_list}

**Usage examples:**
- Cherry-pick to all suggested branches: `/cherry-pick {' '.join(stable_branches)}`
- Cherry-pick to specific branches: `/cherry-pick stable-25-1 stable-24-4`
- Cherry-pick to a single branch: `/cherry-pick stable-25-1`

The cherry-pick process will:
1. Create new branches for each target stable branch
2. Apply your changes via `git cherry-pick`
3. Create pull requests for each branch
4. Report any conflicts or errors

If you don't need to cherry-pick this change, you can ignore this message.

---
*Automated cherry-pick suggestion for PR #{self.pr_number} (commit {self.merge_commit_sha[:8]})*"""

        try:
            pr.create_issue_comment(comment_body)
            self.logger.info(f"Posted cherry-pick suggestion comment for PR #{self.pr_number}")
        except Exception as e:
            self.logger.error(f"Failed to post suggestion comment: {e}")

    def process(self):
        """Main processing function"""
        self.logger.info(f"Processing cherry-pick suggestion for PR #{self.pr_number}: {self.pr_title}")
        
        if not self.should_suggest_cherry_pick():
            self.logger.info("Cherry-pick suggestion skipped")
            return
        
        stable_branches = self.get_stable_branches()
        if not stable_branches:
            self.logger.warning("No stable branches found, skipping suggestion")
            return
        
        self.logger.info(f"Found stable branches: {stable_branches}")
        self.post_suggestion_comment(stable_branches)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pr-number", required=True, help="Pull request number")
    parser.add_argument("--pr-title", required=True, help="Pull request title")
    parser.add_argument("--pr-author", required=True, help="Pull request author")
    parser.add_argument("--merge-commit-sha", required=True, help="Merge commit SHA")
    args = parser.parse_args()

    log_fmt = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    logging.basicConfig(format=log_fmt, level=logging.DEBUG)
    
    CherryPickSuggester(args).process()


if __name__ == "__main__":
    main()