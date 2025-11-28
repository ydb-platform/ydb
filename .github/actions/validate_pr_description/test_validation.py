#!/usr/bin/env python3
"""
Test script to validate PR description locally.

Usage:
    python3 test_validation.py <pr_number>
    python3 test_validation.py <pr_number> --body-file <file_path>
    python3 test_validation.py --body-file <file_path>

Environment variables:

Required for fetching PR from GitHub:
    export GITHUB_TOKEN="your_github_token"

Optional for table generation testing:
    export SHOW_RUN_TESTS_IN_PR="TRUE"        # Enable test execution table generation
    export SHOW_BACKPORT_IN_PR="TRUE"         # Enable backport table generation
    export APP_DOMAIN="your-app-domain.com"   # Required if either table flag is TRUE

Note: GITHUB_WORKSPACE and GITHUB_REPOSITORY are automatically set if not provided.
      GITHUB_REPOSITORY is determined from git remote origin URL.
"""
import os
import sys
import json
from pathlib import Path
from validate_pr_description import (
    validate_pr_description_from_file,
    ensure_tables_in_pr_body,
    update_pr_body
)

def find_repo_root():
    """Find repository root by looking for .github or .git directory."""
    current = Path(__file__).resolve().parent
    while current != current.parent:
        if (current / ".github").exists() or (current / ".git").exists():
            return str(current)
        current = current.parent
    # Fallback to current working directory
    return os.getcwd()

def find_github_repository():
    """Find GitHub repository from git remote."""
    repo_root = find_repo_root()
    git_dir = Path(repo_root) / ".git"
    
    if not git_dir.exists():
        raise ValueError("Not a git repository. Cannot determine GITHUB_REPOSITORY.")
    
    try:
        import subprocess
        result = subprocess.run(
            ["git", "remote", "get-url", "origin"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            check=True
        )
        url = result.stdout.strip()
        
        # Parse git URL (supports both https and ssh formats)
        if "github.com" in url:
            if url.startswith("https://"):
                # https://github.com/owner/repo.git
                parts = url.replace("https://github.com/", "").replace(".git", "").strip()
                if "/" in parts:
                    return parts
            elif url.startswith("git@") or url.startswith("ssh://"):
                # git@github.com:owner/repo.git or ssh://git@github.com/owner/repo.git
                parts = url.split("github.com")[-1].replace(":", "/").replace(".git", "").strip("/")
                if "/" in parts:
                    return parts
        
        raise ValueError(f"Could not parse GitHub repository from remote URL: {url}")
    except subprocess.CalledProcessError:
        raise ValueError("Failed to get git remote URL. Cannot determine GITHUB_REPOSITORY.")
    except Exception as e:
        raise ValueError(f"Failed to determine GITHUB_REPOSITORY: {e}")

def test_validation(pr_body: str, pr_number: int = None, base_ref: str = "main"):
    """Test validation and table generation."""
    print("=" * 60)
    print("PR Body from GitHub")
    print("=" * 60)
    print(pr_body)
    print("=" * 60)
    print()
    
    print("=" * 60)
    print("Testing PR description validation")
    print("=" * 60)
    
    # Validate
    is_valid, txt = validate_pr_description_from_file(description=pr_body)
    print(f"\nValidation result: {'✅ PASSED' if is_valid else '❌ FAILED'}")
    print(f"Message: {txt}\n")
    
    if not is_valid:
        return False, pr_body
    
    # Test table generation if enabled
    show_test_table = os.environ.get("SHOW_RUN_TESTS_IN_PR", "").upper() == "TRUE"
    show_backport_table = os.environ.get("SHOW_BACKPORT_IN_PR", "").upper() == "TRUE"
    result_body = pr_body
    
    if show_test_table or show_backport_table:
        print("=" * 60)
        print("Testing table generation")
        print("=" * 60)
        
        app_domain = os.environ.get("APP_DOMAIN")
        if not app_domain:
            print("⚠️  APP_DOMAIN not set, skipping table generation test")
            print("   Set APP_DOMAIN environment variable to test table generation")
            return is_valid, pr_body
        
        if not pr_number:
            print("⚠️  PR number not provided, skipping table generation test")
            print("   Provide PR number to test table generation")
            return is_valid, pr_body
        
        # Check current state
        test_marker = "<!-- test-execution-table -->"
        backport_marker = "<!-- backport-table -->"
        has_test = test_marker in pr_body
        has_backport = backport_marker in pr_body
        
        print(f"Current state:")
        print(f"  Test table exists: {has_test}")
        print(f"  Backport table exists: {has_backport}")
        print(f"Flags:")
        print(f"  SHOW_RUN_TESTS_IN_PR: {show_test_table}")
        print(f"  SHOW_BACKPORT_IN_PR: {show_backport_table}")
        print()
        
        updated_body = ensure_tables_in_pr_body(pr_body, pr_number, base_ref, app_domain,
                                                show_test_table=show_test_table,
                                                show_backport_table=show_backport_table)
        if updated_body:
            result_body = updated_body
            print("✅ Tables would be added to PR body")
            print("\nGenerated tables preview:")
            print("-" * 60)
            # Extract just the tables part for preview
            if test_marker in updated_body:
                test_start = updated_body.find(test_marker)
                test_end = updated_body.find("###", test_start + 1)
                if test_end == -1:
                    test_end = updated_body.find("**Legend:**", test_start + 1)
                if test_end != -1:
                    print(updated_body[test_start:test_end].strip())
            if backport_marker in updated_body:
                backport_start = updated_body.find(backport_marker)
                backport_end = updated_body.find("**Legend:**", backport_start + 1)
                if backport_end != -1:
                    print(updated_body[backport_start:backport_end].strip())
            print("-" * 60)
        else:
            if has_test and has_backport:
                print("ℹ️  Both tables already exist in PR body")
            else:
                print("⚠️  Function returned None but tables don't exist - this is unexpected")
    else:
        print("ℹ️  Neither SHOW_RUN_TESTS_IN_PR nor SHOW_BACKPORT_IN_PR is TRUE, skipping table generation test")
        print("   Set SHOW_RUN_TESTS_IN_PR=TRUE and/or SHOW_BACKPORT_IN_PR=TRUE to test table generation")
    
    return is_valid, result_body

def main():
    if len(sys.argv) < 2 and "--body-file" not in sys.argv:
        print(__doc__)
        sys.exit(1)
    
    # Set GITHUB_WORKSPACE for local testing if not already set
    if not os.environ.get("GITHUB_WORKSPACE"):
        repo_root = find_repo_root()
        os.environ["GITHUB_WORKSPACE"] = repo_root
        print(f"ℹ️  Set GITHUB_WORKSPACE={repo_root} for local testing")
    
    # Set GITHUB_REPOSITORY for local testing if not already set
    if not os.environ.get("GITHUB_REPOSITORY"):
        try:
            github_repo = find_github_repository()
            os.environ["GITHUB_REPOSITORY"] = github_repo
            print(f"ℹ️  Set GITHUB_REPOSITORY={github_repo} for local testing")
        except ValueError as e:
            print(f"❌ Error: {e}")
            print("   Set GITHUB_REPOSITORY environment variable manually")
            sys.exit(1)
    
    pr_number = None
    pr_body = None
    base_ref = "main"
    
    # Parse arguments
    if "--body-file" in sys.argv:
        idx = sys.argv.index("--body-file")
        if idx + 1 >= len(sys.argv):
            print("Error: --body-file requires a file path")
            sys.exit(1)
        with open(sys.argv[idx + 1], 'r') as f:
            pr_body = f.read()
        # Try to get PR number from remaining args
        if len(sys.argv) > idx + 2:
            try:
                pr_number = int(sys.argv[idx + 2])
            except ValueError:
                pass
    else:
        try:
            pr_number = int(sys.argv[1])
        except (ValueError, IndexError):
            print("Error: PR number must be an integer")
            sys.exit(1)
        
        # Try to get PR body from GitHub API if PR number provided
        github_token = os.environ.get("GITHUB_TOKEN")
        if github_token:
            try:
                from github import Github, Auth as GithubAuth
                gh = Github(auth=GithubAuth.Token(github_token))
                repo = gh.get_repo("ydb-platform/ydb")
                pr = repo.get_pull(pr_number)
                pr_body = pr.body or ""
                base_ref = pr.base.ref
                print(f"📥 Fetched PR #{pr_number} from GitHub")
            except Exception as e:
                print(f"⚠️  Failed to fetch PR from GitHub: {e}")
                print("   Provide PR body via --body-file option")
                sys.exit(1)
        else:
            print("Error: GITHUB_TOKEN not set. Cannot fetch PR from GitHub.")
            print("   Set GITHUB_TOKEN or use --body-file option")
            sys.exit(1)
    
    if not pr_body:
        print("Error: PR body is required")
        sys.exit(1)
    
    success, result_body = test_validation(pr_body, pr_number, base_ref)
    
    print()
    print("=" * 60)
    print("Resulting PR Body")
    print("=" * 60)
    print(result_body)
    print("=" * 60)
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()

