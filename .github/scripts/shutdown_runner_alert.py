import datetime
import os
import requests
import argparse

from telegram.send_telegram_message import send_telegram_message


def get_alert_logins() -> str:
    logins = os.getenv('GH_ALERTS_TG_LOGINS')
    return logins.strip() if logins else "@empEfarinov"


def str_to_date(str):
    return datetime.datetime.strptime(str, '%Y-%m-%dT%H:%M:%SZ')


def get_workflows_from_ts(owner, repo, token, ts, max_runs=50):
    """Get recent workflow runs filtered by status and event type."""
    url = f"https://api.github.com/repos/{owner}/{repo}/actions/runs"
    workflow_runs = []
    page_size = min(100, max_runs)
    while len(workflow_runs) < max_runs:
        params = {
            'status': 'cancelled',
            'event': 'pull_request_target',
            'per_page': page_size,
            'page': len(workflow_runs) // page_size + 1
        }
        headers = {
            'Accept': 'application/vnd.github.v3+json',
            'Authorization': f'token {token}'
        }
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        workflow_runs += response.json()['workflow_runs']

        if str_to_date(workflow_runs[-1]['created_at']) < ts:
            break
    return list(filter(lambda run: str_to_date(run['created_at']) >= ts, workflow_runs))


def get_workflow_jobs(owner, repo, run_id, token):
    """Get jobs for a specific workflow run."""
    url = f"https://api.github.com/repos/{owner}/{repo}/actions/runs/{run_id}/jobs"
    headers = {
        'Accept': 'application/vnd.github.v3+json',
        'Authorization': f'token {token}'
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()['jobs']


def check_logs_for_shutdown_signal(owner, repo, run_id, token):
    """Check workflow logs for the specific shutdown signal error."""
    all_jobs = get_workflow_jobs(owner, repo, run_id, token)
    fail = False
    try:
        for job in all_jobs:
            if job['conclusion'] != 'cancelled':
                continue
            url = f"https://api.github.com/repos/{owner}/{repo}/actions/jobs/{job['id']}/logs"
            headers = {
                'Accept': 'application/vnd.github.v3+json',
                'Authorization': f'token {token}'
            }
            # Get the redirect URL for logs
            response = requests.get(url, headers=headers, allow_redirects=False)
            if response.status_code == 302:
                log_url = response.headers['Location']
                log_response = requests.get(log_url)
                if log_response.status_code == 200:
                    fail = fail or ("##[error]The runner has received a shutdown signal" in log_response.text)
        return fail
    except Exception as e:
        print(f"Error while log reading: {e}")
    return False


def main():
    """Main function to execute the workflow check."""
    parser = argparse.ArgumentParser(
        description='Check GitHub PR target workflows for runner shutdown errors'
    )
    parser.add_argument('--bot-token', help='Telegram bot token (or use TELEGRAM_BOT_TOKEN env var)')
    parser.add_argument('--chat-id', help='Telegram chat ID')
    parser.add_argument('--channel', help='Telegram channel ID (alternative to --chat-id)')
    parser.add_argument('--thread-id', type=int, help='Telegram thread ID for group messages')

    parser.add_argument('--owner', help='Repository owner')
    parser.add_argument('--repo', help='Repository name')
    parser.add_argument('--token', help='Github token')
    parser.add_argument('--hours-delta', help='Number of hours to analyze from current timestamp', default=12, type=int)
    parser.add_argument('--max-rows', help='Max number of workflow runs to analyze', default=100, type=int)
    args = parser.parse_args()

    # Get GitHub token from environment
    token = args.token or os.getenv('GITHUB_TOKEN')
    if not token:
        print("Error: GITHUB_TOKEN environment variable not set")
        return

    # Get repo owner/name from arguments or environment
    repo_full_name = os.getenv('GITHUB_REPOSITORY')
    if repo_full_name:
        owner, repo = repo_full_name.split('/')
    elif args.owner and args.repo:
        owner, repo = args.owner, args.repo
    else:
        parser.print_help()
        return

    try:
        # Get recent workflow runs
        current_date = datetime.datetime.now()
        workflows = get_workflows_from_ts(owner, repo, token, current_date - datetime.timedelta(hours=args.hours_delta), max_runs=args.max_rows)
        print(f'Got {len(workflows)} workflows created from the last {args.hours_delta} hours')
        recent_workflows = sorted(workflows, key=lambda x: x['created_at'], reverse=True)

        errors = []
        for workflow in recent_workflows:
            if check_logs_for_shutdown_signal(owner, repo, workflow['id'], token):
                print(f"\n🔴 SHUTDOWN ERROR - Workflow #{workflow['id']}")
                print(f"Created: {workflow['created_at']}")
                print(f"URL: {workflow['html_url']}")
                print(f"PR: {workflow['pull_requests'][0]['url'] if workflow['pull_requests'] else 'N/A'}")
                errors.append({
                    "workflow_id": workflow['id'],
                    "workflow_name": workflow['name'],
                    "created_at": str_to_date(workflow['created_at']).strftime('%Y-%m-%d %H:%M'),
                    "workflow_url": workflow['html_url'],
                    "pr_url": workflow['pull_requests'][0]['url'] if workflow['pull_requests'] else None
                })

        if len(errors) > 0:
            bot_token = args.bot_token or os.getenv('TELEGRAM_BOT_TOKEN')
            chat_id = args.channel or args.chat_id or os.getenv('TELEGRAM_CHAT_ID')
            thread_id = args.thread_id or os.getenv('TELEGRAM_THREAD_ID')

            message = "🚨 *RUNNER DIED DURING RUN*\n"
            for error in errors:
                message += f"""
• Workflow *{error['workflow_name']}* [#{error['workflow_id']}]({error['workflow_url']})
  Created at: {error['created_at']}"""
                if error['pr_url']:
                    message += f"""
  Linked PR: {error['pr_url']}"""
                message += "\n"

            message += f"""
  CC {get_alert_logins()}"""

            if chat_id and not chat_id.startswith('-') and len(chat_id) >= 10:
                # Add -100 prefix for supergroup
                chat_id = f"-100{chat_id}"
            send_telegram_message(
                bot_token,
                chat_id,
                message,
                message_thread_id=thread_id)
        print(f"\nFound {len(errors)} workflows with shutdown errors out of {len(recent_workflows)} checked")

    except Exception as e:
        print(f"Error: {str(e)}")
        raise e


if __name__ == "__main__":
    main()
