#!/usr/bin/env python3
"""Revalidate PR failures using mute rules from the current target branch.

Only a completed CI run's explicit failure record can be revalidated. Old runs
without that record, missing reports, and failures outside individual tests stay red.
"""
import argparse
import datetime
import json
import os
from pathlib import Path
import urllib.error
import urllib.parse
import urllib.request

from mute.mute_check import YaMuteCheck

REVALIDATED_DESCRIPTION = "Revalidated: all remaining failures are now muted; tests not rerun"


class GitHub:
    def __init__(self):
        self.repo = os.environ["GITHUB_REPOSITORY"]
        self.root = f"https://api.github.com/repos/{self.repo}"

    def request(self, path, data=None, raw=False):
        headers = {
            "Authorization": f"Bearer {os.environ['GITHUB_TOKEN']}",
            "Accept": "application/vnd.github.raw+json" if raw else "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        body = json.dumps(data).encode() if data is not None else None
        request = urllib.request.Request(self.root + path, data=body, headers=headers)
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.read().decode() if raw else json.load(response)

    def pages(self, path, key=None):
        separator = "&" if "?" in path else "?"
        for page in range(1, 11):  # GitHub workflow searches return at most 1,000 runs.
            result = self.request(f"{path}{separator}per_page=100&page={page}")
            items = result[key] if key else result
            yield from items
            if len(items) < 100:
                return


def current_mutes(gh, branch, preset):
    sha = gh.request(f"/commits/{urllib.parse.quote(branch, safe='')}")["sha"]
    config = json.loads(gh.request(f"/contents/.github/config/mute_config.json?ref={sha}", raw=True))
    path = config["muted_ya_paths"].get(preset, config["default_muted_ya_path"])
    text = gh.request(f"/contents/{path}?ref={sha}", raw=True)
    matcher = YaMuteCheck()
    for line in text.splitlines():
        if line.strip():
            suite, test = line.strip().split(" ", 1)
            matcher.populate(suite, test)
    return matcher, sha


def failures(report):
    """Reject incomplete/malformed reports and failures other than test cases."""
    results = report["results"]
    if not isinstance(results, list) or not results:
        raise ValueError("Empty or invalid test report")
    failed = []
    for result in results:
        status = result["status"]
        if status not in ("PASSED", "SKIPPED", "MUTE", "FAILED", "ERROR"):
            raise ValueError(f"Unexpected test status: {status}")
        if status in ("FAILED", "ERROR"):
            name = ".".join(filter(None, (result.get("name"), result.get("subtest_name"))))
            if (result.get("type") != "test" or result.get("suite") or result.get("chunk")
                    or not result.get("path") or not name):
                raise ValueError("Failure is not an individual test")
            failed.append((result, name))
    return failed


def accepted_failures(report, matcher):
    failed = failures(report)
    if not failed or not all(matcher(result["path"], name) for result, name in failed):
        return []
    return failed


def explanation(gh, accepted, sha, report_url):
    names = "\n".join(f"- `{result['path']}/{name}`" for result, name in accepted)
    return (
        f"🟢 CI revalidated against [current mute rules](https://github.com/{gh.repo}/commit/{sha}). "
        "Build passed; all remaining failures are now muted. Tests were not rerun.\n\n"
        f"{names}\n\n[Original test report]({report_url})"
    )


def apply_current_mutes(gh, report_path):
    """Run immediately before CI publishes its final test status."""
    path = Path(report_path)
    report = json.loads(path.read_text())
    if not failures(report):
        return
    matcher, sha = current_mutes(gh, os.environ["GITHUB_BASE_REF"], os.environ["BUILD_PRESET"])
    accepted = accepted_failures(report, matcher)
    if not accepted:
        return
    message = explanation(gh, accepted, sha, os.environ["PUBLIC_DIR_URL"] + "/index.html")
    # Preserve the original report; the final checker reads this separate copy.
    for result, _ in accepted:
        result.update(status="MUTE", muted=True)
    updated = path.with_name("revalidated_report.json")
    updated.write_text(json.dumps(report))
    Path("mute_revalidation_comment.txt").write_text(message)


def record_failures(report_path):
    report = json.loads(Path(report_path).read_text())
    failed = failures(report)
    if not failed:
        return
    event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())
    pr = event["pull_request"]
    record = {
        "run_id": int(os.environ["GITHUB_RUN_ID"]),
        "run_attempt": int(os.environ["GITHUB_RUN_ATTEMPT"]),
        "pr": pr["number"],
        "head_sha": pr["head"]["sha"],
        "base": pr["base"]["ref"],
        "preset": os.environ["BUILD_PRESET"],
        "results": [{key: result[key] for key in ("type", "path", "name", "subtest_name", "status")
                     if key in result} for result, _ in failed],
    }
    Path(os.environ["PUBLIC_DIR"], "mute_revalidation.json").write_text(json.dumps(record))


def statuses(gh, sha):
    return {item["context"]: item for item in gh.request(f"/commits/{sha}/status")["statuses"]}


def is_current(gh, run, record):
    pr = gh.request(f"/pulls/{record['pr']}")
    if pr["state"] != "open" or pr["head"]["sha"] != record["head_sha"] or pr["base"]["ref"] != record["base"]:
        return False
    latest = gh.request(f"/actions/runs/{run['id']}")
    if latest["status"] != "completed" or latest["conclusion"] != "failure" or latest["run_attempt"] != record["run_attempt"]:
        return False
    current = statuses(gh, record["head_sha"])
    integrated = current.get("checks_integrated", {})
    test = current.get("test_relwithdebinfo", {})
    return (
        integrated.get("target_url") == run["html_url"]
        and integrated.get("state") != "pending"
        and current.get("build_relwithdebinfo", {}).get("state") == "success"
        and (test.get("state") == "failure" or (
            test.get("state") == "success" and test.get("description") == REVALIDATED_DESCRIPTION))
    )


def revalidate_run(gh, run, dry_run):
    if (run["path"] != ".github/workflows/pr_check.yml" or run["event"] != "pull_request_target"
            or run["status"] != "completed" or run["conclusion"] != "failure"):
        return
    # This is the only required test preset in pr_check.yml. Keep the gate unchanged.
    prefix = (f"{os.environ['AWS_ENDPOINT'].rstrip('/')}/{os.environ['AWS_BUCKET']}/"
              f"{gh.repo}/PR-check/{run['id']}/ya-x86-64")
    try:
        with urllib.request.urlopen(prefix + "/mute_revalidation.json", timeout=60) as response:
            record = json.load(response)
    except urllib.error.HTTPError as error:
        if error.code == 404:  # Pre-rollout runs and runs with infrastructure failures.
            return
        raise
    if (record["run_id"] != run["id"] or record["run_attempt"] != run["run_attempt"]
            or record["head_sha"] != run["head_sha"] or record["preset"] != "relwithdebinfo"):
        return
    if not is_current(gh, run, record):
        return
    # A post-action failure (e.g. checkout cleanup) must not be forgiven either.
    jobs = list(gh.pages(f"/actions/runs/{run['id']}/attempts/{run['run_attempt']}/jobs", "jobs"))
    if not any(job["name"] == "Build and test relwithdebinfo" and job["conclusion"] == "failure" for job in jobs):
        return
    for job in jobs:
        if job["conclusion"] not in ("success", "skipped", "failure"):
            return
        if job["conclusion"] == "failure":
            if job["name"] != "Build and test relwithdebinfo":
                return
            if [step["name"] for step in job["steps"] if step["conclusion"] == "failure"] != ["Build and test"]:
                return
    matcher, sha = current_mutes(gh, record["base"], record["preset"])
    accepted = accepted_failures(record, matcher)
    if not accepted:
        return
    message = explanation(gh, accepted, sha, prefix + "/index.html")
    print(f"PR #{record['pr']}, run {run['id']}: {message}")
    if dry_run or not is_current(gh, run, record):
        return
    # Comment first: a status must never become green without its explanation.
    previous = statuses(gh, record["head_sha"])["test_relwithdebinfo"]
    if previous["state"] == "success" and previous.get("description") == REVALIDATED_DESCRIPTION:
        comment_url = previous["target_url"]  # Resume a partially published revalidation.
    else:
        comment_url = gh.request(f"/issues/{record['pr']}/comments", {"body": message})["html_url"]
    if not is_current(gh, run, record):
        return
    gh.request(f"/statuses/{record['head_sha']}", {
        "context": "test_relwithdebinfo", "state": "success",
        "description": REVALIDATED_DESCRIPTION,
        "target_url": comment_url,
    })
    current = statuses(gh, record["head_sha"])
    # Do not replace a newer run's pending integrated status.
    if (current["checks_integrated"]["target_url"] == run["html_url"]
            and current["checks_integrated"]["state"] != "pending"
            and all(current.get(name, {}).get("state") == "success"
                    for name in ("build_relwithdebinfo", "test_relwithdebinfo"))):
        gh.request(f"/statuses/{record['head_sha']}", {
            "context": "checks_integrated", "state": "success",
            "description": "Build passed; remaining test failures accepted by current mute rules",
            "target_url": comment_url,
        })


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("apply", "record", "recent"))
    parser.add_argument("--report")
    parser.add_argument("--run-id", type=int)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    gh = GitHub()
    if args.mode == "apply":
        apply_current_mutes(gh, args.report)
    elif args.mode == "record":
        record_failures(args.report)
    else:
        if args.run_id:
            runs = [gh.request(f"/actions/runs/{args.run_id}")]
        else:
            since = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=7)).isoformat()
            runs = gh.pages("/actions/workflows/pr_check.yml/runs?event=pull_request_target"
                            f"&status=failure&created={urllib.parse.quote('>=' + since)}", "workflow_runs")
        for run in runs:
            try:
                revalidate_run(gh, run, args.dry_run)
            except (KeyError, ValueError, OSError) as error:
                print(f"::warning::Run {run['id']} was not revalidated: {error}")


if __name__ == "__main__":
    main()
