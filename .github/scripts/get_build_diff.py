#!/usr/bin/env python3

import datetime
from decimal import Decimal, ROUND_HALF_UP
import get_current_build_size
import get_main_build_size
import humanize
import os
import subprocess


# Форматирование числа
def format_number(num):
    return humanize.intcomma(num).replace(",", " ")


def bytes_to_human_iec(num):
    return humanize.naturalsize(num, binary=True)


def main():

    yellow_treshold = int(os.environ.get("yellow_treshold"))
    red_treshold = int(os.environ.get("red_treshold"))

    github_srv = os.environ.get("GITHUB_SERVER_URL")
    repo_name = os.environ.get("GITHUB_REPOSITORY")
    repo_url = f"{github_srv}/{repo_name}/"

    branch = os.environ.get("branch_to_compare")
    current_pr_commit_sha = os.environ.get("commit_git_sha")

    if current_pr_commit_sha is not None:
        git_commit_time_bytes = subprocess.check_output(
            ["git", "show", "--no-patch", "--format=%cI", current_pr_commit_sha]
        )
        git_commit_time = datetime.datetime.fromisoformat(
            git_commit_time_bytes.decode("utf-8").strip()
        )
        git_commit_time_unix = int(git_commit_time.timestamp())
    else:
        print(f"Error: Cant get commit {current_pr_commit_sha} timestamp")
        return 1

    current_sizes_result = get_current_build_size.get_build_size()
    main_sizes_result = get_main_build_size.get_build_size(git_commit_time_unix)

    if main_sizes_result and current_sizes_result:
        main_github_sha = main_sizes_result["github_sha"]
        main_size_bytes = int(main_sizes_result["size_bytes"])
        main_size_stripped_bytes = int(main_sizes_result["size_stripped_bytes"])

        current_size_bytes = int(current_sizes_result["size_bytes"])
        current_size_stripped_bytes = int(current_sizes_result["size_stripped_bytes"])

        bytes_diff = current_size_bytes - main_size_bytes
        stripped_bytes_diff = current_size_stripped_bytes - main_size_stripped_bytes

        diff_perc = Decimal(bytes_diff * 100 / main_size_bytes).quantize(
            Decimal(".001"), rounding=ROUND_HALF_UP
        )
        stripped_diff_perc = Decimal(
            stripped_bytes_diff * 100 / main_size_stripped_bytes
        ).quantize(Decimal(".001"), rounding=ROUND_HALF_UP)

        human_readable_size = bytes_to_human_iec(current_size_bytes)
        human_readable_size_diff = bytes_to_human_iec(bytes_diff)
        human_readable_stripped_size_diff = bytes_to_human_iec(stripped_bytes_diff)
        if bytes_diff > 0:
            sign = "+"
            if bytes_diff >= red_treshold:
                color = "red"
                summary_core = f" >= {bytes_to_human_iec(red_treshold)} vs {branch}: **Alert**"
            elif bytes_diff >= yellow_treshold:
                color = "yellow"
                summary_core = f" >= {bytes_to_human_iec(yellow_treshold)} vs {branch}: **Warning**"
            else:
                color = "green"
                summary_core = f" < {bytes_to_human_iec(yellow_treshold)} vs {branch}: **OK**"
        else:
            sign = ""
            color = "green"
            summary_core = f" <= 0 Bytes vs {branch}: **OK**"

        if stripped_bytes_diff > 0:
            stripped_sign = "+"
        else:
            stripped_sign = ""

        summary_start = f"ydbd size **{human_readable_size}** changed* by **{sign}{human_readable_size_diff}**, which is"
        summary = f"{summary_start}{summary_core}"

        comment = (
            f"{summary}\n"
            f"|[ydbd size dash](https://datalens.yandex/cu6hzmpaki700)|{branch}: {main_github_sha} |merge: {current_pr_commit_sha} |diff | diff %%|\n"
            f"|:--- | ---: | ---: | ---: | ---: |\n"
            f"|ydbd size|**{format_number(main_size_bytes)}** Bytes |**{format_number(current_size_bytes)}** Bytes|**{sign}{human_readable_size_diff}**|**{sign}{diff_perc}%%**|\n"
            f"|ydbd stripped size|**{format_number(main_size_stripped_bytes)}** Bytes|**{format_number(current_size_stripped_bytes)}** Bytes|**{stripped_sign}{human_readable_stripped_size_diff}**|**{stripped_sign}{stripped_diff_perc}%%**|\n\n"
            f"<sup>*please be aware that the difference is based on comparing your commit and the last completed build from the post-commit, check [comparation]({repo_url}compare/{main_github_sha}..{current_pr_commit_sha})</sup>"
        )
        print(f"{color};;;{comment}")
    else:
        print(
            f"Error: Cant get build data: {branch}_sizes_result = {main_sizes_result}, current_sizes_result = {current_sizes_result}"
        )


if __name__ == "__main__":
    main()
