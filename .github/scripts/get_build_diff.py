#!/usr/bin/env python3

from decimal import Decimal, ROUND_HALF_UP
import get_current_build_size
import get_main_build_size
import humanize
import os


# Форматирование числа
def format_number(num):
    return humanize.intcomma(num).replace(",", " ")


def bytes_to_human_iec(num):
    return humanize.naturalsize(num, binary=True)


def main():

    yellow_treshold = int(os.environ.get("yellow_treshold"))
    red_treshold = int(os.environ.get("red_treshold"))

    branch = os.environ.get("branch_to_compare")
    current_pr_commit_sha = os.environ.get("commit_git_sha")

    current_sizes_result = get_current_build_size.get_build_size()
    main_sizes_result = get_main_build_size.get_build_size()

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
        human_readable_stripped_size_diff = bytes_to_human_iec(stripped_diff_perc)

        if bytes_diff >= 0:
            sign = "+"
            if bytes_diff >= red_treshold:
                color = "red"
            elif bytes_diff >= yellow_treshold:
                color = "yellow"
            else:
                color = "green"
        else:
            sign = ""
            color = "green"

        if stripped_diff_perc > 0:
            stripped_sign = "+"
        else:
            stripped_sign = ""

        comment = (
            f"merge: {current_pr_commit_sha} ydbd size {human_readable_size} **{sign}{human_readable_size_diff} {sign}{diff_perc}%%** vs build {branch}: {main_github_sha}\n\n"
            "<details><summary>Build size details</summary><p>\n\n"
            f"{branch}: {main_github_sha} |merge: {current_pr_commit_sha} |diff | diff %%|\n"
            f"| ---: | ---: | ---: | ---: |\n"
            f"|**{format_number(main_size_bytes)}** Bytes |**{format_number(current_size_bytes)}** Bytes|**{sign}{human_readable_size_diff}**|{sign}{diff_perc}%%**|\n"
            f"|**{format_number(main_size_stripped_bytes)}** Bytes|**{format_number(current_size_stripped_bytes)}** Bytes|**{stripped_sign}{human_readable_stripped_size_diff}**|**{stripped_sign}{stripped_diff_perc}%%**|\n\n"
            "[ydbd size dashboard](https://datalens.yandex/cu6hzmpaki700)\n\n"
            "</p></details>"
        )
        print(f"{color};;;{comment}")
    else:
        print(f'Error: Cant get build data: {branch}_sizes_result = {main_sizes_result}, current_sizes_result = {current_sizes_result}')   
    

if __name__ == "__main__":
    main()
