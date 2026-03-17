"""
Code for `sarif blame` command.
"""

import json
import os
import subprocess
import sys
from typing import Callable, Iterable, List, Union
import urllib.parse
import urllib.request

from sarif.sarif_file import SarifFileSet


def _run_git_blame(repo_path: str, file_path: str) -> List[bytes]:
    cmd = ["git", "blame", "--porcelain", _make_path_git_compatible(file_path)]
    with subprocess.Popen(cmd, stdout=subprocess.PIPE, cwd=repo_path) as proc:
        result = []
        if proc.stdout:
            result = [x for x in proc.stdout.readlines()]

        # Ensure process terminates
        proc.communicate()
        if proc.returncode:
            cmd_str = " ".join(cmd)
            sys.stderr.write(
                f"WARNING: Command `{cmd_str} "
                f"failed with exit code {proc.returncode} in {repo_path}\n"
            )

        return result


def enhance_with_blame(
    input_files: SarifFileSet,
    repo_path: str,
    output: str,
    output_multiple_files: bool,
    run_git_blame: Callable[[str, str], List[bytes]] = _run_git_blame,
):
    """
    Enhance SARIF files with information from `git blame`.  The `git` command is run in the current
    directory, which must be a git repository containing the files at the paths specified in the
    input files.  Updated files are written to output_path if specified, otherwise to the current
    directory.
    """
    if not input_files:
        return
    if not os.path.isdir(repo_path):
        raise ValueError(f"No git repository directory found at {repo_path}")

    _enhance_with_blame(input_files, repo_path, run_git_blame)

    for input_file in input_files:
        input_file_name = input_file.get_file_name()
        if any(
            "blame" in result.get("properties", {})
            for result in input_file.get_results()
        ):
            output_file = output
            if output_multiple_files:
                output_filename = (
                    input_file.get_file_name_without_extension()
                    + "_with_blame."
                    + input_file.get_file_name_extension()
                )
                output_file = os.path.join(output, output_filename)
            print(
                "Writing",
                output_file,
                "combining original SARIF from",
                input_file_name,
                "with git blame information",
            )
            with open(output_file, "w", encoding="utf-8") as file_out:
                json.dump(input_file.data, file_out)
        else:
            sys.stderr.write(
                f"WARNING: did not find any git blame information for {input_file_name}\n"
            )


def _enhance_with_blame(
    input_files: SarifFileSet,
    repo_path: str,
    run_git_blame: Callable[[str, str], List[bytes]],
):
    """
    Run `git blame --porcelain` for each file path listed in input_files.
    Then enhance the results in error_list by adding a "blame" property including "hash", "author"
    and "timestamp".
    Porcelain format is used for parseability and stability.  See documentation at
    https://git-scm.com/docs/git-blame#_the_porcelain_format.
    """
    files_to_blame = set(item["Location"] for item in input_files.get_records())
    file_count = len(files_to_blame)
    print(
        "Running `git blame --porcelain` on",
        "one file" if file_count == 1 else f"{file_count} files",
        "in",
        repo_path,
    )
    file_blame_info = _run_git_blame_on_files(files_to_blame, repo_path, run_git_blame)

    # Now join up blame output with result list
    blame_info_count = 0
    item_count = 0
    for result, record in zip(input_files.get_results(), input_files.get_records()):
        item_count += 1
        file_path = record["Location"]
        if file_path in file_blame_info:
            blame_info = file_blame_info[file_path]
            # raw_line can be None if no line number information was included in the SARIF result.
            raw_line = record["Line"]
            if raw_line:
                line_no = str(raw_line)
                if line_no in blame_info["line_to_commit"]:
                    commit_hash = blame_info["line_to_commit"][line_no]
                    commit = blame_info["commits"][commit_hash]
                    # Add blame information to the SARIF Property Bag of the result
                    result.setdefault("properties", {})["blame"] = commit
                    blame_info_count += 1
    print(f"Found blame information for {blame_info_count} of {item_count} results")


def _make_path_git_compatible(file_path):
    try:
        path_as_url = urllib.parse.urlparse(file_path)
        if path_as_url.scheme == "file":
            return urllib.request.url2pathname(path_as_url.path)
        return file_path
    except ValueError:
        return file_path


def _run_git_blame_on_files(
    files_to_blame: Iterable[str],
    repo_path: str,
    run_git_blame: Callable[[str, str], List[bytes]],
):
    file_blame_info = {}
    for file_path in files_to_blame:
        git_blame_output = run_git_blame(repo_path, file_path)
        blame_info = {"commits": {}, "line_to_commit": {}}
        file_blame_info[file_path] = blame_info
        commit_hash: Union[str, None] = None

        for line_bytes in git_blame_output:
            # Convert byte sequence to string and remove trailing LF
            line_string = line_bytes.decode("utf-8", errors="replace")[:-1]
            # Now parse output from git blame --porcelain
            if commit_hash:
                if line_string.startswith("\t"):
                    commit_hash = None
                    # Ignore line contents = source code
                elif " " in line_string:
                    space_pos = line_string.index(" ")
                    key = line_string[0:space_pos]
                    value = line_string[space_pos + 1 :].strip()
                    blame_info["commits"][commit_hash][key] = value
                else:
                    # e.g. "boundary"
                    key = line_string
                    blame_info["commits"][commit_hash][key] = True
            else:
                commit_line_info = line_string.split(" ")
                commit_hash = commit_line_info[0]
                commit_line = commit_line_info[2]
                blame_info["commits"].setdefault(commit_hash, {})
                blame_info["line_to_commit"][commit_line] = commit_hash

    return file_blame_info
