# -*- coding:utf-8 -*-

#  ************************** Copyrights and license ***************************
#
# This file is part of gcovr 8.6, a parsing and reporting tool for gcov.
# https://gcovr.com/en/8.6
#
# _____________________________________________________________________________
#
# Copyright (c) 2013-2026 the gcovr authors
# Copyright (c) 2013 Sandia Corporation.
# Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
# the U.S. Government retains certain rights in this software.
#
# This software is distributed under the 3-clause BSD License.
# For more information, see the README.rst file.
#
# ****************************************************************************

from __future__ import absolute_import

import datetime
import os
import re
import shutil
import subprocess  # nosec # Commands are trusted.
from typing import Any

from ...data_model.container import CoverageContainer
from ...data_model.coverage import FileCoverage
from ...exceptions import SanityCheckError
from ...options import Options
from ...utils import get_md5_hexdigest, write_json_output


def write_report(
    covdata: CoverageContainer, output_file: str, options: Options
) -> None:
    """
    Outputs a JSON report in the Coveralls API coverage format

    @param covdata: is a dictionary of file coverage objects, keyed with an absolute filepath
    @param output_file: is the name of the file to create
    @param options: options object
    """

    # Create object to collect coverage data (https://docs.coveralls.io/api-jobs-endpoint#json-object-job)
    json_dict = dict[str, Any]()

    # Capture timestamp
    timestamp: datetime.datetime = options.timestamp.astimezone(datetime.timezone.utc)
    json_dict["run_at"] = timestamp.strftime("%Y-%m-%d %H:%M:%S UTC")

    # Pull environment variables
    if os.environ.get("COVERALLS_REPO_TOKEN") is not None:
        json_dict["repo_token"] = os.environ.get("COVERALLS_REPO_TOKEN", "")

    current_branch = None
    current_commit = None
    current_pull_request = None
    # Stub for own test suite
    if os.environ.get("GCOVR_TEST_SUITE") is not None:
        json_dict["service_name"] = "gcovr-test-suite"
        json_dict["service_job_id"] = "id"
        json_dict["service_number"] = "number"
        current_pull_request = "pr"
        current_branch = "branch"
        current_commit = None
    # Consume Travis CI specific environment variables _(if available)_
    # See https://docs.travis-ci.com/user/environment-variables
    elif os.environ.get("TRAVIS_JOB_ID") is not None:
        json_dict["service_name"] = "travis-ci"
        json_dict["service_job_id"] = os.environ.get("TRAVIS_JOB_ID", "")
        json_dict["service_number"] = os.environ.get("TRAVIS_BUILD_NUMBER", "")
        current_commit = os.environ.get("TRAVIS_COMMIT", "")
        current_pull_request = os.environ.get("TRAVIS_PULL_REQUEST", "")
        current_branch = os.environ.get("TRAVIS_BRANCH", "")
    # Consume Appveyor specific environment variables _(if available)_
    # See https://www.appveyor.com/docs/environment-variables/
    elif os.environ.get("APPVEYOR_URL") is not None:
        json_dict["service_name"] = "appveyor"
        json_dict["service_job_id"] = os.environ.get("APPVEYOR_JOB_ID", "")
        json_dict["service_number"] = os.environ.get("APPVEYOR_JOB_NUMBER", "")
        current_commit = os.environ.get("APPVEYOR_REPO_COMMIT", "")
        current_pull_request = os.environ.get("APPVEYOR_PULL_REQUEST_NUMBER", "")
        current_branch = os.environ.get("APPVEYOR_REPO_BRANCH", "")
    # Consume Jenkins specific environment variables _(if available)_
    # See https://opensource.triology.de/jenkins/pipeline-syntax/globals
    elif os.environ.get("JENKINS_URL") is not None:
        json_dict["service_name"] = "jenkins-ci"
        json_dict["service_job_id"] = os.environ.get("JOB_NAME", "")
        json_dict["service_number"] = os.environ.get("BUILD_ID", "")
        if os.environ.get("GIT_COMMIT") is not None:
            current_commit = os.environ.get("GIT_COMMIT", "")
        current_pull_request = os.environ.get("CHANGE_ID", "")
        current_branch = os.environ.get("BRANCH_NAME", "")
    # Consume GitHup Actions specific environment variables _(if available)_
    # See https://docs.github.com/en/actions/configuring-and-managing-workflows/using-environment-variables#default-environment-variables
    elif os.environ.get("GITHUB_ACTIONS") is not None:
        json_dict["service_name"] = "github-actions-ci"
        json_dict["service_job_id"] = os.environ.get("GITHUB_WORKFLOW", "")
        json_dict["service_number"] = os.environ.get("GITHUB_RUN_ID", "")
        current_commit = os.environ.get("GITHUB_SHA", "")
        if os.environ.get("GITHUB_HEAD_REF") is not None:
            current_pull_request = re.sub(
                r"^refs/pull/(\d+)/merge$", r"\1", os.environ.get("GITHUB_HEAD_REF", "")
            )
            current_branch = os.environ.get("GITHUB_REF", "")
        else:
            current_branch = re.sub(
                r"^refs/heads/", "", os.environ.get("GITHUB_REF", "")
            )

    if current_pull_request is not None:
        json_dict["service_pull_request"] = current_pull_request

    git = (
        shutil.which("git")
        if os.environ.get("GCOVR_TEST_SUITE_NO_GIT_COMMAND") is None
        else None
    )

    def run_git_cmd(*args: str) -> str:
        if git is None:
            raise SanityCheckError("Function must only be executed if git is found.")
        process = subprocess.run(  # nosec # We execute git
            [git] + list(args),
            stdout=subprocess.PIPE,
            cwd=options.root_dir,
            encoding="utf-8",
            check=True,
        )
        return process.stdout.rstrip()

    def run_git_log_cmd(arg: str) -> str:
        return run_git_cmd("--no-pager", "log", "-1", f"--pretty=format:{arg}")

    if git and "true" in run_git_cmd("rev-parse", "--is-inside-work-tree"):
        if current_branch is None:
            current_branch = run_git_cmd("rev-parse", "--abbrev-ref", "HEAD")
        if current_commit is None:
            current_commit = run_git_log_cmd("%H")

        json_dict["git"] = {
            "head": {
                "id": current_commit,
                "author_name": run_git_log_cmd("%aN"),
                "author_email": run_git_log_cmd("%ae"),
                "committer_name": run_git_log_cmd("%cN"),
                "committer_email": run_git_log_cmd("%ce"),
                "message": run_git_log_cmd("%s"),
            },
            "branch": current_branch,
            "remotes": [
                {"name": line.split()[0], "url": line.split()[1]}
                for line in run_git_cmd("remote", "-v").split("\n")
                if line.endswith("(fetch)")
            ],
        }
    elif current_commit is not None:
        json_dict["commit_sha"] = current_commit

    # Loop through each coverage file collecting details
    json_dict["source_files"] = []
    for _, filecov in sorted(covdata.items()):
        # File data has been compiled
        json_dict["source_files"].append(_make_source_file(filecov, options))

    write_json_output(
        json_dict,
        pretty=options.coveralls_pretty,
        filename=output_file,
        default_filename="coveralls.json",
    )


def _make_source_file(filecov: FileCoverage, options: Options) -> dict[str, Any]:
    # Object with Coveralls file details (https://docs.coveralls.io/api-jobs-endpoint#json-object-source-file)
    source_file = dict[str, Any]()

    # Isolate relative file path
    relative_file_path = filecov.presentable_filename(options.root_filter)
    source_file["name"] = relative_file_path

    # Generate md5 hash of file contents
    if filecov.filename.endswith("<stdin>"):
        total_line_count = None
    else:
        with open(filecov.filename, "rb") as file_handle:
            contents = file_handle.read()

        source_file["source_digest"] = get_md5_hexdigest(contents)
        total_line_count = len(contents.splitlines())

    # Initialize coverage array and load with line coverage data
    coverage = list[int | None]()
    branches = list[int | None]()
    source_file["coverage"] = coverage
    # source_file['branches'] = []
    for linecov_collection in filecov.lines(sort=True):
        # Comment lines are not collected in `covdata`, but must
        # be reported to coveralls (fill missing lines)
        _extend_with_none(coverage, linecov_collection.lineno - 1)
        linecov_collection = linecov_collection.merge_lines()
        linecov = list(linecov_collection.linecov())[0]
        if linecov_collection.is_reportable:
            coverage.append(linecov.count)
        else:
            coverage.append(None)

        # Record branch information (INCOMPLETE/OMITTED)
        for branchno, branchcov in enumerate(
            branchcov
            for branchcov in linecov.branches(sort=True)
            if branchcov.is_reportable
        ):
            branches.append(linecov.lineno)
            branches.append(branchcov.source_block_id_or_0)
            branches.append(branchno)
            branches.append(branchcov.count)

    if branches:
        source_file["branches"] = branches

    # add trailing empty lines
    if total_line_count is not None:
        _extend_with_none(coverage, total_line_count)

    return source_file


def _extend_with_none(target: list[int | None], wanted_len: int) -> None:
    current_len = len(target)
    target.extend(None for _ in range(current_len, wanted_len))
