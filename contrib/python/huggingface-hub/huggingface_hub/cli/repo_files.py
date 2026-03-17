# coding=utf-8
# Copyright 2023-present, the HuggingFace Inc. team.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Legacy `hf repo-files` command.

Kept for backward compatibility. Users are nudged to use `hf repos delete-files` instead.
"""

import sys
from typing import Annotated, Optional

import typer

from huggingface_hub import logging
from huggingface_hub.utils import ANSI

from ._cli_utils import RepoIdArg, RepoType, RepoTypeOpt, RevisionOpt, TokenOpt, get_hf_api, typer_factory


logger = logging.get_logger(__name__)


repo_files_cli = typer_factory(
    help="(Deprecated) Manage files in a repo on the Hub. Use `hf repos delete-files` instead."
)


@repo_files_cli.command(
    "delete",
)
def repo_files_delete(
    repo_id: RepoIdArg,
    patterns: Annotated[
        list[str],
        typer.Argument(
            help="Glob patterns to match files to delete. Based on fnmatch, '*' matches files recursively.",
        ),
    ],
    repo_type: RepoTypeOpt = RepoType.model,
    revision: RevisionOpt = None,
    commit_message: Annotated[
        Optional[str],
        typer.Option(
            help="The summary / title / first line of the generated commit.",
        ),
    ] = None,
    commit_description: Annotated[
        Optional[str],
        typer.Option(
            help="The description of the generated commit.",
        ),
    ] = None,
    create_pr: Annotated[
        bool,
        typer.Option(
            help="Whether to create a new Pull Request for these changes.",
        ),
    ] = False,
    token: TokenOpt = None,
) -> None:
    print(
        ANSI.yellow("FutureWarning: `hf repo-files delete` is deprecated. Use `hf repos delete-files` instead."),
        file=sys.stderr,
    )
    api = get_hf_api(token=token)
    url = api.delete_files(
        delete_patterns=patterns,
        repo_id=repo_id,
        repo_type=repo_type.value,
        revision=revision,
        commit_message=commit_message,
        commit_description=commit_description,
        create_pr=create_pr,
    )
    print(f"Files correctly deleted from repo. Commit: {url}.")
    logging.set_verbosity_warning()
