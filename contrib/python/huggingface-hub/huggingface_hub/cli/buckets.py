# coding=utf-8
# Copyright 2025-present, the HuggingFace Inc. team.
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
"""Contains commands to interact with buckets via the CLI."""

import json
import os
import sys
import tempfile
from datetime import datetime
from typing import Annotated, Optional, Union

import typer

from huggingface_hub import logging
from huggingface_hub._buckets import (
    BUCKET_PREFIX,
    BucketFile,
    BucketFolder,
    FilterMatcher,
    _is_bucket_path,
    _parse_bucket_path,
    _split_bucket_id_and_prefix,
)
from huggingface_hub.utils import StatusLine, are_progress_bars_disabled, disable_progress_bars, enable_progress_bars

from ._cli_utils import (
    FormatOpt,
    OutputFormat,
    QuietOpt,
    TokenOpt,
    api_object_to_dict,
    get_hf_api,
    print_list_output,
    typer_factory,
)


logger = logging.get_logger(__name__)


buckets_cli = typer_factory(help="Commands to interact with buckets.")


def _parse_bucket_argument(argument: str) -> tuple[str, str]:
    """Parse a bucket argument accepting both 'namespace/name(/prefix)' and 'hf://buckets/namespace/name(/prefix)'.

    Returns:
        tuple: (bucket_id, prefix) where bucket_id is "namespace/bucket_name" and prefix may be empty string.
    """
    if argument.startswith(BUCKET_PREFIX):
        return _parse_bucket_path(argument)
    try:
        return _split_bucket_id_and_prefix(argument)
    except ValueError:
        raise ValueError(
            f"Invalid bucket argument: {argument}. Must be in format namespace/bucket_name"
            f" or {BUCKET_PREFIX}namespace/bucket_name"
        )


def _format_size(size: Union[int, float], human_readable: bool = False) -> str:
    """Format a size in bytes."""
    if not human_readable:
        return str(size)

    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1000:
            if unit == "B":
                return f"{size} {unit}"
            return f"{size:.1f} {unit}"
        size /= 1000
    return f"{size:.1f} PB"


def _format_mtime(mtime: Optional[datetime], human_readable: bool = False) -> str:
    """Format mtime datetime to a readable date string."""
    if mtime is None:
        return ""
    if human_readable:
        return mtime.strftime("%b %d %H:%M")
    return mtime.strftime("%Y-%m-%d %H:%M:%S")


def _build_tree(
    items: list[Union[BucketFile, BucketFolder]],
    human_readable: bool = False,
    quiet: bool = False,
) -> list[str]:
    """Build a tree representation of files and directories.

    Produces ASCII tree with size and date columns before the tree connector.
    When quiet=True, only the tree structure is shown (no size/date).

    Args:
        items: List of BucketFile/BucketFolder items
        human_readable: Whether to show human-readable sizes and short dates
        quiet: If True, show only the tree structure without sizes/dates

    Returns:
        List of formatted tree lines
    """
    # Build a nested structure
    tree: dict = {}

    for item in items:
        parts = item.path.split("/")
        current = tree
        for part in parts[:-1]:
            if part not in current:
                current[part] = {"__children__": {}}
            current = current[part]["__children__"]

        final_part = parts[-1]
        if isinstance(item, BucketFolder):
            if final_part not in current:
                current[final_part] = {"__children__": {}}
        else:
            current[final_part] = {"__item__": item}

    # Compute prefix width for alignment (size + date columns)
    prefix_width = 0
    max_size_width = 0
    max_date_width = 0
    if not quiet:
        for item in items:
            if isinstance(item, BucketFile):
                size_str = _format_size(item.size, human_readable)
                max_size_width = max(max_size_width, len(size_str))
                date_str = _format_mtime(item.mtime, human_readable)
                max_date_width = max(max_date_width, len(date_str))
        if max_size_width > 0:
            prefix_width = max_size_width + 2 + max_date_width

    # Render tree
    lines: list[str] = []
    _render_tree(
        tree,
        lines,
        "",
        prefix_width=prefix_width,
        max_size_width=max_size_width,
        human_readable=human_readable,
    )
    return lines


def _render_tree(
    node: dict,
    lines: list[str],
    indent: str,
    prefix_width: int = 0,
    max_size_width: int = 0,
    human_readable: bool = False,
) -> None:
    """Recursively render a tree structure with size+date prefix."""
    items = sorted(node.items())
    for i, (name, value) in enumerate(items):
        is_last = i == len(items) - 1
        connector = "└── " if is_last else "├── "

        is_dir = "__children__" in value
        children = value.get("__children__", {})

        if prefix_width > 0:
            if is_dir:
                prefix = " " * prefix_width
            else:
                item = value.get("__item__")
                if item is not None:
                    size_str = _format_size(item.size, human_readable)
                    date_str = _format_mtime(item.mtime, human_readable)
                    prefix = f"{size_str:>{max_size_width}}  {date_str}"
                else:
                    prefix = " " * prefix_width
            lines.append(f"{prefix}  {indent}{connector}{name}{'/' if is_dir else ''}")
        else:
            lines.append(f"{indent}{connector}{name}{'/' if is_dir else ''}")

        if children:
            child_indent = indent + ("    " if is_last else "│   ")
            _render_tree(
                children,
                lines,
                child_indent,
                prefix_width=prefix_width,
                max_size_width=max_size_width,
                human_readable=human_readable,
            )


@buckets_cli.command(
    name="create",
    examples=[
        "hf buckets create my-bucket",
        "hf buckets create user/my-bucket",
        "hf buckets create hf://buckets/user/my-bucket",
        "hf buckets create user/my-bucket --private",
        "hf buckets create user/my-bucket --exist-ok",
    ],
)
def create(
    bucket_id: Annotated[
        str,
        typer.Argument(
            help="Bucket ID: bucket_name, namespace/bucket_name, or hf://buckets/namespace/bucket_name",
        ),
    ],
    private: Annotated[
        bool,
        typer.Option(
            "--private",
            help="Create a private bucket.",
        ),
    ] = False,
    exist_ok: Annotated[
        bool,
        typer.Option(
            "--exist-ok",
            help="Do not raise an error if the bucket already exists.",
        ),
    ] = False,
    quiet: QuietOpt = False,
    token: TokenOpt = None,
) -> None:
    """Create a new bucket."""
    api = get_hf_api(token=token)

    if bucket_id.startswith(BUCKET_PREFIX):
        try:
            parsed_id, prefix = _parse_bucket_argument(bucket_id)
        except ValueError as e:
            raise typer.BadParameter(str(e))
        if prefix:
            raise typer.BadParameter(
                f"Cannot specify a prefix for bucket creation: {bucket_id}."
                f" Use namespace/bucket_name or {BUCKET_PREFIX}namespace/bucket_name."
            )
        bucket_id = parsed_id

    bucket_url = api.create_bucket(
        bucket_id,
        private=private if private else None,
        exist_ok=exist_ok,
    )
    if quiet:
        print(bucket_url.handle)
    else:
        print(f"Bucket created: {bucket_url.url} (handle: {bucket_url.handle})")


def _is_bucket_id(argument: str) -> bool:
    """Check if argument is a bucket ID (namespace/name) vs just a namespace."""
    if argument.startswith(BUCKET_PREFIX):
        path = argument[len(BUCKET_PREFIX) :]
    else:
        path = argument
    return "/" in path


@buckets_cli.command(
    name="list | ls",
    examples=[
        "hf buckets list",
        "hf buckets list huggingface",
        "hf buckets list user/my-bucket",
        "hf buckets list user/my-bucket -R",
        "hf buckets list user/my-bucket -h",
        "hf buckets list user/my-bucket --tree",
        "hf buckets list user/my-bucket --tree -h",
        "hf buckets list hf://buckets/user/my-bucket",
        "hf buckets list user/my-bucket/sub -R",
    ],
)
def list_cmd(
    argument: Annotated[
        Optional[str],
        typer.Argument(
            help=(
                "Namespace (user or org) to list buckets, or bucket ID"
                " (namespace/bucket_name(/prefix) or hf://buckets/...) to list files."
            ),
        ),
    ] = None,
    human_readable: Annotated[
        bool,
        typer.Option(
            "--human-readable",
            "-h",
            help="Show sizes in human readable format.",
        ),
    ] = False,
    as_tree: Annotated[
        bool,
        typer.Option(
            "--tree",
            help="List files in tree format (only for listing files).",
        ),
    ] = False,
    recursive: Annotated[
        bool,
        typer.Option(
            "--recursive",
            "-R",
            help="List files recursively (only for listing files).",
        ),
    ] = False,
    format: FormatOpt = OutputFormat.table,
    quiet: QuietOpt = False,
    token: TokenOpt = None,
) -> None:
    """List buckets or files in a bucket.

    When called with no argument or a namespace, lists buckets.
    When called with a bucket ID (namespace/bucket_name), lists files in the bucket.
    """
    # Determine mode: listing buckets or listing files
    is_file_mode = argument is not None and _is_bucket_id(argument)

    if is_file_mode:
        _list_files(
            argument=argument,  # type: ignore[arg-type]
            human_readable=human_readable,
            as_tree=as_tree,
            recursive=recursive,
            format=format,
            quiet=quiet,
            token=token,
        )
    else:
        _list_buckets(
            namespace=argument,
            human_readable=human_readable,
            as_tree=as_tree,
            recursive=recursive,
            format=format,
            quiet=quiet,
            token=token,
        )


def _list_buckets(
    namespace: Optional[str],
    human_readable: bool,
    as_tree: bool,
    recursive: bool,
    format: OutputFormat,
    quiet: bool,
    token: Optional[str],
) -> None:
    """List buckets in a namespace."""
    # Validate incompatible flags
    if as_tree:
        raise typer.BadParameter("Cannot use --tree when listing buckets.")
    if recursive:
        raise typer.BadParameter("Cannot use --recursive when listing buckets.")

    # Handle hf://buckets/namespace format
    if namespace is not None and namespace.startswith(BUCKET_PREFIX):
        namespace = namespace[len(BUCKET_PREFIX) :]
        # Strip trailing slash if any
        namespace = namespace.rstrip("/")

    api = get_hf_api(token=token)
    results = [api_object_to_dict(bucket) for bucket in api.list_buckets(namespace=namespace)]

    if not results:
        if not quiet and format != OutputFormat.json:
            resolved_namespace = namespace if namespace is not None else api.whoami()["name"]
            print(f"No buckets found under namespace '{resolved_namespace}'.")
            return

    headers = ["id", "private", "size", "total_files", "created_at"]

    def row_fn(item: dict) -> list[str]:
        from ._cli_utils import _format_cell

        return [
            _format_cell(item.get("id")),
            _format_cell(item.get("private")),
            _format_size(item.get("size", 0), human_readable=human_readable),
            _format_cell(item.get("total_files")),
            _format_cell(item.get("created_at")),
        ]

    alignments = {"size": "right", "total_files": "right"}
    print_list_output(results, format=format, quiet=quiet, headers=headers, row_fn=row_fn, alignments=alignments)


def _list_files(
    argument: str,
    human_readable: bool,
    as_tree: bool,
    recursive: bool,
    format: OutputFormat,
    quiet: bool,
    token: Optional[str],
) -> None:
    """List files in a bucket."""
    # Validate incompatible flags
    if as_tree and format == OutputFormat.json:
        raise typer.BadParameter("Cannot use --tree with --format json.")

    api = get_hf_api(token=token)

    try:
        bucket_id, prefix = _parse_bucket_argument(argument)
    except ValueError as e:
        raise typer.BadParameter(str(e))

    # Fetch items from the bucket
    items = list(
        api.list_bucket_tree(
            bucket_id,
            prefix=prefix or None,
            recursive=recursive,
        )
    )

    if not items:
        print("(empty)")
        return

    has_directories = any(isinstance(item, BucketFolder) for item in items)

    if format == OutputFormat.json:
        results = [api_object_to_dict(item) for item in items]
        print(json.dumps(results, indent=2))
    elif as_tree:
        # Tree format with size+date prefix, or quiet for structure only
        tree_lines = _build_tree(items, human_readable=human_readable, quiet=quiet)
        for line in tree_lines:
            print(line)
    elif quiet:
        for item in items:
            if isinstance(item, BucketFolder):
                print(f"{item.path}/")
            else:
                print(item.path)
    else:
        # Flat table format
        for item in items:
            if isinstance(item, BucketFolder):
                mtime_str = _format_mtime(item.uploaded_at, human_readable)
                print(f"{'':>12}  {mtime_str:>19}  {item.path}/")
            else:
                size_str = _format_size(item.size, human_readable)
                mtime_str = _format_mtime(item.mtime, human_readable)
                print(f"{size_str:>12}  {mtime_str:>19}  {item.path}")

    if not recursive and has_directories:
        StatusLine().done("Use -R to list files recursively.")


@buckets_cli.command(
    name="info",
    examples=[
        "hf buckets info user/my-bucket",
        "hf buckets info hf://buckets/user/my-bucket",
    ],
)
def info(
    bucket_id: Annotated[
        str,
        typer.Argument(
            help="Bucket ID: namespace/bucket_name or hf://buckets/namespace/bucket_name",
        ),
    ],
    quiet: QuietOpt = False,
    token: TokenOpt = None,
) -> None:
    """Get info about a bucket."""
    api = get_hf_api(token=token)

    try:
        parsed_id, _ = _parse_bucket_argument(bucket_id)
    except ValueError as e:
        raise typer.BadParameter(str(e))

    bucket = api.bucket_info(parsed_id)
    if quiet:
        print(bucket.id)
    else:
        print(json.dumps(api_object_to_dict(bucket), indent=2))


@buckets_cli.command(
    name="delete",
    examples=[
        "hf buckets delete user/my-bucket",
        "hf buckets delete hf://buckets/user/my-bucket",
        "hf buckets delete user/my-bucket --yes",
        "hf buckets delete user/my-bucket --missing-ok",
    ],
)
def delete(
    bucket_id: Annotated[
        str,
        typer.Argument(
            help="Bucket ID: namespace/bucket_name or hf://buckets/namespace/bucket_name",
        ),
    ],
    yes: Annotated[
        bool,
        typer.Option(
            "--yes",
            "-y",
            help="Skip confirmation prompt.",
        ),
    ] = False,
    missing_ok: Annotated[
        bool,
        typer.Option(
            "--missing-ok",
            help="Do not raise an error if the bucket does not exist.",
        ),
    ] = False,
    quiet: QuietOpt = False,
    token: TokenOpt = None,
) -> None:
    """Delete a bucket.

    This deletes the entire bucket and all its contents. Use `hf buckets rm` to remove individual files.
    """
    if bucket_id.startswith(BUCKET_PREFIX):
        try:
            parsed_id, prefix = _parse_bucket_argument(bucket_id)
        except ValueError as e:
            raise typer.BadParameter(str(e))
        if prefix:
            raise typer.BadParameter(
                f"Cannot specify a prefix for bucket deletion: {bucket_id}."
                f" Use namespace/bucket_name or {BUCKET_PREFIX}namespace/bucket_name."
            )
        bucket_id = parsed_id
    elif "/" not in bucket_id:
        raise typer.BadParameter(
            f"Invalid bucket ID: {bucket_id}."
            f" Must be in format namespace/bucket_name or {BUCKET_PREFIX}namespace/bucket_name."
        )

    if not yes:
        confirm = typer.confirm(f"Are you sure you want to delete bucket '{bucket_id}'?")
        if not confirm:
            print("Aborted.")
            raise typer.Abort()

    api = get_hf_api(token=token)
    api.delete_bucket(bucket_id, missing_ok=missing_ok)
    if quiet:
        print(bucket_id)
    else:
        print(f"Bucket deleted: {bucket_id}")


@buckets_cli.command(
    name="remove | rm",
    examples=[
        "hf buckets remove user/my-bucket/file.txt",
        "hf buckets rm hf://buckets/user/my-bucket/file.txt",
        "hf buckets rm user/my-bucket/logs/ --recursive",
        'hf buckets rm user/my-bucket --recursive --include "*.tmp"',
        "hf buckets rm user/my-bucket/data/ --recursive --dry-run",
    ],
)
def remove(
    argument: Annotated[
        str,
        typer.Argument(
            help=(
                "Bucket path: namespace/bucket_name/path or hf://buckets/namespace/bucket_name/path."
                " With --recursive, namespace/bucket_name is also accepted to target all files."
            ),
        ),
    ],
    recursive: Annotated[
        bool,
        typer.Option(
            "--recursive",
            "-R",
            help="Remove files recursively under the given prefix.",
        ),
    ] = False,
    yes: Annotated[
        bool,
        typer.Option(
            "--yes",
            "-y",
            help="Skip confirmation prompt.",
        ),
    ] = False,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help="Preview what would be deleted without actually deleting.",
        ),
    ] = False,
    include: Annotated[
        Optional[list[str]],
        typer.Option(
            help="Include only files matching pattern (can specify multiple). Requires --recursive.",
        ),
    ] = None,
    exclude: Annotated[
        Optional[list[str]],
        typer.Option(
            help="Exclude files matching pattern (can specify multiple). Requires --recursive.",
        ),
    ] = None,
    quiet: QuietOpt = False,
    token: TokenOpt = None,
) -> None:
    """Remove files from a bucket.

    To delete an entire bucket, use `hf buckets delete` instead.
    """
    try:
        bucket_id, prefix = _parse_bucket_argument(argument)
    except ValueError as e:
        raise typer.BadParameter(str(e))

    if prefix == "" and not recursive:
        raise typer.BadParameter(
            f"No file path specified. To remove files, provide a path"
            f" (e.g. '{bucket_id}/FILE') or use --recursive to remove all files."
            f" To delete the entire bucket, use `hf buckets delete {bucket_id}`."
        )

    if (include or exclude) and not recursive:
        raise typer.BadParameter("--include and --exclude require --recursive.")

    api = get_hf_api(token=token)

    if recursive:
        status = StatusLine(enabled=not quiet)
        status.update("Listing files from remote")

        all_files: list[BucketFile] = []
        for item in api.list_bucket_tree(
            bucket_id,
            prefix=prefix.rstrip("/") or None,
            recursive=True,
        ):
            if isinstance(item, BucketFile):
                all_files.append(item)
                status.update(f"Listing files from remote ({len(all_files)} files)")
        status.done(f"Listing files from remote ({len(all_files)} files)")

        if include or exclude:
            matcher = FilterMatcher(include_patterns=include, exclude_patterns=exclude)
            matched_files = [f for f in all_files if matcher.matches(f.path)]
        else:
            matched_files = all_files

        file_paths = [f.path for f in matched_files]
        total_size = sum(f.size for f in matched_files)
        size_str = _format_size(total_size, human_readable=True)

        if not file_paths:
            if not quiet:
                print("No files to remove.")
            return

        count_label = f"{len(file_paths)} file(s) totaling {size_str}"

        if not yes and not dry_run:
            if not quiet:
                for path in file_paths:
                    print(f"  {path}")
            confirm = typer.confirm(f"Remove {count_label} from '{bucket_id}'?")
            if not confirm:
                print("Aborted.")
                raise typer.Abort()

        if dry_run:
            for path in file_paths:
                print(f"delete: {BUCKET_PREFIX}{bucket_id}/{path}")
            print(f"(dry run) {count_label} would be removed.")
            return

        api.batch_bucket_files(bucket_id, delete=file_paths)
        if quiet:
            for path in file_paths:
                print(path)
        else:
            for path in file_paths:
                print(f"delete: {BUCKET_PREFIX}{bucket_id}/{path}")
            print(f"Removed {count_label} from '{bucket_id}'.")

    else:
        file_path = prefix.rstrip("/")
        if not file_path:
            raise typer.BadParameter("File path cannot be empty.")

        if dry_run:
            print(f"delete: {BUCKET_PREFIX}{bucket_id}/{file_path}")
            print("(dry run) 1 file would be removed.")
            return

        if not yes:
            confirm = typer.confirm(f"Remove '{file_path}' from '{bucket_id}'?")
            if not confirm:
                print("Aborted.")
                raise typer.Abort()

        api.batch_bucket_files(bucket_id, delete=[file_path])
        if quiet:
            print(file_path)
        else:
            print(f"delete: {BUCKET_PREFIX}{bucket_id}/{file_path}")


@buckets_cli.command(
    name="move",
    examples=[
        "hf buckets move user/old-bucket user/new-bucket",
        "hf buckets move user/my-bucket my-org/my-bucket",
        "hf buckets move hf://buckets/user/old-bucket hf://buckets/user/new-bucket",
    ],
)
def move(
    from_id: Annotated[
        str,
        typer.Argument(
            help="Source bucket ID: namespace/bucket_name or hf://buckets/namespace/bucket_name",
        ),
    ],
    to_id: Annotated[
        str,
        typer.Argument(
            help="Destination bucket ID: namespace/bucket_name or hf://buckets/namespace/bucket_name",
        ),
    ],
    token: TokenOpt = None,
) -> None:
    """Move (rename) a bucket to a new name or namespace."""
    # Parse from_id
    parsed_from_id, from_prefix = _parse_bucket_argument(from_id)
    if from_prefix:
        raise typer.BadParameter(
            f"Cannot specify a prefix for bucket move: {from_id}."
            f" Use namespace/bucket_name or {BUCKET_PREFIX}namespace/bucket_name."
        )

    # Parse to_id
    parsed_to_id, to_prefix = _parse_bucket_argument(to_id)
    if to_prefix:
        raise typer.BadParameter(
            f"Cannot specify a prefix for bucket move: {to_id}."
            f" Use namespace/bucket_name or {BUCKET_PREFIX}namespace/bucket_name."
        )

    api = get_hf_api(token=token)
    api.move_bucket(from_id=parsed_from_id, to_id=parsed_to_id)
    print(f"Bucket moved: {parsed_from_id} -> {parsed_to_id}")


# =============================================================================
# Sync command
# =============================================================================


@buckets_cli.command(
    name="sync",
    examples=[
        "hf buckets sync ./data hf://buckets/user/my-bucket",
        "hf buckets sync hf://buckets/user/my-bucket ./data",
        "hf buckets sync ./data hf://buckets/user/my-bucket --delete",
        'hf buckets sync hf://buckets/user/my-bucket ./data --include "*.safetensors" --exclude "*.tmp"',
        "hf buckets sync ./data hf://buckets/user/my-bucket --plan sync-plan.jsonl",
        "hf buckets sync --apply sync-plan.jsonl",
        "hf buckets sync ./data hf://buckets/user/my-bucket --dry-run",
        "hf buckets sync ./data hf://buckets/user/my-bucket --dry-run | jq .",
    ],
)
def sync(
    source: Annotated[
        Optional[str],
        typer.Argument(
            help="Source path: local directory or hf://buckets/namespace/bucket_name(/prefix)",
        ),
    ] = None,
    dest: Annotated[
        Optional[str],
        typer.Argument(
            help="Destination path: local directory or hf://buckets/namespace/bucket_name(/prefix)",
        ),
    ] = None,
    delete: Annotated[
        bool,
        typer.Option(
            help="Delete destination files not present in source.",
        ),
    ] = False,
    ignore_times: Annotated[
        bool,
        typer.Option(
            "--ignore-times",
            help="Skip files only based on size, ignoring modification times.",
        ),
    ] = False,
    ignore_sizes: Annotated[
        bool,
        typer.Option(
            "--ignore-sizes",
            help="Skip files only based on modification times, ignoring sizes.",
        ),
    ] = False,
    plan: Annotated[
        Optional[str],
        typer.Option(
            help="Save sync plan to JSONL file for review instead of executing.",
        ),
    ] = None,
    apply: Annotated[
        Optional[str],
        typer.Option(
            help="Apply a previously saved plan file.",
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help="Print sync plan to stdout as JSONL without executing.",
        ),
    ] = False,
    include: Annotated[
        Optional[list[str]],
        typer.Option(
            help="Include files matching pattern (can specify multiple).",
        ),
    ] = None,
    exclude: Annotated[
        Optional[list[str]],
        typer.Option(
            help="Exclude files matching pattern (can specify multiple).",
        ),
    ] = None,
    filter_from: Annotated[
        Optional[str],
        typer.Option(
            help="Read include/exclude patterns from file.",
        ),
    ] = None,
    existing: Annotated[
        bool,
        typer.Option(
            "--existing",
            help="Skip creating new files on receiver (only update existing files).",
        ),
    ] = False,
    ignore_existing: Annotated[
        bool,
        typer.Option(
            "--ignore-existing",
            help="Skip updating files that exist on receiver (only create new files).",
        ),
    ] = False,
    verbose: Annotated[
        bool,
        typer.Option(
            "--verbose",
            "-v",
            help="Show detailed logging with reasoning.",
        ),
    ] = False,
    quiet: Annotated[
        bool,
        typer.Option(
            "--quiet",
            "-q",
            help="Minimal output.",
        ),
    ] = False,
    token: TokenOpt = None,
) -> None:
    """Sync files between local directory and a bucket."""
    api = get_hf_api(token=token)
    api.sync_bucket(
        source=source,
        dest=dest,
        delete=delete,
        ignore_times=ignore_times,
        ignore_sizes=ignore_sizes,
        existing=existing,
        ignore_existing=ignore_existing,
        include=include,
        exclude=exclude,
        filter_from=filter_from,
        plan=plan,
        apply=apply,
        dry_run=dry_run,
        verbose=verbose,
        quiet=quiet,
    )


# =============================================================================
# Cp command
# =============================================================================


@buckets_cli.command(
    name="cp",
    examples=[
        "hf buckets cp hf://buckets/user/my-bucket/config.json",
        "hf buckets cp hf://buckets/user/my-bucket/config.json ./data/",
        "hf buckets cp hf://buckets/user/my-bucket/config.json my-config.json",
        "hf buckets cp hf://buckets/user/my-bucket/config.json -",
        "hf buckets cp my-config.json hf://buckets/user/my-bucket",
        "hf buckets cp my-config.json hf://buckets/user/my-bucket/logs/",
        "hf buckets cp my-config.json hf://buckets/user/my-bucket/remote-config.json",
        "hf buckets cp - hf://buckets/user/my-bucket/config.json",
    ],
)
def cp(
    src: Annotated[str, typer.Argument(help="Source: local file, hf://buckets/... path, or - for stdin")],
    dst: Annotated[
        Optional[str], typer.Argument(help="Destination: local path, hf://buckets/... path, or - for stdout")
    ] = None,
    quiet: QuietOpt = False,
    token: TokenOpt = None,
) -> None:
    """Copy a single file to or from a bucket."""
    api = get_hf_api(token=token)

    src_is_bucket = _is_bucket_path(src)
    dst_is_bucket = dst is not None and _is_bucket_path(dst)
    src_is_stdin = src == "-"
    dst_is_stdout = dst == "-"

    # --- Validation ---
    if src_is_bucket and dst_is_bucket:
        raise typer.BadParameter("Remote-to-remote copy not supported.")

    if not src_is_bucket and not dst_is_bucket and not src_is_stdin:
        if dst is None:
            raise typer.BadParameter("Missing destination. Provide a bucket path as DST.")
        raise typer.BadParameter("One of SRC or DST must be a bucket path (hf://buckets/...).")

    if src_is_stdin and not dst_is_bucket:
        raise typer.BadParameter("Stdin upload requires a bucket destination.")

    if src_is_stdin and dst_is_bucket:
        assert dst is not None
        _, prefix = _parse_bucket_path(dst)
        if prefix == "" or prefix.endswith("/"):
            raise typer.BadParameter("Stdin upload requires a full destination path including filename.")

    if dst_is_stdout and not src_is_bucket:
        raise typer.BadParameter("Cannot pipe to stdout for uploads.")

    if not src_is_bucket and not src_is_stdin and os.path.isdir(src):
        raise typer.BadParameter("Source must be a file, not a directory. Use `hf buckets sync` for directories.")

    # --- Determine direction and execute ---
    if src_is_bucket:
        # Download: remote -> local or stdout
        bucket_id, prefix = _parse_bucket_path(src)
        if prefix == "" or prefix.endswith("/"):
            raise typer.BadParameter("Source path must include a file name, not just a bucket or directory path.")
        filename = prefix.rsplit("/", 1)[-1]

        if dst_is_stdout:
            # Download to stdout: always suppress progress bars to avoid polluting output
            # Only re-enable if they weren't already disabled by the caller
            pbar_was_disabled = are_progress_bars_disabled()
            if not pbar_was_disabled:
                disable_progress_bars()
            try:
                with tempfile.TemporaryDirectory() as tmp_dir:
                    tmp_path = os.path.join(tmp_dir, filename)
                    api.download_bucket_files(bucket_id, [(prefix, tmp_path)])
                    with open(tmp_path, "rb") as f:
                        sys.stdout.buffer.write(f.read())
            finally:
                if not pbar_was_disabled:
                    enable_progress_bars()
        else:
            # Download to file
            if dst is None:
                local_path = filename
            elif os.path.isdir(dst) or dst.endswith(os.sep) or dst.endswith("/"):
                local_path = os.path.join(dst, filename)
            else:
                local_path = dst

            # Ensure parent directory exists
            parent_dir = os.path.dirname(local_path)
            if parent_dir:
                os.makedirs(parent_dir, exist_ok=True)

            if quiet:
                disable_progress_bars()
            try:
                api.download_bucket_files(bucket_id, [(prefix, local_path)])
            finally:
                if quiet:
                    enable_progress_bars()

            if not quiet:
                print(f"Downloaded: {src} -> {local_path}")

    elif src_is_stdin:
        # Upload from stdin
        bucket_id, remote_path = _parse_bucket_path(dst)  # type: ignore[arg-type]
        data = sys.stdin.buffer.read()

        if quiet:
            disable_progress_bars()
        try:
            api.batch_bucket_files(bucket_id, add=[(data, remote_path)])
        finally:
            if quiet:
                enable_progress_bars()

        if not quiet:
            print(f"Uploaded: stdin -> {dst}")

    else:
        # Upload from file
        if not os.path.isfile(src):
            raise typer.BadParameter(f"Source file not found: {src}")

        bucket_id, prefix = _parse_bucket_path(dst)  # type: ignore[arg-type]

        if prefix == "":
            remote_path = os.path.basename(src)
        elif prefix.endswith("/"):
            remote_path = prefix + os.path.basename(src)
        else:
            remote_path = prefix

        if quiet:
            disable_progress_bars()
        try:
            api.batch_bucket_files(bucket_id, add=[(src, remote_path)])
        finally:
            if quiet:
                enable_progress_bars()

        if not quiet:
            print(f"Uploaded: {src} -> {BUCKET_PREFIX}{bucket_id}/{remote_path}")
