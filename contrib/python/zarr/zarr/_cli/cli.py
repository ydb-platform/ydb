import logging
from enum import Enum
from typing import Annotated, Literal, cast

import typer

import zarr
import zarr.metadata.migrate_v3 as migrate_metadata
from zarr.core.sync import sync
from zarr.storage._common import make_store

app = typer.Typer()

logger = logging.getLogger(__name__)


def _set_logging_level(*, verbose: bool) -> None:
    if verbose:
        lvl = "INFO"
    else:
        lvl = "WARNING"
    zarr.set_log_level(cast(Literal["INFO", "WARNING"], lvl))
    zarr.set_format("%(message)s")


class ZarrFormat(str, Enum):
    v2 = "v2"
    v3 = "v3"


class ZarrFormatV3(str, Enum):
    """Limit CLI choice to only v3"""

    v3 = "v3"


@app.command()  # type: ignore[misc]
def migrate(
    zarr_format: Annotated[
        ZarrFormatV3,
        typer.Argument(
            help="Zarr format to migrate to. Currently only 'v3' is supported.",
        ),
    ],
    input_store: Annotated[
        str,
        typer.Argument(
            help=(
                "Input Zarr to migrate - should be a store, path to directory in file system or name of zip file "
                "e.g. 'data/example-1.zarr', 's3://example-bucket/example'..."
            )
        ),
    ],
    output_store: Annotated[
        str | None,
        typer.Argument(
            help=(
                "Output location to write generated metadata (no array data will be copied). If not provided, "
                "metadata will be written to input_store. Should be a store, path to directory in file system "
                "or name of zip file e.g. 'data/example-1.zarr', 's3://example-bucket/example'..."
            )
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option(
            help="Enable a dry-run: files that would be converted are logged, but no new files are created or changed."
        ),
    ] = False,
    overwrite: Annotated[
        bool,
        typer.Option(
            help="Remove any existing v3 metadata at the output location, before migration starts."
        ),
    ] = False,
    force: Annotated[
        bool,
        typer.Option(
            help=(
                "Only used when --overwrite is given. Allows v3 metadata to be removed when no valid "
                "v2 metadata exists at the output location."
            )
        ),
    ] = False,
    remove_v2_metadata: Annotated[
        bool,
        typer.Option(
            help="Remove v2 metadata (if any) from the output location, after migration is complete."
        ),
    ] = False,
) -> None:
    """Migrate all v2 metadata in a zarr hierarchy to v3. This will create a zarr.json file for each level
    (every group / array). v2 files (.zarray, .zattrs etc.) will be left as-is.
    """
    if dry_run:
        _set_logging_level(verbose=True)
        logger.info(
            "Dry run enabled - no new files will be created or changed. Log of files that would be created on a real run:"
        )

    input_zarr_store = sync(make_store(input_store, mode="r+"))

    if output_store is not None:
        output_zarr_store = sync(make_store(output_store, mode="w-"))
        write_store = output_zarr_store
    else:
        output_zarr_store = None
        write_store = input_zarr_store

    if overwrite:
        sync(migrate_metadata.remove_metadata(write_store, 3, force=force, dry_run=dry_run))

    migrate_metadata.migrate_v2_to_v3(
        input_store=input_zarr_store, output_store=output_zarr_store, dry_run=dry_run
    )

    if remove_v2_metadata:
        # There should always be valid v3 metadata at the output location after migration, so force=False
        sync(migrate_metadata.remove_metadata(write_store, 2, force=False, dry_run=dry_run))


@app.command()  # type: ignore[misc]
def remove_metadata(
    zarr_format: Annotated[
        ZarrFormat,
        typer.Argument(help="Which format's metadata to remove - v2 or v3."),
    ],
    store: Annotated[
        str,
        typer.Argument(
            help="Store or path to directory in file system or name of zip file e.g. 'data/example-1.zarr', 's3://example-bucket/example'..."
        ),
    ],
    force: Annotated[
        bool,
        typer.Option(
            help=(
                "Allow metadata to be deleted when no valid alternative exists e.g. allow deletion of v2 metadata, "
                "when no v3 metadata is present."
            )
        ),
    ] = False,
    dry_run: Annotated[
        bool,
        typer.Option(
            help="Enable a dry-run: files that would be deleted are logged, but no files are removed or changed."
        ),
    ] = False,
) -> None:
    """Remove all v2 (.zarray, .zattrs, .zgroup, .zmetadata) or v3 (zarr.json) metadata files from the given Zarr.
    Note - this will remove metadata files at all levels of the hierarchy (every group and array).
    """
    if dry_run:
        _set_logging_level(verbose=True)
        logger.info(
            "Dry run enabled - no files will be deleted or changed. Log of files that would be deleted on a real run:"
        )
    input_zarr_store = sync(make_store(store, mode="r+"))

    sync(
        migrate_metadata.remove_metadata(
            store=input_zarr_store,
            zarr_format=cast(Literal[2, 3], int(zarr_format[1:])),
            force=force,
            dry_run=dry_run,
        )
    )


@app.callback()  # type: ignore[misc]
def main(
    verbose: Annotated[
        bool,
        typer.Option(
            help="enable verbose logging - will print info about metadata files being deleted / saved."
        ),
    ] = False,
) -> None:
    """
    See available commands below - access help for individual commands with zarr COMMAND --help.
    """
    _set_logging_level(verbose=verbose)


if __name__ == "__main__":
    app()
