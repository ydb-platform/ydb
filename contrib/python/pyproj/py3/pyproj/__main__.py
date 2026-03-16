"""
This is the main entry point for pyproj CLI

e.g. python -m pyproj

"""

import argparse
import os

from pyproj import __proj_version__, __version__, _show_versions
from pyproj.aoi import BBox
from pyproj.datadir import get_data_dir, get_user_data_dir
from pyproj.sync import (
    _download_resource_file,
    get_proj_endpoint,
    get_transform_grid_list,
)

parser = argparse.ArgumentParser(
    description=f"pyproj version: {__version__} [PROJ version: {__proj_version__}]"
)
parser.add_argument(
    "-v",
    "--verbose",
    help="Show verbose debugging version information.",
    action="store_true",
)
subparsers = parser.add_subparsers(title="commands")
sync_parser = subparsers.add_parser(
    name="sync",
    description="Tool for synchronizing PROJ datum and transformation support data.",
)
sync_parser.add_argument(
    "--bbox",
    help=(
        "Specify an area of interest to restrict the resources to download. "
        "The area of interest is specified as a "
        "bounding box with geographic coordinates, expressed in degrees in an "
        "unspecified geographic CRS. "
        "`west_long` and `east_long` should be in the [-180,180] range, and "
        "`south_lat` and `north_lat` in the [-90,90]. `west_long` is generally "
        "lower than `east_long`, except in the case where the area of interest "
        "crosses the antimeridian."
    ),
)
sync_parser.add_argument(
    "--spatial-test",
    help=(
        "Specify how the extent of the resource files "
        "are compared to the area of use specified explicitly with `--bbox`. "
        "By default, any resource files whose extent intersects the value specified "
        "by `--bbox` will be selected. If using the ``contains`` strategy, "
        "only resource files whose extent is contained in the value specified by "
        "`--bbox` will be selected."
    ),
    choices=["intersects", "contains"],
    default="intersects",
)
sync_parser.add_argument(
    "--source-id",
    help=(
        "Restrict resource files to be downloaded to those whose source_id property "
        "contains the ID value. Default is all possible values."
    ),
)
sync_parser.add_argument(
    "--area-of-use",
    help=(
        "Restrict resource files to be downloaded to those whose area_of_use property "
        "contains the AREA_OF_USE value. Default is all possible values."
    ),
)
sync_parser.add_argument(
    "--file",
    help=(
        "Restrict resource files to be downloaded to those whose name property "
        " (file name) contains the FILE value. Default is all possible values."
    ),
)
sync_parser.add_argument(
    "--exclude-world-coverage",
    help="Exclude files which have world coverage.",
    action="store_true",
)
sync_parser.add_argument(
    "--include-already-downloaded",
    help="Include grids that are already downloaded.",
    action="store_true",
)
sync_parser.add_argument(
    "--list-files", help="List the files without downloading.", action="store_true"
)
sync_parser.add_argument(
    "--all", help="Download all missing transform grids.", action="store_true"
)
sync_parser.add_argument(
    "--system-directory",
    help=(
        "If enabled, it will sync grids to the main PROJ data directory "
        "instead of the user writable directory."
    ),
    action="store_true",
)
sync_parser.add_argument(
    "--target-directory",
    help="The directory to sync grids to instead of the user writable directory.",
)
sync_parser.add_argument(
    "-v", "--verbose", help="Print download information.", action="store_true"
)


def _parse_sync_command(args):
    """
    Handle sync command arguments
    """
    if not any(
        (
            args.bbox,
            args.list_files,
            args.all,
            args.source_id,
            args.area_of_use,
            args.file,
        )
    ):
        sync_parser.print_help()
        return

    if args.all and any(
        (
            args.bbox,
            args.list_files,
            args.source_id,
            args.area_of_use,
            args.file,
        )
    ):
        raise RuntimeError(
            "Cannot use '--all' with '--list-files', '--source-id',"
            "'--area-of-use', '--bbox', or '--file'."
        )

    bbox = None
    if args.bbox is not None:
        west, south, east, north = args.bbox.split(",")
        bbox = BBox(
            west=float(west),
            south=float(south),
            east=float(east),
            north=float(north),
        )
    if args.target_directory and args.system_directory:
        raise RuntimeError("Cannot set both --target-directory and --system-directory.")
    target_directory = args.target_directory
    if args.system_directory:
        target_directory = get_data_dir().split(os.path.sep)[0]
    elif not target_directory:
        target_directory = get_user_data_dir(True)
    grids = get_transform_grid_list(
        source_id=args.source_id,
        area_of_use=args.area_of_use,
        filename=args.file,
        bbox=bbox,
        spatial_test=args.spatial_test,
        include_world_coverage=not args.exclude_world_coverage,
        include_already_downloaded=args.include_already_downloaded,
        target_directory=target_directory,
    )
    if args.list_files:
        print("filename | source_id | area_of_use")
        print("----------------------------------")
    else:
        endpoint = get_proj_endpoint()
    for grid in grids:
        if args.list_files:
            print(
                grid["properties"]["name"],
                grid["properties"]["source_id"],
                grid["properties"].get("area_of_use"),
                sep=" | ",
            )
        else:
            filename = grid["properties"]["name"]
            _download_resource_file(
                file_url=f"{endpoint}/{filename}",
                short_name=filename,
                directory=target_directory,
                verbose=args.verbose,
                sha256=grid["properties"]["sha256sum"],
            )


def main():
    """
    Main entrypoint into the command line interface.
    """
    args = parser.parse_args()
    if hasattr(args, "bbox"):
        _parse_sync_command(args)
    elif args.verbose:
        _show_versions.show_versions()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
