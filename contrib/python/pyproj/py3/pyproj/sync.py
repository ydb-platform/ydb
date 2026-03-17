"""
Based on the logic in the PROJ projsync CLI program

https://github.com/OSGeo/PROJ/blob/9ff543c4ffd86152bc58d0a0164b2ce9ebbb8bec/src/apps/projsync.cpp
"""

import hashlib
import json
import os
from datetime import datetime
from functools import partial
from pathlib import Path
from typing import Any
from urllib.request import urlretrieve

from pyproj._sync import get_proj_endpoint
from pyproj.aoi import BBox
from pyproj.datadir import get_data_dir, get_user_data_dir


def _bbox_from_coords(coords: list) -> BBox | None:
    """
    Get the bounding box from coordinates
    """
    try:
        xxx, yyy = zip(*coords)
        return BBox(west=min(xxx), south=min(yyy), east=max(xxx), north=max(yyy))
    except ValueError:
        pass
    coord_bbox = None
    for coord_set in coords:
        bbox = _bbox_from_coords(coord_set)
        if bbox is None:
            continue
        if coord_bbox is None:
            coord_bbox = bbox
        else:
            coord_bbox.west = min(coord_bbox.west, bbox.west)
            coord_bbox.south = min(coord_bbox.south, bbox.south)
            coord_bbox.north = max(coord_bbox.north, bbox.north)
            coord_bbox.east = max(coord_bbox.east, bbox.east)
    return coord_bbox


def _bbox_from_geom(geom: dict[str, Any]) -> BBox | None:
    """
    Get the bounding box from geojson geometry
    """
    if "coordinates" not in geom or "type" not in geom:
        return None
    coordinates = geom["coordinates"]
    if geom["type"] != "MultiPolygon":
        return _bbox_from_coords(coordinates)
    found_minus_180 = False
    found_plus_180 = False
    bboxes = []
    for coordinate_set in coordinates:
        bbox = _bbox_from_coords(coordinate_set)
        if bbox is None:
            continue
        if bbox.west == -180:
            found_minus_180 = True
        elif bbox.east == 180:
            found_plus_180 = True
        bboxes.append(bbox)
    grid_bbox = None
    for bbox in bboxes:
        if found_minus_180 and found_plus_180 and bbox.west == -180:
            bbox.west = 180
            bbox.east += 360
        if grid_bbox is None:
            grid_bbox = bbox
        else:
            grid_bbox.west = min(grid_bbox.west, bbox.west)
            grid_bbox.south = min(grid_bbox.south, bbox.south)
            grid_bbox.north = max(grid_bbox.north, bbox.north)
            grid_bbox.east = max(grid_bbox.east, bbox.east)
    return grid_bbox


def _filter_bbox(
    feature: dict[str, Any], bbox: BBox, spatial_test: str, include_world_coverage: bool
) -> bool:
    """
    Filter by the bounding box. Designed to use with 'filter'
    """
    geom = feature.get("geometry")
    if geom is not None:
        geom_bbox = _bbox_from_geom(geom)
        if geom_bbox is None:
            return False
        if (
            geom_bbox.east - geom_bbox.west > 359
            and geom_bbox.north - geom_bbox.south > 179
        ):
            if not include_world_coverage:
                return False
            geom_bbox.west = -float("inf")
            geom_bbox.east = float("inf")
        elif geom_bbox.east > 180 and bbox.west < -180:
            geom_bbox.west -= 360
            geom_bbox.east -= 360
        return getattr(bbox, spatial_test)(geom_bbox)
    return False


def _filter_properties(
    feature: dict[str, Any],
    source_id: str | None = None,
    area_of_use: str | None = None,
    filename: str | None = None,
) -> bool:
    """
    Filter by the properties. Designed to use with 'filter'
    """
    properties = feature.get("properties")
    if not properties:
        return False
    p_filename = properties.get("name")
    p_source_id = properties.get("source_id")
    if not p_filename or not p_source_id:
        return False
    source_id__matched = source_id is None or source_id in p_source_id
    area_of_use__matched = area_of_use is None or area_of_use in properties.get(
        "area_of_use", ""
    )
    filename__matched = filename is None or filename in p_filename
    if source_id__matched and area_of_use__matched and filename__matched:
        return True
    return False


def _is_download_needed(grid_name: str) -> bool:
    """
    Run through all of the PROJ directories to see if the
    file already exists.
    """
    if Path(get_user_data_dir(), grid_name).exists():
        return False
    for data_dir in get_data_dir().split(os.pathsep):
        if Path(data_dir, grid_name).exists():
            return False
    return True


def _filter_download_needed(feature: dict[str, Any]) -> bool:
    """
    Filter grids so only those that need to be downloaded are included.
    """
    properties = feature.get("properties")
    if not properties:
        return False
    filename = properties.get("name")
    if not filename:
        return False
    return _is_download_needed(filename)


def _sha256sum(input_file):
    """
    Return sha256 checksum of file given by path.
    """

    hasher = hashlib.sha256()
    with open(input_file, "rb") as file:
        for chunk in iter(lambda: file.read(65536), b""):
            hasher.update(chunk)

    return hasher.hexdigest()


def _download_resource_file(
    file_url, short_name, directory, verbose=False, sha256=None
):
    """
    Download resource file from PROJ url
    """
    if verbose:
        print(f"Downloading: {file_url}")
    tmp_path = Path(directory, f"{short_name}.part")
    try:
        urlretrieve(file_url, tmp_path)
        if sha256 is not None and sha256 != _sha256sum(tmp_path):
            raise RuntimeError(f"SHA256 mismatch: {short_name}")
        tmp_path.replace(Path(directory, short_name))
    finally:
        try:
            os.remove(tmp_path)
        except FileNotFoundError:
            pass


def _load_grid_geojson(target_directory: str | Path | None = None) -> dict[str, Any]:
    """
    Returns
    -------
    dict[str, Any]:
        The PROJ grid data list.
    """
    if target_directory is None:
        target_directory = get_user_data_dir(True)
    local_path = Path(target_directory, "files.geojson")
    if not local_path.exists() or (
        (datetime.now() - datetime.fromtimestamp(local_path.stat().st_mtime)).days > 0
    ):
        _download_resource_file(
            file_url=f"{get_proj_endpoint()}/files.geojson",
            short_name="files.geojson",
            directory=target_directory,
        )
    return json.loads(local_path.read_text(encoding="utf-8"))


def get_transform_grid_list(
    source_id: str | None = None,
    area_of_use: str | None = None,
    filename: str | None = None,
    bbox: BBox | None = None,
    spatial_test: str = "intersects",
    include_world_coverage: bool = True,
    include_already_downloaded: bool = False,
    target_directory: str | Path | None = None,
) -> tuple:
    """
    Get a list of transform grids that can be downloaded.

    Parameters
    ----------
    source_id: str, optional
    area_of_use: str, optional
    filename: str, optional
    bbox: BBox, optional
    spatial_test: str, default="intersects"
        Can be "contains" or "intersects".
    include_world_coverage: bool, default=True
        If True, it will include grids with a global extent.
    include_already_downloaded: bool, default=False
        If True, it will list grids regardless of if they are downloaded.
    target_directory: str | Path, optional
        The directory to download the geojson file to.
        Default is the user writable directory.

    Returns
    -------
    list[dict[str, Any]]:
        A list of geojson data of containing information about features
        that can be downloaded.
    """
    features = _load_grid_geojson(target_directory=target_directory)["features"]
    if bbox is not None:
        if bbox.west > 180 and bbox.east > bbox.west:
            bbox.west -= 360
            bbox.east -= 360
        elif bbox.west < -180 and bbox.east > bbox.west:
            bbox.west += 360
            bbox.east += 360
        elif abs(bbox.west) < 180 and abs(bbox.east) < 180 and bbox.east < bbox.west:
            bbox.east += 360
        features = filter(
            partial(
                _filter_bbox,
                bbox=bbox,
                spatial_test=spatial_test,
                include_world_coverage=include_world_coverage,
            ),
            features,
        )
    # filter by properties
    features = filter(
        partial(
            _filter_properties,
            source_id=source_id,
            area_of_use=area_of_use,
            filename=filename,
        ),
        features,
    )
    if include_already_downloaded:
        return tuple(features)
    return tuple(filter(_filter_download_needed, features))
