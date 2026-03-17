"""pydantic BaseModel for GeoJSON objects."""

from __future__ import annotations

import warnings
from typing import Any, Dict, List, Optional, Set

from pydantic import BaseModel, SerializationInfo, field_validator, model_serializer

from geojson_pydantic.types import BBox


class _GeoJsonBase(BaseModel):
    bbox: Optional[BBox] = None

    # These fields will not be included when serializing in json mode
    # `.model_dump_json()` or `.model_dump(mode="json")`
    __geojson_exclude_if_none__: Set[str] = {"bbox"}

    @property
    def __geo_interface__(self) -> Dict[str, Any]:
        """GeoJSON-like protocol for geo-spatial (GIS) vector data.

        ref: https://gist.github.com/sgillies/2217756#__geo_interface
        """
        return self.model_dump(mode="json")

    @field_validator("bbox")
    def validate_bbox(cls, bbox: Optional[BBox]) -> Optional[BBox]:
        """Validate BBox values are ordered correctly."""
        # If bbox is None, there is nothing to validate.
        if bbox is None:
            return None

        # A list to store any errors found so we can raise them all at once.
        errors: List[str] = []

        # Determine where the second position starts. 2 for 2D, 3 for 3D.
        offset = len(bbox) // 2

        # Check X
        if bbox[0] > bbox[offset]:
            warnings.warn(
                f"BBOX crossing the Antimeridian line, Min X ({bbox[0]}) > Max X ({bbox[offset]}).",
                UserWarning,
                stacklevel=1,
            )

        # Check Y
        if bbox[1] > bbox[1 + offset]:
            errors.append(f"Min Y ({bbox[1]}) must be <= Max Y ({bbox[1 + offset]}).")

        # If 3D, check Z values.
        if offset > 2 and bbox[2] > bbox[2 + offset]:
            errors.append(f"Min Z ({bbox[2]}) must be <= Max Z ({bbox[2 + offset]}).")

        # Raise any errors found.
        if errors:
            raise ValueError("Invalid BBox. Error(s): " + " ".join(errors))

        return bbox

    # This return is untyped due to a workaround until this issue is resolved:
    # https://github.com/tiangolo/fastapi/discussions/10661
    @model_serializer(when_used="always", mode="wrap")
    def clean_model(self, serializer: Any, info: SerializationInfo):  # type: ignore [no-untyped-def]
        """Custom Model serializer to match the GeoJSON specification.

        Used to remove fields which are optional but cannot be null values.
        """
        # This seems like the best way to have the least amount of unexpected consequences.
        # We want to avoid forcing values in `exclude_none` or `exclude_unset` which could
        # cause issues or unexpected behavior for downstream users.
        # ref: https://github.com/pydantic/pydantic/issues/6575
        data: Dict[str, Any] = serializer(self)

        # Only remove fields when in JSON mode.
        if info.mode_is_json():
            for field in self.__geojson_exclude_if_none__:
                if field in data and data[field] is None:
                    del data[field]

        return data
