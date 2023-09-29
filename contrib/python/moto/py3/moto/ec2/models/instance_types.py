import pathlib

from os import listdir

from moto.utilities.utils import load_resource
from ..exceptions import InvalidInstanceTypeError

import library.python.resource as _ya_res
import os
import json

INSTANCE_TYPES = load_resource("moto.ec2", "resources/instance_types.json")
INSTANCE_FAMILIES = list(set([i.split(".")[0] for i in INSTANCE_TYPES.keys()]))

root = pathlib.Path(__file__).parent.parent
offerings_path = "resources/instance_type_offerings"
INSTANCE_TYPE_OFFERINGS = {}
for entry in _ya_res.resfs_files(prefix=str(root / offerings_path)):
    rel_path = os.path.relpath(entry, root / offerings_path)
    path_parts = os.path.normpath(rel_path).split(os.path.sep)
    if len(path_parts) != 2:
        continue
    location_type, _region = path_parts
    if location_type not in INSTANCE_TYPE_OFFERINGS:
        INSTANCE_TYPE_OFFERINGS[location_type] = {}
    res = json.loads(_ya_res.find(f"resfs/file/{entry}"))
    for instance in res:
        instance["LocationType"] = location_type
        INSTANCE_TYPE_OFFERINGS[location_type][_region.replace(".json", "")] = res


class InstanceTypeBackend:
    def describe_instance_types(self, instance_types=None):
        matches = INSTANCE_TYPES.values()
        if instance_types:
            matches = [t for t in matches if t.get("InstanceType") in instance_types]
            if len(instance_types) > len(matches):
                unknown_ids = set(instance_types) - set(
                    t.get("InstanceType") for t in matches
                )
                raise InvalidInstanceTypeError(unknown_ids)
        return matches


class InstanceTypeOfferingBackend:
    def describe_instance_type_offerings(self, location_type=None, filters=None):
        location_type = location_type or "region"
        matches = INSTANCE_TYPE_OFFERINGS[location_type]
        matches = matches.get(self.region_name, [])
        matches = [
            o for o in matches if self.matches_filters(o, filters or {}, location_type)
        ]
        return matches

    def matches_filters(self, offering, filters, location_type):
        def matches_filter(key, values):
            if key == "location":
                if location_type in ("availability-zone", "availability-zone-id"):
                    return offering.get("Location") in values
                elif location_type == "region":
                    return any(
                        v for v in values if offering.get("Location").startswith(v)
                    )
                else:
                    return False
            elif key == "instance-type":
                return offering.get("InstanceType") in values
            else:
                return False

        return all([matches_filter(key, values) for key, values in filters.items()])
