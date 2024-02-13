import json
import re
from functools import cmp_to_key

from .semver import Version, VersionRange


class ErmJsonLite(object):
    """
    Basic implementation to read `erm-packages.json`.

    It doesn't use any models, works with only raw JSON types: lists, dicts, strings
    """

    class ResourceType(object):
        NPM_PACKAGE = "NPM_PACKAGE"
        NODE_JS = "NODE_JS"

    data = None

    @staticmethod
    def get_versions_of(er_resource):
        # type: (dict) -> list[Version]
        """
        Return all versions of the resource in ASC order (from older to latest)
        """
        unsorted = er_resource.get("versions").keys()
        # We have to sort because in python 2 the order of keys in a dict is not guaranteed
        versions = sorted(unsorted, key=cmp_to_key(Version.cmp))

        return [Version.from_str(v) for v in versions]

    @classmethod
    def load(cls, path):
        # type: (str) -> ErmJsonLite
        erm_json = cls()

        with open(path, encoding='utf-8') as f:
            erm_json.data = dict()
            for k, v in json.load(f).items():
                # Ignore comments (when key starts with `_`), used for banner
                if not k.startswith("_"):
                    erm_json.data[k] = v

        return erm_json

    @staticmethod
    def canonize_name(resource_name):
        # type: (str) -> str
        """
        Canonize resource name

        For example:
            hermione -> hermione
            super-package -> super_package
            @yatool/nots -> yatool_nots
        """
        return re.sub(r"\W+", "_", resource_name).strip("_")

    def get_resource(self, resource_name):
        # type: (str) -> dict
        """
        Return resource by his name
        """
        er_resource = self.data.get(resource_name)
        if not er_resource:
            raise Exception("Requested resource {} is not a toolchain item".format(resource_name))

        return er_resource

    def get_sb_resources(self, resource_name, version):
        # type: (str, Version) -> list[dict]
        """
        Return a list of SB resources for ER version
        """
        er_resource = self.get_resource(resource_name)

        return er_resource.get("versions").get(str(version)).get("resources")

    def is_resource_multiplatform(self, resource_name):
        # type: (str) -> bool
        """
        Return True if resource is multiplatform, False otherwise
        """
        er_resource = self.get_resource(resource_name)

        return er_resource.get("multiplatform", False)

    def list_npm_packages(self):
        # type: () -> list[str]
        """
        Returns a list of the names of the npm tools used in the toolchain
        """
        result = []
        for resource_name, resource in self.data.items():
            if resource.get("type") == self.ResourceType.NPM_PACKAGE:
                result.append(resource_name)

        return result

    def select_version_of(self, resource_name, range_str=None):
        # type: (str, str|None) -> Version|None
        er_resource = self.get_resource(resource_name)

        if range_str is None:
            return Version.from_str(er_resource.get("default"))

        version_range = VersionRange.from_str(range_str)

        # assuming the version list is sorted from the lowest to the highest version,
        # we stop the loop as early as possible and hence return the lowest compatible version
        for version in self.get_versions_of(er_resource):
            if version_range.is_satisfied_by(version):
                return version

        return None

    def use_resource_directly(self, resource_name):
        # type: (str) -> bool
        er_resource = self.get_resource(resource_name)

        return er_resource.get("useDirectly", False)
