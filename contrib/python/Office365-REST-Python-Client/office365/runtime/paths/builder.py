import json
import re
from typing import TYPE_CHECKING

from office365.runtime.client_value import ClientValue
from office365.runtime.compat import is_string_type
from office365.runtime.paths.resource_path import ResourcePath

if TYPE_CHECKING:
    from office365.runtime.paths.service_operation import ServiceOperationPath


class ODataPathBuilder(object):
    @staticmethod
    def parse_url(path_str):
        # type: (str) -> ResourcePath
        """Parses path from a string"""
        segments = [n for n in re.split(r"[('')]|/", path_str) if n]
        if not segments:
            raise TypeError("Invalid path")
        path = None
        for segment in segments:
            path = ResourcePath(segment, path)
        return path

    @staticmethod
    def build_segment(path):
        # type: (ServiceOperationPath) -> str
        """Constructs url for path segment"""
        url = path.name or ""
        if isinstance(path.parameters, ClientValue):
            url += "(@v)?@v={0}".format(json.dumps(path.parameters.to_json()))
        elif path.parameters is not None:
            url += "("
            if isinstance(path.parameters, dict):
                url += ",".join(
                    [
                        "%s=%s" % (key, ODataPathBuilder._encode_method_value(value))
                        for (key, value) in path.parameters.items()
                        if value is not None
                    ]
                )
            else:
                url += ",".join(
                    [
                        "%s" % (ODataPathBuilder._encode_method_value(value))
                        for (i, value) in enumerate(path.parameters)
                        if value is not None
                    ]
                )
            url += ")"
        return url

    @staticmethod
    def _encode_method_value(value):
        if is_string_type(value):
            value = value.replace("'", "''")

            # Same replacements as SQL Server
            # https://web.archive.org/web/20150101222238/http://msdn.microsoft.com/en-us/library/aa226544(SQL.80).aspx
            # https://stackoverflow.com/questions/4229054/how-are-special-characters-handled-in-an-odata-query#answer-45883747
            value = value.replace("%", "%25")
            value = value.replace("+", "%2B")
            value = value.replace("/", "%2F")
            value = value.replace("?", "%3F")
            value = value.replace("#", "%23")
            value = value.replace("&", "%26")

            value = "'{0}'".format(value)
        elif isinstance(value, bool):
            value = str(value).lower()
        return value
