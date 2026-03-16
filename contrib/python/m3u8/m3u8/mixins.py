from os.path import dirname
from urllib.parse import urljoin, urlsplit


class BasePathMixin:
    @property
    def absolute_uri(self):
        if self.uri is None:
            return None

        ret = urljoin(self.base_uri, self.uri)
        if self.base_uri and (not urlsplit(self.base_uri).scheme):
            return ret

        if not urlsplit(ret).scheme:
            raise ValueError("There can not be `absolute_uri` with no `base_uri` set")

        return ret

    @property
    def base_path(self):
        if self.uri is None:
            return None
        return dirname(self.get_path_from_uri())

    def get_path_from_uri(self):
        """Some URIs have a slash in the query string."""
        return self.uri.split("?")[0]

    @base_path.setter
    def base_path(self, newbase_path):
        if self.uri is not None:
            if not self.base_path:
                self.uri = f"{newbase_path}/{self.uri}"
            else:
                self.uri = self.uri.replace(self.base_path, newbase_path)


class GroupedBasePathMixin:
    def _set_base_uri(self, new_base_uri):
        for item in self:
            item.base_uri = new_base_uri

    base_uri = property(None, _set_base_uri)

    def _set_base_path(self, newbase_path):
        for item in self:
            item.base_path = newbase_path

    base_path = property(None, _set_base_path)
