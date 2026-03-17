def read_content_types_xml_element(element):
    extension_defaults = dict(map(
        _read_default,
        element.find_children("content-types:Default")
    ))
    overrides = dict(map(
        _read_override,
        element.find_children("content-types:Override")
    ))
    return _ContentTypes(extension_defaults, overrides)


def _read_default(element):
    extension = element.attributes["Extension"]
    content_type = element.attributes["ContentType"]
    return extension, content_type


def _read_override(element):
    part_name = element.attributes["PartName"]
    content_type = element.attributes["ContentType"]
    return part_name.lstrip("/"), content_type


class _ContentTypes(object):
    _image_content_types = {
        "png": "png",
        "gif": "gif",
        "jpeg": "jpeg",
        "jpg": "jpeg",
        "tif": "tiff",
        "tiff": "tiff",
        "bmp": "bmp",
    }
    
    def __init__(self, extension_defaults, overrides):
        self._extension_defaults = extension_defaults
        self._overrides = overrides
    
    def find_content_type(self, path):
        if path in self._overrides:
            return self._overrides[path]

        extension = _get_extension(path)
        default_type = self._extension_defaults.get(extension)
        if default_type is not None:
            return default_type

        image_type = self._image_content_types.get(extension.lower())
        if image_type is not None:
            return "image/" + image_type
        
        return None

empty_content_types = _ContentTypes({}, {})

def _get_extension(path):
    return path.rpartition(".")[2]
