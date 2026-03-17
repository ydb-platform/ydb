import logging

log = logging.getLogger(__name__)

def metadata_to_dict(obj):
    """
    Convert object to dict with all serializable properties like:
    dict, list, set, tuple, str, bool, int, float, type, object, etc.
    """
    try:
        if isinstance(obj, dict):
            metadata = {}
            for key, value in obj.items():
                metadata[key] = metadata_to_dict(value)
            return metadata
        elif isinstance(obj, type):
            return str(obj)
        elif hasattr(obj, "_ast"):
            return metadata_to_dict(obj._ast())
        elif hasattr(obj, "__iter__") and not isinstance(obj, str):
            metadata = []
            for item in obj:
                metadata.append(metadata_to_dict(item))
            return metadata
        elif hasattr(obj, "__dict__"):
            metadata = {}
            for key, value in vars(obj).items():
                if not callable(value) and not key.startswith('_'):
                    metadata[key] = metadata_to_dict(value)
            return metadata
        else:
            return obj
    except Exception as e:
        import pprint
        log.warning("Failed to convert metadata to dict:\n%s", pprint.pformat(getattr(e, "args", None)))
        return {}
