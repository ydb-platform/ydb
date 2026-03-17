def substitute(json_obj, hook):
    if hook is None:
        return json_obj
    if isinstance(json_obj, dict):
        return hook(
            {key: substitute(value, hook) for key, value in json_obj.items()},
        )
    if isinstance(json_obj, list):
        return [substitute(element, hook) for element in json_obj]
    return json_obj


def build_object_hook(object_hook):
    if object_hook is None:
        return None
    if callable(object_hook):
        return object_hook
    if isinstance(object_hook, dict):
        object_hook = object_hook.items()

    def hook(doc):
        for key, hook in object_hook:
            if key in doc:
                return hook(doc)
        return doc

    return hook
