# coding: utf-8

def get_modified_time(storage, path):
    if hasattr(storage, "get_modified_time"):
        return storage.get_modified_time(path)
    return storage.modified_time(path)
    