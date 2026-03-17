import os
import pkgutil
import tempfile
import __res

VERSION = (2, 0, 1)
__version__ = '.'.join(map(str, VERSION))

resource_temp_dir = None


def extract_resources():
    global resource_temp_dir
    if resource_temp_dir is not None:
        return
    resource_temp_dir = tempfile.TemporaryDirectory()
    resource_files_path = __res.resfs_files("contrib/python/flower/flower")
    for files_path in resource_files_path:
        rel_resource_path = os.path.relpath(files_path.decode("utf-8"), "contrib/python/flower/flower")
        rel_new_resource_temp_dir_path = os.path.dirname(rel_resource_path)
        abs_new_resource_temp_dir_path = os.path.join(resource_temp_dir.name, rel_new_resource_temp_dir_path)
        os.makedirs(abs_new_resource_temp_dir_path, exist_ok=True)

        resource_data = pkgutil.get_data("flower", rel_resource_path)
        with open(os.path.join(resource_temp_dir.name, rel_resource_path), "wb") as new_resource_temp_file:
            new_resource_temp_file.write(resource_data)


extract_resources()
