import os
import tempfile


def get_valid_filename(filename, dirname):
    current_file, counter = filename, 0
    while os.path.exists(os.path.join(dirname, current_file)):
        current_file = "%s_%d" % (filename, counter)
        counter += 1
    valid_path = os.path.join(dirname, current_file)
    os.mknod(valid_path)
    return valid_path


def get_valid_tmpdir(name, tmp_dir):
    current_dir, counter = name, 0
    while os.path.exists(os.path.join(tmp_dir, current_dir)):
        current_dir = "%s_%d" % (name, counter)
        counter += 1
    os.mkdir(os.path.join(tmp_dir, current_dir))
    return os.path.join(tmp_dir, current_dir)


def get_base_tmpdir(name):
    tmppath = tempfile.gettempdir()
    return get_valid_tmpdir(name, tmppath)
