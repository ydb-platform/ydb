# -*- coding: utf-8 -*-

import io
import os.path
import zipfile


def check_file(filename):
    if not os.path.exists(filename):
        raise FileNotFoundError(f'{filename} not found.')


def get_file_data(filename):
    """
    Read and return file bytes data.
    :param filename: filename to be read
    :return: file bytes data
    """
    with open(filename, 'rb') as f:
        d = f.read()
    return d


def seek_and_read_file_data(filename, offset=0, size=-1):
    """
    Seek offset and read part of file data.
    :param filename: filename to be read
    :param offset: seek position relative to 0
    :param size: number of bytes to read
    :return: file bytes data
    """
    with open(filename, 'rb') as f:
        f.seek(offset)
        d = f.read(size)
    return d


def create_zip_data_from_dir(_dir):
    """
    Create zip data from a dir.
    :param _dir: a extension dir path
    :return: zip data
    """
    zip_io = io.BytesIO()
    with zipfile.ZipFile(zip_io, 'w', zipfile.ZIP_DEFLATED) as zf:
        for root, dirs, files in os.walk(_dir):
            for file_name in files:
                file_path = os.path.join(root, file_name)
                zf.write(file_path, os.path.relpath(file_path, _dir))
    return zip_io.getvalue()
