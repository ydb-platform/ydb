# -*- coding: utf-8 -*-

import io
import os
import struct
import zipfile

from crx3.common import FILE_BUFFER_SIZE
from crx3.exceptions import FileFormatNotSupportedError


def _seek_to_zip_position(crx_fp):
    crx_fp.seek(8)  # Skip magic string and version
    header_size = crx_fp.read(4)  # Read header size
    header_size_num = struct.unpack('<I', header_size)[0]
    position = 12 + header_size_num
    crx_fp.seek(position)
    return position


def convert_to_zip(crx_file, output_zip_name):
    """
    Convert a CRX file to a ZIP file.
    :param crx_file: CRX3 file
    :param output_zip_name:
    :return:
    """
    crx_fp = open(crx_file, 'rb')
    position = _seek_to_zip_position(crx_fp)

    if os.path.isdir(output_zip_name):
        crx_fp.close()
        raise IsADirectoryError(output_zip_name)

    parent_path = os.path.dirname(output_zip_name)
    if os.path.exists(parent_path):
        if not os.path.isdir(parent_path):
            crx_fp.close()
            raise NotADirectoryError(parent_path)
    else:
        os.makedirs(parent_path, exist_ok=True)

    crx_file_size = os.path.getsize(crx_file)
    zip_data_len = crx_file_size - position
    with open(output_zip_name, 'wb') as zf:
        for i in range(0, zip_data_len, FILE_BUFFER_SIZE):
            if i + FILE_BUFFER_SIZE <= zip_data_len:
                zf.write(crx_fp.read(FILE_BUFFER_SIZE))
            else:
                zf.write(crx_fp.read())
    crx_fp.close()


def _unpack_zip(zip_file_or_data, output_dir):
    if os.path.exists(output_dir) and not os.path.isdir(output_dir):
        raise NotADirectoryError(output_dir)
    elif not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    if isinstance(zip_file_or_data, bytes):
        zf = zipfile.ZipFile(io.BytesIO(zip_file_or_data))
    else:
        zf = zipfile.ZipFile(zip_file_or_data)

    zf.extractall(output_dir)
    zf.close()


def _unpack_crx(crx_file, output_dir):
    crx_fp = open(crx_file, 'rb')
    _seek_to_zip_position(crx_fp)
    zip_data = crx_fp.read()
    _unpack_zip(zip_data, output_dir)


def unpack(crx_file, output_dir):
    if crx_file.lower().endswith('.zip'):
        _unpack_zip(crx_file, output_dir)
    elif crx_file.lower().endswith('.crx'):
        _unpack_crx(crx_file, output_dir)
    else:
        raise FileFormatNotSupportedError(crx_file)
