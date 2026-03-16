# -*- coding: utf-8 -*-
# Refer: https://source.chromium.org/chromium/chromium/src/+/main:components/crx_file/
import os.path
import struct
import zipfile

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric.utils import Prehashed

from crx3 import utils, crx3_pb2, id_util, key_util
from crx3.common import SIGNATURE_CONTEXT, FILE_BUFFER_SIZE, VERSION, CRX_FILE_HEADER_MAGIC
from crx3.exceptions import FileFormatNotSupportedError


def create_private_key_file(output_file):
    """
    Create a private key in pem format.
    :param output_file: private key file name for output, such as: extension.pem .
    """
    private_key = key_util.create_private_key()
    parent_path = os.path.dirname(output_file)
    if not os.path.exists(parent_path):
        os.makedirs(parent_path, exist_ok=True)
    with open(output_file, 'wb') as pf:
        pf.write(key_util.get_private_key_data(private_key))


def _create_signed_header_data(public_key):
    signed_header_data = crx3_pb2.SignedData()
    signed_header_data.crx_id = id_util.calc_crx_id_by_public_key(public_key)
    # print('public_key', public_key)
    # print('crx_id', signed_header_data.crx_id)
    return signed_header_data.SerializeToString()


def _sign_archive(signed_header_data, zip_data, private_key):
    signed_header_size_octets = struct.pack('<I', len(signed_header_data))
    sha256_algorithm = hashes.SHA256()
    sha256 = hashes.Hash(sha256_algorithm, default_backend())
    sha256.update(SIGNATURE_CONTEXT)
    sha256.update(signed_header_size_octets)
    sha256.update(signed_header_data)

    for i in range(0, len(zip_data), FILE_BUFFER_SIZE):
        if i + FILE_BUFFER_SIZE <= len(zip_data):
            sha256.update(zip_data[i: i + FILE_BUFFER_SIZE])
        else:
            sha256.update(zip_data[i: len(zip_data)])

    digest = sha256.finalize()
    # https://chromium.googlesource.com/chromium/src.git/+/refs/heads/main/crypto/signature_creator.h#44
    # Signs the precomputed sha256 algorithm digest data using private key as specified in PKCS #1 v1.5.
    return private_key.sign(digest, padding.PKCS1v15(), Prehashed(sha256_algorithm))


def _create_crx_file_header(public_key, archive_signed_data, signed_header_data):
    proof = crx3_pb2.AsymmetricKeyProof()
    proof.public_key = public_key
    proof.signature = archive_signed_data

    crx_file_header = crx3_pb2.CrxFileHeader()
    crx_file_header.sha256_with_rsa.append(proof)
    crx_file_header.signed_header_data = signed_header_data
    return crx_file_header.SerializeToString()


def _write_crx(crx_file_header, zip_data, output_file):
    parent_path = os.path.dirname(output_file)
    if not os.path.exists(parent_path):
        os.makedirs(parent_path, exist_ok=True)

    version_octets = struct.pack('<I', VERSION)
    header_size_octets = struct.pack('<I', len(crx_file_header))
    with open(output_file, 'wb') as crx:
        crx.write(CRX_FILE_HEADER_MAGIC)
        crx.write(version_octets)
        crx.write(header_size_octets)
        crx.write(crx_file_header)
        crx.write(zip_data)


def create_crx_file(zip_file_or_dir, private_key_file, output_file):
    """
    Create crx file from extension zip file or extension dir.
    :param zip_file_or_dir: extension zip file or extension dir
    :param private_key_file: private key file in pem format
    :param output_file: output filename, such as: extension.crx
    """
    utils.check_file(zip_file_or_dir)
    if os.path.isdir(zip_file_or_dir):
        zip_data = utils.create_zip_data_from_dir(zip_file_or_dir)
    elif zipfile.is_zipfile(zip_file_or_dir):
        zip_data = utils.get_file_data(zip_file_or_dir)
    else:
        raise FileFormatNotSupportedError(zip_file_or_dir)

    utils.check_file(private_key_file)
    private_key = key_util.load_private_key_from_pem(utils.get_file_data(private_key_file))
    public_key = key_util.get_public_key_data(key_util.extract_public_key_from_private_key(private_key))

    signed_header_data = _create_signed_header_data(public_key)

    archive_signed_data = _sign_archive(signed_header_data, zip_data, private_key)

    crx_file_header = _create_crx_file_header(public_key, archive_signed_data, signed_header_data)

    _write_crx(crx_file_header, zip_data, output_file)
