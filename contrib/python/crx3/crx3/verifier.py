# -*- coding: utf-8 -*-

import base64
import hashlib
import os.path
import os.path
import struct
from enum import Enum

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric.utils import Prehashed

from crx3 import crx3_pb2, id_util, key_util
from crx3.common import CRX_FILE_HEADER_MAGIC_SIZE, CRX_FILE_HEADER_MAGIC, SIGNATURE_CONTEXT, FILE_BUFFER_SIZE
from crx3.crx_info import CrxHeaderInfo


class VerifierResult(Enum):
    """
    Refer: https://source.chromium.org/chromium/chromium/src/+/main:components/crx_file/crx_verifier.h
    """
    OK_FULL = 'OK_FULL'  # The file verifies as a correct full CRX file.
    # OK_DELTA = 'OK_DELTA'  # The file verifies as a correct differential CRX file.
    ERROR_FILE_NOT_READABLE = 'ERROR_FILE_NOT_READABLE'  # Unsupported: Cannot open the CRX file.
    ERROR_HEADER_INVALID = 'ERROR_HEADER_INVALID'  # Failed to parse or understand CRX header.
    # ERROR_EXPECTED_HASH_INVALID = 'ERROR_EXPECTED_HASH_INVALID'  # Unsupported: Expected hash is not well-formed.
    # ERROR_FILE_HASH_FAILED = 'ERROR_FILE_HASH_FAILED'  # Unsupported: The file's actual hash != the expected hash.
    # ERROR_SIGNATURE_INITIALIZATION_FAILED = 'ERROR_SIGNATURE_INITIALIZATION_FAILED'  # Unsupported: A signature or key is malformed.
    ERROR_SIGNATURE_VERIFICATION_FAILED = 'ERROR_SIGNATURE_VERIFICATION_FAILED'  # A signature doesn't match.
    ERROR_REQUIRED_PROOF_MISSING = 'ERROR_REQUIRED_PROOF_MISSING'  # RequireKeyProof was unsatisfied.


def _verity_signature(crx_fp, file_size, public_key_bytes, signed_header_data, signature):
    signed_header_size_octets = struct.pack('<I', len(signed_header_data))
    sha256_algorithm = hashes.SHA256()
    sha256 = hashes.Hash(sha256_algorithm, default_backend())
    sha256.update(SIGNATURE_CONTEXT)
    sha256.update(signed_header_size_octets)
    sha256.update(signed_header_data)

    current_pos = crx_fp.tell()
    zip_data_len = file_size - current_pos
    for i in range(0, zip_data_len, FILE_BUFFER_SIZE):
        if i + FILE_BUFFER_SIZE <= zip_data_len:
            sha256.update(crx_fp.read(FILE_BUFFER_SIZE))
        else:
            sha256.update(crx_fp.read())

    digest = sha256.finalize()

    public_key = key_util.load_public_key_from_der(public_key_bytes)
    return public_key.verify(signature, digest, padding.PKCS1v15(), Prehashed(sha256_algorithm))


def verify(crx_file, required_key_hashes=None):
    """
    Verify the file as a valid Crx of CRX3 format.
    :param crx_file: crx file path.
    :param required_key_hashes: a list of hex strings of public key hashes.
    :return: (VerifierResult, CrxHeaderInfo or None)
    """
    if not os.access(crx_file, os.F_OK | os.R_OK):
        return VerifierResult.ERROR_FILE_NOT_READABLE, None

    crx_file_size = os.path.getsize(crx_file)
    crx_fp = open(crx_file, 'rb')

    def return_error(verifier_error):
        crx_fp.close()
        return verifier_error, None

    # Check magic number
    magic_number = crx_fp.read(CRX_FILE_HEADER_MAGIC_SIZE)
    if len(magic_number) < CRX_FILE_HEADER_MAGIC_SIZE:
        return return_error(VerifierResult.ERROR_HEADER_INVALID)
    if magic_number != CRX_FILE_HEADER_MAGIC:
        return return_error(VerifierResult.ERROR_HEADER_INVALID)

    # Check version
    version_number = crx_fp.read(4)
    if len(version_number) != 4:
        return return_error(VerifierResult.ERROR_HEADER_INVALID)
    if struct.unpack('<I', version_number)[0] != 3:
        return return_error(VerifierResult.ERROR_HEADER_INVALID)

    # verify crx3
    header_size = crx_fp.read(4)
    if len(header_size) != 4:
        return return_error(VerifierResult.ERROR_HEADER_INVALID)
    header_size_num = struct.unpack('<I', header_size)[0]
    header_bytes = crx_fp.read(header_size_num)
    if len(header_bytes) != header_size_num:
        return return_error(VerifierResult.ERROR_HEADER_INVALID)
    crx_file_header = crx3_pb2.CrxFileHeader()
    crx_file_header.ParseFromString(header_bytes)

    signed_header_data = crx3_pb2.SignedData()
    signed_header_data.ParseFromString(crx_file_header.signed_header_data)
    crx_id = signed_header_data.crx_id
    declared_crx_id = id_util.convert_hex_crx_id_to_alphabet(crx_id.hex())

    current_pos = crx_fp.tell()

    # verified_contents = crx_file_header.verified_contents
    # Ignore verified_contents
    # print('verified_contents', verified_contents)
    rsa_proofs = crx_file_header.sha256_with_rsa
    if len(rsa_proofs) == 0:
        return return_error(VerifierResult.ERROR_REQUIRED_PROOF_MISSING)

    public_key_bytes = None
    required_key_set = set(required_key_hashes or [])
    for rsa_proof in rsa_proofs:
        # print(rsa_proof)
        if id_util.calc_crx_id_by_public_key(rsa_proof.public_key) == crx_id:
            public_key_bytes = rsa_proof.public_key
        crx_fp.seek(current_pos)
        try:
            # Verify signature
            _verity_signature(crx_fp, crx_file_size, rsa_proof.public_key, crx_file_header.signed_header_data,
                              rsa_proof.signature)
        except Exception as e:
            # print(e)
            return return_error(VerifierResult.ERROR_SIGNATURE_VERIFICATION_FAILED)
        public_key_hash = hashlib.sha256(rsa_proof.public_key).hexdigest()
        required_key_set.discard(public_key_hash)

    if public_key_bytes is None or required_key_set:
        return return_error(VerifierResult.ERROR_REQUIRED_PROOF_MISSING)
    # Convert public key bytes to base64 string.
    public_key_str = base64.b64encode(public_key_bytes).decode('utf-8')

    crx_fp.close()
    return VerifierResult.OK_FULL, CrxHeaderInfo(declared_crx_id, public_key_str)
