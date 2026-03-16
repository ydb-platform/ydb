#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

import codecs
import glob
import os
from os import path

from snowflake.connector import encryption_util
from snowflake.connector.constants import UTF8
from snowflake.connector.encryption_util import SnowflakeEncryptionUtil
from snowflake.connector.file_util import SnowflakeFileUtil

try:  # pragma: no cover
    from snowflake.connector.storage_client import SnowflakeFileEncryptionMaterial
except ImportError:  # keep olddrivertest from breaking
    from snowflake.connector.remote_storage_util import (
        SnowflakeFileEncryptionMaterial,
    )

from ..generate_test_files import generate_k_lines_of_n_files

THIS_DIR = path.dirname(path.realpath(__file__))


def test_encrypt_decrypt_file(tmp_path):
    """Encrypts and Decrypts a file."""
    encryption_material = SnowflakeFileEncryptionMaterial(
        query_stage_master_key="ztke8tIdVt1zmlQIZm0BMA==",
        query_id="123873c7-3a66-40c4-ab89-e3722fbccce1",
        smk_id=3112,
    )
    data = "test data"
    input_file = tmp_path / "test_encrypt_decrypt_file"
    encrypted_file = None
    decrypted_file = None
    try:
        with input_file.open("w", encoding=UTF8) as fd:
            fd.write(data)

        (metadata, encrypted_file) = SnowflakeEncryptionUtil.encrypt_file(
            encryption_material, input_file
        )
        decrypted_file = SnowflakeEncryptionUtil.decrypt_file(
            metadata, encryption_material, encrypted_file
        )

        contents = ""
        with codecs.open(decrypted_file, "r", encoding=UTF8) as fd:
            for line in fd:
                contents += line
        assert data == contents, "encrypted and decrypted contents"
    finally:
        input_file.unlink()
        if encrypted_file:
            os.remove(encrypted_file)
        if decrypted_file:
            os.remove(decrypted_file)


def test_encrypt_decrypt_large_file(tmpdir):
    """Encrypts and Decrypts a large file."""
    encryption_material = SnowflakeFileEncryptionMaterial(
        query_stage_master_key="ztke8tIdVt1zmlQIZm0BMA==",
        query_id="123873c7-3a66-40c4-ab89-e3722fbccce1",
        smk_id=3112,
    )

    # generates N files
    number_of_files = 1
    number_of_lines = 100_000
    tmp_dir = generate_k_lines_of_n_files(
        number_of_lines, number_of_files, tmp_dir=str(tmpdir.mkdir("data"))
    )

    files = glob.glob(os.path.join(tmp_dir, "file*"))
    input_file = files[0]
    encrypted_file = None
    decrypted_file = None
    try:
        digest_in, size_in = SnowflakeFileUtil.get_digest_and_size_for_file(input_file)
        for run_count in range(2):
            # Test padding cases when size is and is not multiple of block_size
            if run_count == 1:
                # second time run, truncate the file to test a different padding case
                with open(input_file, "wb") as f_in:
                    if size_in % encryption_util.block_size == 0:
                        size_in -= 3
                    else:
                        size_in -= size_in % encryption_util.block_size
                    f_in.truncate(size_in)
                digest_in, size_in = SnowflakeFileUtil.get_digest_and_size_for_file(
                    input_file
                )

            (metadata, encrypted_file) = SnowflakeEncryptionUtil.encrypt_file(
                encryption_material, input_file
            )
            decrypted_file = SnowflakeEncryptionUtil.decrypt_file(
                metadata, encryption_material, encrypted_file
            )

            digest_dec, size_dec = SnowflakeFileUtil.get_digest_and_size_for_file(
                decrypted_file
            )
            assert size_in == size_dec
            assert digest_in == digest_dec

    finally:
        os.remove(input_file)
        if encrypted_file:
            os.remove(encrypted_file)
        if decrypted_file:
            os.remove(decrypted_file)
