#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

import random
import string

from snowflake.connector.secret_detector import SecretDetector


def test_mask_aws_secret():
    sql = (
        "copy into 's3://xxxx/test' from \n"
        "(select seq1(), random()\n"
        ", random(), random(), random(), random()\n"
        ", random(), random(), random(), random()\n"
        ", random() , random(), random(), random()\n"
        "\tfrom table(generator(rowcount => 10000)))\n"
        "credentials=(\n"
        "  aws_key_id='xxdsdfsafds'\n"
        "  aws_secret_key='safas+asfsad+safasf'\n"
        "  )\n"
        "OVERWRITE = TRUE \n"
        "MAX_FILE_SIZE = 500000000 \n"
        "HEADER = TRUE \n"
        "FILE_FORMAT = (TYPE = PARQUET SNAPPY_COMPRESSION = TRUE )\n"
        ";"
    )

    correct = (
        "copy into 's3://xxxx/test' from \n"
        "(select seq1(), random()\n"
        ", random(), random(), random(), random()\n"
        ", random(), random(), random(), random()\n"
        ", random() , random(), random(), random()\n"
        "\tfrom table(generator(rowcount => 10000)))\n"
        "credentials=(\n"
        "  aws_key_id='****'\n"
        "  aws_secret_key='****'\n"
        "  )\n"
        "OVERWRITE = TRUE \n"
        "MAX_FILE_SIZE = 500000000 \n"
        "HEADER = TRUE \n"
        "FILE_FORMAT = (TYPE = PARQUET SNAPPY_COMPRESSION = TRUE )\n"
        ";"
    )

    # Mask an aws key id and secret key
    _, masked_sql, _ = SecretDetector.mask_secrets(sql)
    assert masked_sql == correct


def test_mask_sas_token():
    azure_sas_token = (
        "https://someaccounts.blob.core.windows.net/results/018b90ab-0033-"
        "5f8e-0000-14f1000bd376_0/main/data_0_0_1?sv=2015-07-08&amp;"
        "sig=iCvQmdZngZNW%2F4vw43j6%2BVz6fndHF5LI639QJba4r8o%3D&amp;"
        "spr=https&amp;st=2016-04-12T03%3A24%3A31Z&amp;"
        "se=2016-04-13T03%3A29%3A31Z&amp;srt=s&amp;ss=bf&amp;sp=rwl"
    )

    masked_azure_sas_token = (
        "https://someaccounts.blob.core.windows.net/results/018b90ab-0033-"
        "5f8e-0000-14f1000bd376_0/main/data_0_0_1?sv=2015-07-08&amp;"
        "sig=****&amp;"
        "spr=https&amp;st=2016-04-12T03%3A24%3A31Z&amp;"
        "se=2016-04-13T03%3A29%3A31Z&amp;srt=s&amp;ss=bf&amp;sp=rwl"
    )

    s3_sas_token = (
        "https://somebucket.s3.amazonaws.com/vzy1-s-va_demo0/results/018b92f3"
        "-01c2-02dd-0000-03d5000c8066_0/main/data_0_0_1?"
        "x-amz-server-side-encryption-customer-algorithm=AES256&"
        "response-content-encoding=gzip&AWSAccessKeyId=AKIAIOSFODNN7EXAMPLE"
        "&Expires=1555481960&Signature=zFiRkdB9RtRRYomppVes4fQ%2ByWw%3D"
    )

    masked_s3_sas_token = (
        "https://somebucket.s3.amazonaws.com/vzy1-s-va_demo0/results/018b92f3"
        "-01c2-02dd-0000-03d5000c8066_0/main/data_0_0_1?"
        "x-amz-server-side-encryption-customer-algorithm=AES256&"
        "response-content-encoding=gzip&AWSAccessKeyId=****"
        "&Expires=1555481960&Signature=****"
    )

    # Mask azure token
    _, masked_text, _ = SecretDetector.mask_secrets(azure_sas_token)
    assert masked_text == masked_azure_sas_token

    # Mask s3 token
    _, masked_text, _ = SecretDetector.mask_secrets(s3_sas_token)
    assert masked_text == masked_s3_sas_token

    text = "".join([random.choice(string.ascii_lowercase) for i in range(200)])
    _, masked_text, _ = SecretDetector.mask_secrets(text)
    # Randomly generated string should cause no substitutions
    assert masked_text == text

    # Mask multiple azure tokens
    _, masked_text, _ = SecretDetector.mask_secrets(
        azure_sas_token + "\n" + azure_sas_token
    )
    assert masked_text == masked_azure_sas_token + "\n" + masked_azure_sas_token

    # Mask multiple s3 tokens
    _, masked_text, _ = SecretDetector.mask_secrets(s3_sas_token + "\n" + s3_sas_token)
    assert masked_text == masked_s3_sas_token + "\n" + masked_s3_sas_token

    # Mask azure and s3 token
    _, masked_text, _ = SecretDetector.mask_secrets(
        azure_sas_token + "\n" + s3_sas_token
    )
    assert masked_text == masked_azure_sas_token + "\n" + masked_s3_sas_token


def test_mask_secrets():
    sql = (
        "create stage mystage "
        "URL = 's3://mybucket/mypath/' "
        "credentials = (aws_key_id = 'AKIAIOSFODNN7EXAMPLE' "
        "aws_secret_key = 'frJIUN8DYpKDtOLCwo//yllqDzg='); "
        "create stage mystage2 "
        "URL = 'azure//mystorage.blob.core.windows.net/cont' "
        "credentials = (azure_sas_token = "
        "'?sv=2016-05-31&ss=b&srt=sco&sp=rwdl&se=2018-06-27T10:05:50Z&"
        "st=2017-06-27T02:05:50Z&spr=https,http&"
        "sig=bgqQwoXwxzuD2GJfagRg7VOS8hzNr3QLT7rhS8OFRLQ%3D')"
    )

    masked_sql = (
        "create stage mystage "
        "URL = 's3://mybucket/mypath/' "
        "credentials = (aws_key_id='****' "
        "aws_secret_key='****'); "
        "create stage mystage2 "
        "URL = 'azure//mystorage.blob.core.windows.net/cont' "
        "credentials = (azure_sas_token = "
        "'?sv=2016-05-31&ss=b&srt=sco&sp=rwdl&se=2018-06-27T10:05:50Z&"
        "st=2017-06-27T02:05:50Z&spr=https,http&"
        "sig=****')"
    )

    # Test masking all kinds of secrets
    _, masked_text, _ = SecretDetector.mask_secrets(sql)
    assert masked_text == masked_sql

    text = "".join([random.choice(string.ascii_lowercase) for i in range(500)])
    _, masked_text, _ = SecretDetector.mask_secrets(text)
    # Randomly generated string should cause no substitutions
    assert masked_text == text


def test_mask_private_keys():
    text = '"privateKeyData": "aslkjdflasjf"'

    filtered_text = '"privateKeyData": "XXXX"'

    _, result, _ = SecretDetector.mask_secrets(text)
    assert result == filtered_text
