#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

import logging
from unittest import mock

from snowflake.connector.secret_detector import SecretDetector


def basic_masking(test_str):
    masked, masked_str, err_str = SecretDetector.mask_secrets(test_str)
    assert not masked
    assert err_str is None
    assert masked_str == test_str


def test_none_string():
    basic_masking(None)


def test_empty_string():
    basic_masking("")


def test_no_masking():
    basic_masking("This string is innocuous")


@mock.patch.object(
    SecretDetector,
    "mask_connection_token",
    mock.Mock(side_effect=Exception("Test exception")),
)
def test_exception_in_masking():
    test_str = "This string will raise an exception"
    masked, masked_str, err_str = SecretDetector.mask_secrets(test_str)
    assert masked
    assert err_str == "Test exception"
    assert masked_str == "Test exception"


def exception_in_log_masking():
    test_str = "This string will raise an exception"
    log_record = logging.LogRecord(
        SecretDetector.__name__,
        logging.DEBUG,
        "test_unit_log_secret_detector.py",
        45,
        test_str,
        list(),
        None,
    )
    log_record.asctime = "2003-07-08 16:49:45,896"
    secret_detector = SecretDetector()
    sanitized_log = secret_detector.format(log_record)
    assert "Test exception" in sanitized_log
    assert "secret_detector.py" in sanitized_log
    assert "sanitize_log_str" in sanitized_log
    assert test_str not in sanitized_log


@mock.patch.object(
    SecretDetector,
    "mask_connection_token",
    mock.Mock(side_effect=Exception("Test exception")),
)
def test_exception_in_secret_detector_while_log_masking():
    exception_in_log_masking()


@mock.patch.object(
    SecretDetector, "mask_secrets", mock.Mock(side_effect=Exception("Test exception"))
)
def test_exception_while_log_masking():
    exception_in_log_masking()


def test_mask_token():
    long_token = (
        "_Y1ZNETTn5/qfUWj3Jedby7gipDzQs=U"
        "KyJH9DS=nFzzWnfZKGV+C7GopWCGD4Lj"
        "OLLFZKOE26LXHDt3pTi4iI1qwKuSpf/F"
        "mClCMBSissVsU3Ei590FP0lPQQhcSGcD"
        "u69ZL_1X6e9h5z62t/iY7ZkII28n2qU="
        "nrBJUgPRCIbtJQkVJXIuOHjX4G5yUEKj"
        "ZBAx4w6=_lqtt67bIA=o7D=oUSjfywsR"
        "FoloNIkBPXCwFTv+1RVUHgVA2g8A9Lw5"
        "XdJYuI8vhg=f0bKSq7AhQ2Bh"
    )

    token_str_w_prefix = "Token =" + long_token
    masked, masked_str, err_str = SecretDetector.mask_secrets(token_str_w_prefix)
    assert masked
    assert err_str is None
    assert masked_str == "Token =****"

    id_token_str_w_prefix = "idToken : " + long_token
    masked, masked_str, err_str = SecretDetector.mask_secrets(id_token_str_w_prefix)
    assert masked
    assert err_str is None
    assert masked_str == "idToken : ****"

    session_token_w_prefix = "sessionToken : " + long_token
    masked, masked_str, err_str = SecretDetector.mask_secrets(session_token_w_prefix)
    assert masked
    assert err_str is None
    assert masked_str == "sessionToken : ****"

    master_token_w_prefix = "masterToken : " + long_token
    masked, masked_str, err_str = SecretDetector.mask_secrets(master_token_w_prefix)
    assert masked
    assert err_str is None
    assert masked_str == "masterToken : ****"

    assertion_w_prefix = "assertion content:" + long_token
    masked, masked_str, err_str = SecretDetector.mask_secrets(assertion_w_prefix)
    assert masked
    assert err_str is None
    assert masked_str == "assertion content:****"


def test_token_false_positives():
    false_positive_token_str = (
        "2020-04-30 23:06:04,069 - MainThread auth.py:397"
        " - write_temporary_credential() - DEBUG - no ID "
        "token is given when try to store temporary credential"
    )

    masked, masked_str, err_str = SecretDetector.mask_secrets(false_positive_token_str)
    assert not masked
    assert err_str is None
    assert masked_str == false_positive_token_str


def test_password():
    random_password = "Fh[+2J~AcqeqW%?"
    random_password_w_prefix = "password:" + random_password
    masked, masked_str, err_str = SecretDetector.mask_secrets(random_password_w_prefix)
    assert masked
    assert err_str is None
    assert masked_str == "password:****"

    random_password_caps = "PASSWORD:" + random_password
    masked, masked_str, err_str = SecretDetector.mask_secrets(random_password_caps)
    assert masked
    assert err_str is None
    assert masked_str == "PASSWORD:****"

    random_password_mix_case = "PassWorD:" + random_password
    masked, masked_str, err_str = SecretDetector.mask_secrets(random_password_mix_case)
    assert masked
    assert err_str is None
    assert masked_str == "PassWorD:****"

    random_password_equal_sign = "password = " + random_password
    masked, masked_str, err_str = SecretDetector.mask_secrets(
        random_password_equal_sign
    )
    assert masked
    assert err_str is None
    assert masked_str == "password = ****"

    random_password = "Fh[+2J~AcqeqW%?"
    random_password_w_prefix = "pwd:" + random_password
    masked, masked_str, err_str = SecretDetector.mask_secrets(random_password_w_prefix)
    assert masked
    assert err_str is None
    assert masked_str == "pwd:****"


def test_token_password():
    long_token = (
        "_Y1ZNETTn5/qfUWj3Jedby7gipDzQs=U"
        "KyJH9DS=nFzzWnfZKGV+C7GopWCGD4Lj"
        "OLLFZKOE26LXHDt3pTi4iI1qwKuSpf/F"
        "mClCMBSissVsU3Ei590FP0lPQQhcSGcD"
        "u69ZL_1X6e9h5z62t/iY7ZkII28n2qU="
        "nrBJUgPRCIbtJQkVJXIuOHjX4G5yUEKj"
        "ZBAx4w6=_lqtt67bIA=o7D=oUSjfywsR"
        "FoloNIkBPXCwFTv+1RVUHgVA2g8A9Lw5"
        "XdJYuI8vhg=f0bKSq7AhQ2Bh"
    )

    long_token2 = (
        "ktL57KJemuq4-M+Q0pdRjCIMcf1mzcr"
        "MwKteDS5DRE/Pb+5MzvWjDH7LFPV5b_"
        "/tX/yoLG3b4TuC6Q5qNzsARPPn_zs/j"
        "BbDOEg1-IfPpdsbwX6ETeEnhxkHIL4H"
        "sP-V"
    )

    random_pwd = "Fh[+2J~AcqeqW%?"
    random_pwd2 = random_pwd + "vdkav13"

    test_string_w_prefix = (
        "token=" + long_token + " random giberish " + "password:" + random_pwd
    )
    masked, masked_str, err_str = SecretDetector.mask_secrets(test_string_w_prefix)
    assert masked
    assert err_str is None
    assert masked_str == "token=****" + " random giberish " + "password:****"

    # order reversed
    test_string_w_prefix = (
        "password:" + random_pwd + " random giberish " + "token=" + long_token
    )

    masked, masked_str, err_str = SecretDetector.mask_secrets(test_string_w_prefix)
    assert masked
    assert err_str is None
    assert masked_str == "password:****" + " random giberish " + "token=****"

    # multiple tokens and password
    test_string_w_prefix = (
        "token="
        + long_token
        + " random giberish "
        + "password:"
        + random_pwd
        + " random giberish "
        + "idToken:"
        + long_token2
    )
    masked, masked_str, err_str = SecretDetector.mask_secrets(test_string_w_prefix)
    assert masked
    assert err_str is None
    assert (
        masked_str
        == "token=****"
        + " random giberish "
        + "password:****"
        + " random giberish "
        + "idToken:****"
    )

    # multiple passwords
    test_string_w_prefix = (
        "password=" + random_pwd + " random giberish " + "pwd:" + random_pwd2
    )
    masked, masked_str, err_str = SecretDetector.mask_secrets(test_string_w_prefix)
    assert masked
    assert err_str is None
    assert masked_str == "password=" + "****" + " random giberish " + "pwd:" + "****"

    test_string_w_prefix = (
        "password="
        + random_pwd
        + " random giberish "
        + "password="
        + random_pwd2
        + " random giberish "
        + "password="
        + random_pwd
    )
    masked, masked_str, err_str = SecretDetector.mask_secrets(test_string_w_prefix)
    assert masked
    assert err_str is None
    assert (
        masked_str
        == "password="
        + "****"
        + " random giberish "
        + "password="
        + "****"
        + " random giberish "
        + "password="
        + "****"
    )
