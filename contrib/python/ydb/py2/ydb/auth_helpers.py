# -*- coding: utf-8 -*-
import os

from . import credentials, tracing
import warnings


def read_bytes(f):
    with open(f, "rb") as fr:
        return fr.read()


def load_ydb_root_certificate():
    path = os.getenv("YDB_SSL_ROOT_CERTIFICATES_FILE", None)
    if path is not None and os.path.exists(path):
        return read_bytes(path)
    return None


def construct_credentials_from_environ(tracer=None):
    tracer = tracer if tracer is not None else tracing.Tracer(None)
    warnings.warn(
        "using construct_credentials_from_environ method for credentials instantiation is deprecated and will be "
        "removed in the future major releases. Please instantialize credentials by default or provide correct credentials "
        "instance to the Driver."
    )

    # dynamically import required authentication libraries
    if (
        os.getenv("USE_METADATA_CREDENTIALS") is not None
        and int(os.getenv("USE_METADATA_CREDENTIALS")) == 1
    ):
        import ydb.iam

        tracing.trace(tracer, {"credentials.metadata": True})
        return ydb.iam.MetadataUrlCredentials()

    if os.getenv("YDB_TOKEN") is not None:
        tracing.trace(tracer, {"credentials.access_token": True})
        return credentials.AuthTokenCredentials(os.getenv("YDB_TOKEN"))

    if os.getenv("SA_KEY_FILE") is not None:

        import ydb.iam

        tracing.trace(tracer, {"credentials.sa_key_file": True})
        root_certificates_file = os.getenv("SSL_ROOT_CERTIFICATES_FILE", None)
        iam_channel_credentials = {}
        if root_certificates_file is not None:
            iam_channel_credentials = {
                "root_certificates": read_bytes(root_certificates_file)
            }
        return ydb.iam.ServiceAccountCredentials.from_file(
            os.getenv("SA_KEY_FILE"),
            iam_channel_credentials=iam_channel_credentials,
            iam_endpoint=os.getenv("IAM_ENDPOINT", "iam.api.cloud.yandex.net:443"),
        )
