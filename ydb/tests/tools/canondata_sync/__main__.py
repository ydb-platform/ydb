import argparse
import os

import requests

from ydb.tests.tools.canondata_sync.lib import s3, utils


def get_yc_token() -> str:
    yc_token = os.environ.get("YC_TOKEN")

    if not yc_token:
        print("Environment variable YC_TOKEN is required")
        print("run: export YC_TOKEN=$(yc --profile prod iam create-token)")
        raise SystemExit(1)

    return yc_token


def get_lockbox_secret(secret_id):
    token = get_yc_token()
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(
        f"https://payload.lockbox.api.cloud.yandex.net/lockbox/v1/secrets/{secret_id}/payload",
        headers=headers,
    )
    if response.status_code == 401:
        print("Invalid YC_TOKEN or secret_id")
        raise SystemExit(1)
    response.raise_for_status()
    return {e["key"]: e["textValue"] for e in response.json()["entries"]}


def get_s3_keys(secret_provider_name, secret_id):
    env_ydb_canonical_key_id = os.environ.get('YDB_CANONICAL_KEY_ID')
    env_ydb_canonical_key_secret = os.environ.get('YDB_CANONICAL_KEY_SECRET')
    if env_ydb_canonical_key_id is not None and env_ydb_canonical_key_secret is not None:
        return env_ydb_canonical_key_id, env_ydb_canonical_key_secret

    if secret_provider_name == "lockbox":
        secret = get_lockbox_secret(secret_id)
        return secret["KEY_ID"], secret["KEY_SECRET"]

    raise ValueError("Only yav is supported now")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--secret-provider", default="lockbox")
    parser.add_argument("--secret-id", default="e6qp4m4b2d8mmhnrb02v")
    parser.add_argument("--bucket-name", default="ydb-canondata")
    parser.add_argument("root_dir")

    args = parser.parse_args()
    aws_access_key_id, aws_secret_access_key = get_s3_keys(args.secret_provider, args.secret_id)

    found_objects = utils.get_urls(args.root_dir)
    if not found_objects:
        print("Nothing to sync")
        return

    bucket = s3.s3_get_bucket(aws_access_key_id, aws_secret_access_key, args.bucket_name)
    s3.do_sync(bucket, found_objects)


if __name__ == "__main__":
    main()
