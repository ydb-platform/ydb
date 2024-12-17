resource "yandex_iam_service_account" "s3-ydb-gh" {
  name        = "s3-ydb-gh"
  description = "ydb github s3 SA"
}

resource "yandex_iam_service_account_static_access_key" "s3-ydb-gh" {
  service_account_id = yandex_iam_service_account.s3-ydb-gh.id
  description        = "static access key for github"
}


resource "yandex_lockbox_secret" "s3-ydb-gh-sa" {
  name        = "s3-ydb-gh-sa"
  description = "Keys for s3-ydb-gh SA"
}

resource "yandex_lockbox_secret_version" "s3-ydb-gh-sa" {
  secret_id = yandex_lockbox_secret.s3-ydb-gh-sa.id

  entries {
    key        = "access_key"
    text_value = yandex_iam_service_account_static_access_key.s3-ydb-gh.access_key
  }
  entries {
    key        = "secret_key"
    text_value = yandex_iam_service_account_static_access_key.s3-ydb-gh.secret_key
  }
}