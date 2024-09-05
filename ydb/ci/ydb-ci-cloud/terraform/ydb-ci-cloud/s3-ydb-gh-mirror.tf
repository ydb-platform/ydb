resource "yandex_storage_bucket" "ydb-gh-runner-mirror" {
  bucket = "ydb-gh-runner-mirror"

  access_key = yandex_iam_service_account_static_access_key.s3-ydb-gh-logs.access_key
  secret_key = yandex_iam_service_account_static_access_key.s3-ydb-gh-logs.secret_key

  grant {
    permissions = ["READ", "WRITE"]
    type        = "CanonicalUser"
    id          = yandex_iam_service_account.s3-ydb-gh.id
  }
}
