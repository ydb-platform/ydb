resource "yandex_iam_service_account" "s3-ydb-gh-logs" {
  name = "s3-ydb-gh-logs"
}

resource "yandex_resourcemanager_folder_iam_member" "s3-ydb-gh-logs-roles" {
  for_each  = toset(["storage.admin"])
  folder_id = var.folder_id
  member    = "serviceAccount:${yandex_iam_service_account.s3-ydb-gh-logs.id}"
  role      = each.key
}

resource "yandex_iam_service_account_static_access_key" "s3-ydb-gh-logs" {
  service_account_id = yandex_iam_service_account.s3-ydb-gh-logs.id
  description        = "static access key for ydb-gh-logs object storage (used by terraform)"
}


resource "yandex_storage_bucket" "ydb-gh-logs" {
  bucket = "ydb-gh-logs"

  access_key = yandex_iam_service_account_static_access_key.s3-ydb-gh-logs.access_key
  secret_key = yandex_iam_service_account_static_access_key.s3-ydb-gh-logs.secret_key

  grant {
    permissions = ["READ", "WRITE"]
    type        = "CanonicalUser"
    id          = yandex_iam_service_account.s3-ydb-gh.id
  }

  lifecycle_rule {
    id      = "Expire testing_out_stuff after 30 days"
    enabled = true

    expiration {
      days = 30
    }

    filter {
      prefix = "testing_out_stuff"
    }
  }

  lifecycle_rule {
    id      = "Expire logs files after 45 days"
    enabled = true

    expiration {
      days = 45
    }

    filter {
      prefix = "ydb-platform"
    }
  }
}
