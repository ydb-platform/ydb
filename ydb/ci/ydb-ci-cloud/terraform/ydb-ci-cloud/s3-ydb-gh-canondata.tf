resource "yandex_lockbox_secret" "canondata-s3-secrets" {
  name        = "canondata-s3-secrets"
  description = "Contain S3 static key for uploading to canondata s3 bucket"
}


resource "yandex_iam_service_account" "team-canondata-s3" {
  name = "ydb-team-canondata-s3"
}


resource "yandex_lockbox_secret_iam_binding" "team-canondata-s3" {
  secret_id = yandex_lockbox_secret.canondata-s3-secrets.id
  for_each  = toset(["lockbox.payloadViewer", "lockbox.viewer"])
  role      = each.value
  members = [
    "serviceAccount:${yandex_iam_service_account.team-canondata-s3.id}"
  ]
}
#
#
#resource "yandex_iam_service_account" "s3-canondata" {
#  name = "s3-canondata"
#}
#
#resource "yandex_resourcemanager_folder_iam_member" "s3-canondata-roles" {
#  for_each  = toset(["storage.admin"])
#  folder_id = var.folder_id
#  member    = "serviceAccount:${yandex_iam_service_account.s3-canondata.id}"
#  role      = each.key
#}
#
#resource "yandex_iam_service_account_static_access_key" "s3-canondata" {
#  service_account_id = yandex_iam_service_account.s3-canondata.id
#  description        = "static access key for canondata object storage (used by terraform)"
#}
#
#resource "yandex_storage_bucket" "canondata" {
#  bucket = "ydb-gh-canondata"
#
#  access_key = yandex_iam_service_account_static_access_key.s3-canondata.access_key
#  secret_key = yandex_iam_service_account_static_access_key.s3-canondata.secret_key
#
#
#  grant {
#    permissions = ["READ", "WRITE"]
#    type        = "CanonicalUser"
#    id          = yandex_iam_service_account.team-canondata-s3.id
#  }
#
#}
