resource "yandex_iam_service_account" "vm-sa" {
  name = "gh-runner-vm-sa"
}


resource "yandex_resourcemanager_folder_iam_binding" "vm-sa-monitoring-binding" {
  folder_id = var.folder_id
  members = [
    "serviceAccount:${yandex_iam_service_account.vm-sa.id}"
  ]
  role = "monitoring.editor"
}
