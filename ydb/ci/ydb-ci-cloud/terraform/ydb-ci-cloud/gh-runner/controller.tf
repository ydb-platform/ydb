resource "yandex_iam_service_account" "runner-controller" {
  name = "gh-runner-runner-sa"
}

resource "yandex_resourcemanager_folder_iam_binding" "compute-sa-binding" {
  folder_id = var.folder_id
  members = [
    "serviceAccount:${yandex_iam_service_account.runner-controller.id}"
  ]
  role = "compute.editor"
}

resource "yandex_lockbox_secret_iam_binding" "controller-gh-lockbox-binding" {
  members = [
    "serviceAccount:${yandex_iam_service_account.runner-controller.id}"
  ]
  role      = "lockbox.payloadViewer"
  secret_id = yandex_lockbox_secret.github-secrets.id
}

resource "yandex_lockbox_secret_iam_binding" "controller-lockbox-secrets-binding" {
  members = [
    "serviceAccount:${yandex_iam_service_account.runner-controller.id}"
  ]
  secret_id = yandex_lockbox_secret.secrets.id
  role      = "lockbox.payloadViewer"
}


resource "yandex_container_registry_iam_binding" "controller-registry-iam-binding" {
  registry_id = var.registry_id
  role        = "container-registry.images.puller"
  members = [
    "serviceAccount:${yandex_iam_service_account.runner-controller.id}"
  ]
}

resource "yandex_resourcemanager_folder_iam_binding" "controller-log-iam-folder-binding" {
  folder_id = var.folder_id
  role      = "logging.writer"
  members = [
    "serviceAccount:${yandex_iam_service_account.runner-controller.id}"
  ]

}


resource "yandex_compute_instance" "controller" {
  name        = "gh-runners-controller"
  platform_id = "standard-v3"
  zone        = var.yc_zone

  resources {
    cores         = 2
    memory        = 2
    core_fraction = 100
  }

  boot_disk {
    initialize_params {
      image_id = var.controller-image-id
    }
  }

  network_interface {
    subnet_id = var.subnet_id
    dns_record {
      fqdn = "gh-runners-controller.${var.dns_zone}."
      ptr  = true
    }
  }

  service_account_id = yandex_iam_service_account.runner-controller.id

  metadata = {
    serial-port-enable : "1"
    user-data = var.instance-metadata
  }
}


