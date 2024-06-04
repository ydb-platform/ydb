terraform {
  required_providers {
    yandex = {
      source  = "yandex-cloud/yandex"
      version = ">= 0.71.0"
    }
  }
}

provider "yandex" {
  endpoint  = var.yc_endpoint
  cloud_id  = var.cloud_id
  folder_id = var.folder_id
  zone      = var.yc_zone
}
