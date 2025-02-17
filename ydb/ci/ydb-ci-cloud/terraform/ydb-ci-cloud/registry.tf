resource "yandex_container_registry" "registry" {
  name      = "registry"
  folder_id = var.folder_id
}
