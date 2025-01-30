resource "yandex_logging_group" "controller-logs" {
  folder_id        = var.folder_id
  name             = "gh-runners-controller-logs"
  retention_period = "168h"
}
