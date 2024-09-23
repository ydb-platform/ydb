output "controller-internal-ip" {
  value = yandex_compute_instance.controller.network_interface[0].ip_address
}
