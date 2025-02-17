output "registry" {
  # FIXME: cr.yandex hardcode
  value = "cr.yandex/${yandex_container_registry.registry.id}"
}


output "gh-runner-controller-ip" {
  value = module.gh-runner.controller-internal-ip
}

output "gh-gw-external-ip" {
  value = yandex_compute_instance.gw.network_interface[0].nat_ip_address
}

output "ansible-vault-secret-id" {
  value = yandex_lockbox_secret.ansible-vault.id
}
