module "gh-runner" {
  source = "./gh-runner/"

  yc_endpoint = var.yc_endpoint
  cloud_id    = var.cloud_id
  folder_id   = var.folder_id
  yc_zone     = var.yc_zone
  network_id  = var.network_id
  subnet_id   = yandex_vpc_subnet.default[var.yc_zone].id
  dns_zone    = var.dns_zone_fqdn

  instance-metadata       = local.instance-metadata
  registry_id             = yandex_container_registry.registry.id
  webhook_container_image = var.webhook_container_image

}
