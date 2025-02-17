resource "yandex_vpc_address" "gw" {
  name                = "gw"
  deletion_protection = true

  external_ipv4_address {
    zone_id = var.yc_zone
  }
}

resource "yandex_compute_instance" "gw" {
  name        = "gh-runners-gw"
  platform_id = "standard-v3"
  zone        = var.yc_zone

  resources {
    cores         = 2
    memory        = 1
    core_fraction = 20
  }

  boot_disk {
    initialize_params {
      image_id = var.gw-image
    }
  }

  network_interface {
    subnet_id      = yandex_vpc_subnet.default[var.yc_zone].id
    nat            = true
    nat_ip_address = yandex_vpc_address.gw.external_ipv4_address[0].address
  }

  metadata = {
    serial-port-enable : "1"
    user-data = local.instance-metadata
  }
}
