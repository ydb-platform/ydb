resource "yandex_compute_disk" "cachesrv-ccache" {
  name = "cachesrv-ccache"
  zone = var.yc_zone
  type = "network-ssd-nonreplicated"
  size = 2 * 93
}

resource "yandex_compute_disk" "cachesrv-ya" {
  name = "cachesrv-ya"
  zone = var.yc_zone
  type = "network-ssd-nonreplicated"
  size = 45 * 93
}

resource "yandex_vpc_address" "cachesrv" {
  name                = "cachesrv external ip"
  deletion_protection = true

  external_ipv4_address {
    zone_id = var.yc_zone
  }
}

resource "yandex_compute_instance" "cachesrv" {
  name        = "cachesrv"
  platform_id = "standard-v3"
  zone        = var.yc_zone

  resources {
    cores         = 32
    memory        = 96
    core_fraction = 100
  }

  boot_disk {
    initialize_params {
      type     = "network-ssd"
      size     = 64
      image_id = var.cachesrv-image-id
    }
  }
  secondary_disk {
    disk_id     = yandex_compute_disk.cachesrv-ccache.id
    device_name = "ccache"
  }

  secondary_disk {
    disk_id     = yandex_compute_disk.cachesrv-ya.id
    device_name = "ya-cache"
  }

  network_interface {
    subnet_id      = yandex_vpc_subnet.default[var.yc_zone].id
    nat            = true
    nat_ip_address = yandex_vpc_address.cachesrv.external_ipv4_address[0].address
    dns_record {
      fqdn = "cachesrv.${var.dns_zone_fqdn}."
      ptr  = true
    }
  }

  metadata = {
    serial-port-enable : "1"
    user-data = local.instance-metadata
  }
}
