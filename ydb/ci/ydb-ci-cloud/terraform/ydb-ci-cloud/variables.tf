variable "yc_endpoint" {
  type    = string
  default = "api.cloud.yandex.net:443"
}

variable "cloud_id" {
  type    = string
  default = "b1ggceeul2pkher8vhb6"
}

variable "folder_id" {
  type    = string
  default = "b1grf3mpoatgflnlavjd"
}

variable "yc_zone" {
  type    = string
  default = "ru-central1-d"
}

variable "network_id" {
  type    = string
  default = "enp8a7lb14c3120j198t"
}

variable "subnets" {
  type = map(string)

  default = {
    "ru-central1-a" : "10.128.0.0/24",
    "ru-central1-b" : "10.129.0.0/24",
    "ru-central1-c" : "10.130.0.0/24",
    "ru-central1-d" : "10.131.0.0/24",
  }
}

variable "dns_zone_fqdn" {
  type    = string
  default = "internal"
}

variable "cachesrv-image-id" {
  type = string
  # ubuntu-2204-lts-oslogin
  default = "fd8bu9gsckcm2351kqaq"
}


variable "gw-image" {
  type = string
  # ubuntu-2204-lts-oslogin
  default = "fd8bu9gsckcm2351kqaq"
  # FIXME: remove
  #  default = "fd8un8f40qgmlenpa0qb"
}

variable "ssh-keys-lockbox-secret-id" {
  type    = string
  default = "e6qfkc47f8bbjpau9l93"
}

variable "ssh-keys-lockbox-version-id" {
  type    = string
  default = "e6qai3jgd6e9dinvcmjj"
}

variable "webhook_container_image" {
  type    = string
  default = "cr.yandex/crp2lrlsrs36odlvd8dv/github-runner-scale-webhook:1"
}
