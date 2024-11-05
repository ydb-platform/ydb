variable "yc_endpoint" {
  type = string
}

variable "cloud_id" {
  type = string
}

variable "folder_id" {
  type = string
}

variable "yc_zone" {
  type = string
}

variable "registry_id" {
  type = string
}

variable "network_id" {
  type = string
}

variable "subnet_id" {
  type = string
}

variable "dns_zone" {
  type = string
}

variable "instance-metadata" {
  type = string
}

variable "ch-dbname" {
  type    = string
  default = "gh"
}

variable "ch-username" {
  type    = string
  default = "gh"
}

variable "controller-image-id" {
  type    = string
  # ubuntu-2204-lts-oslogin
  default = "fd8bu9gsckcm2351kqaq"
}

variable "webhook_container_image" {
  type = string
}

