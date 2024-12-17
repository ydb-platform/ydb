resource "yandex_vpc_network" "default" {
  name      = "default"
  folder_id = var.folder_id
}


resource "yandex_vpc_subnet" "default" {
  for_each       = var.subnets
  v4_cidr_blocks = [each.value]
  zone           = each.key
  network_id     = yandex_vpc_network.default.id
  route_table_id = yandex_vpc_route_table.default.id
}


resource "yandex_vpc_gateway" "default" {
  name      = "default"
  folder_id = var.folder_id
  shared_egress_gateway {}
}


resource "yandex_vpc_route_table" "default" {
  name       = "default"
  network_id = var.network_id

  static_route {
    destination_prefix = "0.0.0.0/0"
    gateway_id         = yandex_vpc_gateway.default.id
  }
}
