resource "random_password" "ch-password" {
  length  = 16
  special = false
}

resource "yandex_mdb_clickhouse_cluster" "jobs" {
  name        = "gh-jobs"
  environment = "PRODUCTION"
  network_id  = var.network_id



  clickhouse {
    resources {
      resource_preset_id = "s3-c2-m8"
      disk_type_id       = "network-ssd"
      disk_size          = 128
    }
  }

  access {
    web_sql   = true
    data_lens = true
  }

  database {
    name = var.ch-dbname
  }

  host {
    type      = "CLICKHOUSE"
    zone      = var.yc_zone
    subnet_id = var.subnet_id
  }


  user {
    name = var.ch-username
    # FIXME: password leak via terraform state
    password = random_password.ch-password.result
    permission {
      database_name = var.ch-dbname
    }
  }
}

resource "yandex_lockbox_secret_version" "clickhouse" {
  secret_id = yandex_lockbox_secret.secrets.id

  entries {
    key        = "ch_fqdns"
    text_value = join(",", yandex_mdb_clickhouse_cluster.jobs.host[*].fqdn)
  }
  entries {
    key        = "ch_database"
    text_value = var.ch-dbname
  }
  entries {
    key        = "ch_username"
    text_value = var.ch-username
  }
  entries {
    key        = "ch_password"
    text_value = random_password.ch-password.result
  }
}
