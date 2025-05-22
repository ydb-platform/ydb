resource "yandex_iam_service_account" "gh-runner-webhook-sa" {
  name = "gh-runner-webhook-sa"
}

resource "yandex_container_registry_iam_binding" "gh-runner-iam-binding" {
  registry_id = var.registry_id
  role        = "container-registry.images.puller"
  members = [
    "serviceAccount:${yandex_iam_service_account.gh-runner-webhook-sa.id}"
  ]
}

resource "yandex_lockbox_secret_iam_binding" "gh-runner-iam-binding" {
  secret_id = yandex_lockbox_secret.secrets.id
  for_each  = toset(["lockbox.payloadViewer", "lockbox.viewer"])
  role      = each.value
  members = [
    "serviceAccount:${yandex_iam_service_account.gh-runner-webhook-sa.id}"
  ]
}

resource "yandex_lockbox_secret_iam_binding" "gh-runner-iam-github-binding" {
  secret_id = yandex_lockbox_secret.github-secrets.id
  for_each  = toset(["lockbox.payloadViewer", "lockbox.viewer"])
  role      = each.value
  members = [
    "serviceAccount:${yandex_iam_service_account.gh-runner-webhook-sa.id}"
  ]
}


resource "yandex_serverless_container" "gh-runner-webhook" {
  name          = "gh-runner-github-webhook"
  description   = "PUBLIC Container for receiving GitHub (https://github.com/ydb-platform/ydb) webhooks about Jobs"
  cores         = 1
  core_fraction = 100
  memory        = 256
  concurrency   = 4

  execution_timeout = "90s"

  service_account_id = yandex_iam_service_account.gh-runner-webhook-sa.id

  dynamic "secrets" {
    for_each = yandex_lockbox_secret_version.clickhouse.entries
    content {
      environment_variable = upper(secrets.value.key)
      id                   = yandex_lockbox_secret.secrets.id
      version_id           = yandex_lockbox_secret_version.clickhouse.id
      key                  = secrets.value.key
    }
  }
  secrets {
    environment_variable = "GH_WEBHOOK_SECRET"
    id                   = yandex_lockbox_secret.github-secrets.id
    key                  = "GH_WEBHOOK_SECRET"
    version_id           = data.yandex_lockbox_secret.github-secrets-version.current_version.0.id
  }

  connectivity {
    network_id = var.network_id
  }

  image {
    url = var.webhook_container_image
  }
}
