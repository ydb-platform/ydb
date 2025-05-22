resource "yandex_lockbox_secret" "secrets" {
  name        = "gh-runner-secrets"
  description = "Secrets for Managed services, like Clickhouse"
}

resource "yandex_lockbox_secret" "github-secrets" {
  name        = "gh-runner-secrets-github-secrets"
  description = "Lockbox for GitHub Tokens and keys. All versions are updated by hands."
}

data "yandex_lockbox_secret" "github-secrets-version" {
  secret_id = yandex_lockbox_secret.github-secrets.id

  lifecycle {
    postcondition {
      condition     = contains(self.current_version[0].payload_entry_keys, "GH_WEBHOOK_SECRET")
      error_message = "You must add GH_WEBHOOK_SECRET to the gh-runner-secrets-github-secrets lockbox"
    }
  }
}
