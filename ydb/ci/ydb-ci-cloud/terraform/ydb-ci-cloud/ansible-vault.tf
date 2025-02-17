resource "random_password" "ansible-vault" {
  length  = 16
  special = false
}

resource "yandex_lockbox_secret" "ansible-vault" {
  name        = "ansible-vault"
  description = "ansible-vault key"
}


resource "yandex_lockbox_secret_version" "ansible-vault" {
  secret_id = yandex_lockbox_secret.ansible-vault.id
  entries {
    key        = "key"
    text_value = random_password.ansible-vault.result
  }

}