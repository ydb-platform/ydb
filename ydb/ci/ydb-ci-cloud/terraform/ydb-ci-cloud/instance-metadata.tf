data "yandex_lockbox_secret_version" "instance-metadata-ssh-keys" {
  secret_id  = var.ssh-keys-lockbox-secret-id
  version_id = var.ssh-keys-lockbox-version-id
}

locals {
  lockbox-instance-metadata-contents = {
    for k, v in data.yandex_lockbox_secret_version.instance-metadata-ssh-keys.entries : v.key => v.text_value
  }
  instance-metadata = <<EOT
#cloud-config
ssh_pwauth: no
users:
${chomp(lookup(local.lockbox-instance-metadata-contents, "users"))}
EOT
}
