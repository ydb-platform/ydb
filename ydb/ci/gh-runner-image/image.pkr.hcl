source "yandex" "this" {
  folder_id = var.folder_id
  zone      = var.zone_id
  subnet_id = var.subnet_id

  disk_size_gb = 10
  disk_type    = "network-ssd"

  image_name        = "gh-runner-ubuntu-2204-v${formatdate("YYYYMMDhhmmss", timestamp())}"
  image_family      = "gh-runner-ubuntu-2204"
  image_description = "ydb github actions runner image"
  image_pooled      = false

  source_image_family = "ubuntu-2204-lts"
  instance_cores      = 4
  instance_mem_gb     = 16

  platform_id   = "standard-v3"
  ssh_username  = "ubuntu"
  state_timeout = "10m"

  use_ipv4_nat = true
  metadata = {
    user-data : "#cloud-config\npackage_update: false\npackage_upgrade: false"
    serial-port-enable : "1"
  }

}

build {
  sources = [
    "source.yandex.this"
  ]

  provisioner "file" {
    content     = <<EOF
set -xe
apt-get update
# wait for unattended-upgrade is finished
apt-get -o DPkg::Lock::Timeout=600 -y --no-install-recommends dist-upgrade
apt-get -y install --no-install-recommends \
  antlr3 clang-12 clang-14 cmake docker.io git jq libaio-dev libaio1 libicu70 libidn11-dev libkrb5-3 \
  liblttng-ust1 lld-14 llvm-14 m4 make ninja-build parallel postgresql-client postgresql-client \
  python-is-python3 python3-pip s3cmd s3cmd zlib1g linux-tools-common linux-tools-generic

apt-get -y purge lxd-agent-loader snapd modemmanager
apt-get -y autoremove

pip3 install conan==1.59 pytest==7.1.3 pytest-timeout pytest-xdist==3.3.1 setproctitle==1.3.2 \
  grpcio grpcio-tools PyHamcrest tornado xmltodict pyarrow boto3 moto[server] psutil pygithub==2.3.0

(CCACHE_VERSION=4.8.1 OS_ARCH=$(uname -m);
  curl -s -L https://github.com/ccache/ccache/releases/download/v$CCACHE_VERSION/ccache-$CCACHE_VERSION-linux-$OS_ARCH.tar.xz \
    | tar -xJ -C /usr/local/bin/ --strip-components=1 --no-same-owner ccache-$CCACHE_VERSION-linux-$OS_ARCH/ccache
)

curl -fsSL https://deb.nodesource.com/setup_20.x | bash
apt-get install -y nodejs

npm install -g @testmo/testmo-cli
EOF
    destination = "/tmp/install-packages.sh"
  }

  provisioner "file" {
    content     = <<EOF
#!/bin/env/sh
set -xe

mkdir -p /opt/cache/actions-runner/latest

cd /opt/cache/actions-runner/latest
curl -O -L https://github.com/actions/runner/releases/download/v${var.github_runner_version}/actions-runner-linux-x64-${var.github_runner_version}.tar.gz
tar -xzf actions-runner-linux-x64-${var.github_runner_version}.tar.gz
rm actions-runner-linux-x64-${var.github_runner_version}.tar.gz
./bin/installdependencies.sh
EOF
    destination = "/tmp/install-agent.sh"
  }

  provisioner "file" {
    content = templatefile("conf/unified-agent-linux.yml", {
      FOLDER_ID = var.folder_id
    })
    destination = "/tmp/yc-vm.yml"
  }

  provisioner "file" {
    content     = <<EOF
#!/bin/env/sh
set -xe
curl -s -O "https://storage.yandexcloud.net/yc-unified-agent/releases/${var.unified_agent_version}/deb/${var.unified_agent_ubuntu_name}/yandex-unified-agent_${var.unified_agent_version}_amd64.deb"
dpkg -i yandex-unified-agent_${var.unified_agent_version}_amd64.deb

mv /tmp/yc-vm.yml /etc/yandex/unified_agent/conf.d/
chown unified_agent:unified_agent /etc/yandex/unified_agent/conf.d/yc-vm.yml
EOF
    destination = "/tmp/install-unified-agent.sh"
  }

  provisioner "shell" {
    inline = [
      "sudo bash /tmp/install-packages.sh",
      "sudo bash /tmp/install-agent.sh",
      "sudo bash /tmp/install-unified-agent.sh",
      "sudo rm /tmp/install-packages.sh /tmp/install-agent.sh /tmp/install-unified-agent.sh",
      "sudo time sync",
    ]
  }

}