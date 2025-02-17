#!/bin/bash

#function create_secret {
#  TF_VAR_cloud_id=b1ggceeul2pkher8vhb6 \
#  TF_VAR_folder_id=b1grf3mpoatgflnlavjd \
#  TF_VAR_instance=ydbtech \
#  TF_VAR_yc_endpoint="api.cloud.yandex.net:443" \
#  TF_VAR_yc_storage_endpoint="storage.yandexcloud.net:443" \
#  terraform apply
#}

yc --profile ydbtech --endpoint api.cloud.yandex.net:443 \
  --folder-id b1grf3mpoatgflnlavjd --cloud-id b1ggceeul2pkher8vhb6 \
    lockbox payload get --key config e6q75n7s571uk3f6oemc > backend-configuration.tf
