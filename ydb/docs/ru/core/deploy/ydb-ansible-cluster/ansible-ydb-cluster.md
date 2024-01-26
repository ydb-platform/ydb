# Развертывание кластера с помощью Ansible и Terraform

Инструкция описывает развёртывание кластера {{ ydb-short-name }} с помощью Terraform и его конфигурирование через Ansible. Кластер развертывается на девяти виртуальных машинах, имеет тип избыточности [mirror-3-dc](../../cluster/topology.md) (данные реплицируются в 3 зоны доступности, использующие 3 домена отказа (обычно стойки) внутри каждой зоны) и использует внутреннюю DNS-зону для обращения к нодам по FQDN (Fully Qualified Domain Name – полностью определённое доменное имя/полное имя домена).

**Для развертывания YDB кластера в облачной инфраструктуре потребуется**:
* Terraform версии v1.5.7 или выше установленный на локальной машине.
* Terraform провайдер Yandex Cloud. 
* AWS CLI для настройки подключения Terraform.
* Активный платежный аккаунт с положительным балансом, достаточным для работы десяти виртуальных машин.
* В случае использования Яндекс Облака потребуются повышенные лимиты на количество vCPU виртуальных машин, общий объём SSD-дисков и количество всех публичных IP-адресов в частном облаке.

Скачать Terraform проект для автоматического создания инфраструктуры в Яндекс облаке можно из GitHub репозитория [YDB](https://github.com/ydb-platform/ydb-terraform/), а Ansible проект скачать можно из [соседней директории](https://github.com/ydb-platform/ydb-ansible/tree/refactor-use-collections) репозитория.  


## Настройка Terraform { #terraform-setup }

{% include [terraform-install](./_includes/terraform-install.md) %}

## Создания инфраструктуры в Яндекс Облаке { #create-infra-yc }

{% include [yc-terraform-prepare](./_includes/yc-terraform-prepare.md) %}

## Установка YDB на виртуальные машины 

{% include [ansible-ydb-install](./_includes/ansible-ydb-install.md) %}


