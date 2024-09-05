Зарегистрируйтесь в Google Cloud Console и [создайте](https://console.cloud.google.com/projectselector2/home) проект. Активируйте [платежный аккаунт](https://console.cloud.google.com/billing/manage) и пополните его средствами для запуска девяти ВМ. Рассчитать стоимость можно в [калькуляторе](https://cloud.google.com/products/calculator).

Настройте GCP CLI:

1. Актируйте [Compute Engine API](https://console.cloud.google.com/apis/api/compute.googleapis.com/metrics) и [Cloud DNS API](https://console.cloud.google.com/apis/api/dns.googleapis.com/metrics).
2. Скачайте и установите GCP CLI, следуя [этой](https://cloud.google.com/sdk/docs/install) инструкции.
3. Перейдите в поддиректорию `.../google-cloud-sdk/bin` и выполните команду `./gcloud compute regions list` для получения списка доступных регионов.
4. Выполните команду `./gcloud auth application-default login` для настройки профиля подключения.

Перейдите в поддиректорию `gcp` (находится в скаченном репозитории) и в файле `variables.tf` задайте актуальные значения следующим переменным:

1. `project` – название проекта, которое было задано в облачной консоли Google Cloud.
2. `region` – регион, где будет развернута инфраструктура.
3. `zones` – список зон доступности, в которых будут созданы подсети и ВМ.

Теперь, находясь в поддиректории `gcp`, можно выполнить последовательность следующих команд для установки провайдера, инициализации модулей и создания инфраструктуры:

1. `terraform init` – установка провайдера и инициализация модулей.
2. `terraform plan` – создание плана будущей инфраструктуры.
3. `terraform apply` (повторное выполнение) – создание ресурсов в облаке.

Далее используются команды `terraform plan`, `terraform apply` и `terraform destroy` (уничтожение созданной инфраструктуры).