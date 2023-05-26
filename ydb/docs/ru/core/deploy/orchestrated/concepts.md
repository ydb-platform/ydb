# Обзор

Развертывание {{ ydb-short-name }} в {{ k8s }} — это простой и доступный способ установки {{ ydb-short-name }}. С {{ k8s }} вы можете использовать универсальный подход к управлению приложением в любом облачном провайдере. Это руководство содержит инструкции для развертывания {{ ydb-short-name }} в [{{ managed-k8s-full-name}}](yc_managed_kubernetes.md) и [AWS EKS](aws_eks.md).

{{ ydb-short-name }} поставляется в виде Helm-чарта — пакета, который содержит шаблоны структур {{ k8s }}. Чарт может быть развернут в следующем окружении:

1. Установлен менеджер пакетов Helm версии старше 3.1.0.
1. Используется {{ k8s }} версии 1.20 и старше.
1. Установлена утилита kubectl и настроен доступ к кластеру.
1. Поддерживается динамическое предоставление томов в кластере ([Dynamic Volume Provisioning](https://kubernetes.io/docs/concepts/storage/dynamic-provisioning/)).

Подробнее о Helm читайте в [документации]{% if lang == "en" %}(https://helm.sh/docs/){% endif %}{% if lang == "ru" %}(https://helm.sh/ru/docs/){% endif %}.

Helm-чарт устанавливает в кластер {{ k8s }} контроллер, который реализует необходимую логику для развертывания компонентов {{ ydb-short-name }}. Контроллер построен по паттерну [Оператор](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/).

{{ydb-short-name}} состоит из двух компонентов:

* _storage nodes_ — обеспечивают слой хранения данных;
* _dynamic nodes_ — реализуют доступ к данным и их обработку.

Для развертывания каждого из компонентов достаточно создать соответствующий ресурс: [Storage](https://github.com/ydb-platform/ydb-kubernetes-operator/tree/master/samples/storage-block-4-2.yaml) или [Database](https://github.com/ydb-platform/ydb-kubernetes-operator/tree/master/samples/database.yaml) с желаемыми параметрами. Со схемой ресурсов можно ознакомиться [здесь](https://github.com/ydb-platform/ydb-kubernetes-operator/tree/master/deploy/ydb-operator/crds).

После обработки чарта контроллером будут созданы следующие ресурсы:

* [StatefulSet](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/) — контроллер рабочей нагрузки, который предоставляет предсказуемые сетевые имена и дисковые ресурсы для каждого контейнера.
* [Service](https://kubernetes.io/docs/concepts/services-networking/service/) для доступа к созданным базам данных из приложений.
* [ConfigMap](https://kubernetes.io/docs/concepts/configuration/configmap/) для хранения конфигурации кластера.

Ознакомиться с исходным кодом контроллера можно [здесь](https://github.com/ydb-platform/ydb-kubernetes-operator), Helm-чарт расположен в папке `deploy`.
При разворачивании контейнеров {{ydb-short-name}} используются образы `cr.yandex/yc/ydb`, на данный момент доступные только как предсобранные артефакты.