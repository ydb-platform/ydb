# Загрузка Корпоративной СУБД Яндекса

[Корпоративная СУБД Яндекса](https://ydb.yandex.ru) — коммерческая СУБД на основе ядра {{ ydb-short-name }}.

## Информация об условиях использования Корпоративной СУБД Яндекса

Корпоративная СУБД Яндекса распространяется в соответствии с условиями лицензионного соглашения.

### Бесплатное использование

Корпоративная СУБД Яндекса (Продукт) может использоваться бесплатно [на условиях лицензионного соглашения](https://ясубд.рф/cond/) в следующих целях:

- для оценки и изучения свойств Продукта;
- в рамках процессов разработки и тестирования программного обеспечения, использующего или встраивающего функции Продукта;
- для обучения администрированию Продукта;
- для обучения особенностям разработки программного обеспечения, взаимодействующего с Продуктом.

### Коммерческое использование

Для коммерческого использования Корпоративной СУБД Яндекса необходимо приобретение лицензии, [текст лицензионного соглашения](https://ясубд.рф/cond-commercial/). Цены на лицензии доступны [по запросу](https://forms.yandex.ru/surveys/13735628.a5bd9c7417fe06c03f7130d8863bed569e373119/).

## Скачивание дистрибутивов

Дистрибутивы Корпоративной СУБД Яндекса доступны для скачивания по приведенным ниже ссылкам.

### Linux

{{ ydb-short-name }} Enterprise Server (`ydbd`) — исполняемый файл для запуска узла Корпоративной СУБД Яндекса.

Версия |  Дата выпуска | Скачать | Список изменений
:--- | :--- | :--- | :---
**v24.3**
v.24.3.13  | 05.12.2024 | `cr.yandex/crptqonuodf51kdj7a7d/ydb:24.3.11.13` | [См. список](../../../changelog-server.md#24-3)
**v24.2**
v.24.2.7  | 20.08.2024 | `cr.yandex/crptqonuodf51kdj7a7d/ydb:24.2.7` | [См. список](../../../changelog-server.md#24-2)
**v24.1**
v.24.1.18 | 31.07.2024 | `cr.yandex/crptqonuodf51kdj7a7d/ydb:24.1.18` | [См. список](../../../changelog-server.md#24-1)
**v23.4**
v.23.4.11 | 14.05.24 | `cr.yandex/crptqonuodf51kdj7a7d/ydb:23.4.11` | [См. список](../../../changelog-server.md#23-4)
**v23.3**
v.23.3.17 | 14.12.23 | `cr.yandex/crptqonuodf51kdj7a7d/ydb:23.3.17` | [См. список](../../../changelog-server-23.md#23-3-17)
v.23.3.13 | 12.10.23 | `cr.yandex/crptqonuodf51kdj7a7d/ydb:23.3.13` | [См. список](../../../changelog-server.md#23-3)

### Docker

Версия |  Дата выпуска | Скачать | Список изменений
:--- | :--- | :--- | :---
**v24.3**
v.24.3.13  | 05.12.2024 | `cr.yandex/crptqonuodf51kdj7a7d/ydb:24.3.11.13` | [См. список](../../../changelog-server.md#24-3)
**v24.2**
v.24.2.7  | 20.08.2024 | `cr.yandex/crptqonuodf51kdj7a7d/ydb:24.2.7` | [См. список](../../../changelog-server.md#24-2)
**v24.1**
v.24.1.18 | 31.07.2024 | `cr.yandex/crptqonuodf51kdj7a7d/ydb:24.1.18` | [См. список](../../../changelog-server.md#24-1)
**v23.4**
v.23.4.11 | 14.05.24 | `cr.yandex/crptqonuodf51kdj7a7d/ydb:23.4.11` | [См. список](../../../changelog-server.md#23-4)
**v23.3**
v.23.3.17 | 14.12.23 | `cr.yandex/crptqonuodf51kdj7a7d/ydb:23.3.17` | [См. список](../../../changelog-server-23.md#23-3-17)
v.23.3.13 | 12.10.23 | `cr.yandex/crptqonuodf51kdj7a7d/ydb:23.3.13` | [См. список](../../../changelog-server.md#23-3)


## Сценарии для установки серверной части с помощью Ansible {#ydb-ansible}

Набор сценариев для установки и сопровождения серверной части Корпоративной СУБД Яндекса с помощью инструмента [Ansible](https://docs.ansible.com/) доступен для скачивания по ссылкам ниже.

| Версия | Дата выпуска | Скачать | Контрольные суммы |
| ------ | ------------ | ------- | ----------------- |
| v0.14   | 30.11.2024   | [ydb-ansible-0.14.zip](https://binaries.ясубд.рф/ansible/ydb-ansible-0.14.zip) | [ydb-ansible-0.14.txt](https://binaries.ясубд.рф/ansible/ydb-ansible-0.14.txt) |
| v0.10   | 01.08.2024   | [ydb-ansible-0.10.zip](https://binaries.ясубд.рф/ansible/ydb-ansible-0.10.zip) | [ydb-ansible-0.10.txt](https://binaries.ясубд.рф/ansible/ydb-ansible-0.10.txt) |
| v0.9   | 20.07.2024   | [ydb-ansible-0.9.zip](https://binaries.ясубд.рф/ansible/ydb-ansible-0.9.zip) | [ydb-ansible-0.9.txt](https://binaries.ясубд.рф/ansible/ydb-ansible-0.9.txt) |
| v0.8   | 10.07.2024   | [ydb-ansible-0.8.zip](https://binaries.ясубд.рф/ansible/ydb-ansible-0.8.zip) | [ydb-ansible-0.7.txt](https://binaries.ясубд.рф/ansible/ydb-ansible-0.8.txt) |
| v0.7   | 03.05.2024   | [ydb-ansible-0.7.zip](https://binaries.ясубд.рф/ansible/ydb-ansible-0.7.zip) | [ydb-ansible-0.7.txt](https://binaries.ясубд.рф/ansible/ydb-ansible-0.7.txt) |