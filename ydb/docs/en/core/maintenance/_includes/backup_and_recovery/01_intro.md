---
title: "Резервное копирование Yandex Database (YDB)"
description: "В разделе описаны поддерживаемые способы создания резервных копий баз данных Yandex Database (YDB) и восстановления из созданных ранее резервных копий. YDB позволяет использовать для создания резервных копий csv-файлы на файловой системе и AWS S3-совместимые хранилища."
---
# Backups

This section describes supported methods for creating YDB database backups and restoring data from previously created backups. {{ ydb-short-name }} lets you use the following destinations to create backups:

* CSV files on the file system.
* AWS S3-compatible storage.

{% note warning "Влияние резервного копирования на производительность" %}

Backups may negatively affect the database interaction indicators. Queries may take longer to execute. Before performing a database backup under load in production databases, test the process in the testing environment.

{% endnote %}

