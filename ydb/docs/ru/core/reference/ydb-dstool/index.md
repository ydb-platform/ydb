# Обзор утилиты {{ ydb-short-name }} DSTool

С помощью утилиты {{ ydb-short-name }} DSTool вы можете управлять дисковой подсистемой кластера {{ ydb-short-name }}. Чтобы установить и настроить утилиту, выполните [инструкцию](install.md).

Утилита {{ ydb-short-name }} DSTool включает следующие команды:

Команда | Описание
--- | ---
[device list](device-list.md) | Вывести список устройств хранения.
pdisk add-by-serial | Добавить PDisk в набор по серийному номеру.
pdisk remove-by-serial | Удалить PDisk из набора по серийному номеру.
pdisk set | Задать параметры PDisk'а.
pdisk list | Вывести список PDisk'ов.
vdisk evict | Переместить VDisk'и на другие PDisk'и.
vdisk remove-donor | Удалить VDisk-донор.
vdisk wipe | Очистить VDisk'и
vdisk list | Вывести список VDisk'ов.
group add | Добавить группы хранения в пул.
group check | Проверить группы хранения.
group show blob-info | Вывести информацию о блобе.
group show usage-by-tablets | Вывести информацию об использовании таблеток группами.
group state | Вывести или изменить состояние группы хранения.
group take-snapshot | Сделать снапшот метаданных группы хранения.
group list | Вывести список групп хранения.
pool list | Вывести список пулов.
box list | Вывести список наборов PDisk'ов.
node list | Вывести список узлов.
cluster balance | Переместить VDisk'и с перегруженных PDisk'ов.
cluster get | Вывести параметры кластера.
cluster set | Задать параметры кластера.
cluster workload run | Создать рабочую нагрузку для тестирования модели отказа.
cluster list | Вывести информацию о кластере.
