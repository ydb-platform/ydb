```t
[Unit]
Description=YDB storage node                        # Описание юнита.
After=network-online.target rc-local.service        # Запускать юнит после запуска указанных сервисов.
Wants=network-online.target                         # Для запуска юнита желателен запущенный сервис(сы).
StartLimitInterval=10                               # Интервал времени, за который должно произойти указанное количество сбоев, чтобы действие было выполнено.
StartLimitBurst=15                                  # Количество сбоев, при достижении которого будет выполнено действие.

[Service]
Restart=always                                      # Рестарт процесса в случае остановки.
RestartSec=1                                        # Время простоя в секундах, после которого systemd перезапускает службу процесс.
User=ydb                                            # Пользовать от чьего имени стартует процесс
PermissionsStartOnly=true                           # Перед стартом нужна специальная подготовка: создание папок, изменение прав и так далее.
SyslogIdentifier=ydbd                               # Идентификатор системного журнала
SyslogFacility=daemon                               # Источник данных лога. daemon – источник данных служба сервисов systemd.
SyslogLevel=err                                     # Уровень детализации журнала. err – будут записываться и отражаться все ошибки.
Environment=LD_LIBRARY_PATH={{ ydb_dir }}/release/{{ ydb_version }}/lib                 # Установка значения переменной окружения LD_LIBRARY_PATH 
ExecStart={{ ydb_dir }}/release/{{ ydb_version }}/bin/ydbd server \                     # Запуск процесса YDB с параметром server 
    --log-level 3 --syslog --tcp --yaml-config  {{ ydb_dir }}/cfg/dynamic_config.yaml \ # Передача конфигурационного файла динамической ноды
    --grpc-port {{ grpc_port | int +1 }} \                                              # Установка значения gRPC порта из переменной {{ grpc_port }} + 1
    --ic-port {{ ic_port | int +1 }} \                                                  # Установка значения IC порта из переменной {{ ic_port }} + 1
    --mon-port {{ mon_port | int +1 }} \                                                # Установка значения порта мониторинга из переменной {{ mon_port }} + 1
    --tenant /{{ ydb_domain }}/{{ ydb_dbname }} \                                       # Подключение к базе данных.
    --node-broker grpc://{{ inner_net }}:{{ grpc_port }}                                # Подключение к статической ноде.     
LimitNOFILE=65536                                                                       # Максимальное число открытых файлов
LimitCORE=0                                                                             # Макс. размер дампа в байтах, который процесс может сохранить. Если 0 – дамп не создаётся.
LimitMEMLOCK=3221225472                                                                 # Макс. выделенной оперативной памяти для процесса в байтах.

[Install]
WantedBy=multi-user.target                                                              # Уровень запуска сервиса. multi-user.target – многопользовательский режим без графики. 
```