```yaml
ansible_python_interpreter: /usr/bin/python3           # Путь к интерпретатору Python на хосте.
ydb_unpack_options: "--strip-component=1"              # Опция распаковки архива YDB.
ydb_version: 23.3.13                                   # Версия YDB.
ydb_archive: ydbd-{{ ydb_version }}-linux-amd64.tar.gz # Шаблон названия архива YDB для скачивания.
ydb_download_url: https://binaries.ydb.tech/release/{{ ydb_version }}/{{ ydb_archive }} # Шаблон URL для скачивания архива YDB.
ydb_dir: /opt/ydb                                      # Директория для распаковки архива YDB 
ydb_domain: Root                                       # Название домена (кластера)
ydb_dbname: testdb                                     # Название тестовой базы данных
grpc_port: 2135                                        # Порт gRPC для статической ноды. Для динамической ноды берется значение порта gRPC +1.
ic_port: 19001                                         # Порт IC для статической ноды. Для динамической ноды берется значение порта IC +1.
mon_port: 8765                                         # Порт мониторинга для статической ноды. Для динамической ноды берется значение порта мониторинга +1.
inner_net: 127.0.0.1                                   # Адрес внутренней сети хоста
```