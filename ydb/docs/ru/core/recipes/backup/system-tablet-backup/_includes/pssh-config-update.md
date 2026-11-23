Скопируйте обновлённую конфигурацию во временный файл на каждом хосте:

```bash
pscp -h hosts.txt config.yaml /tmp/config.yaml.new
```

Сделайте резервную копию текущей конфигурации, замените её новой версией и защитите от случайных изменений:

```bash
pssh -h hosts.txt 'sudo cp /opt/ydb/cfg/config.yaml /opt/ydb/cfg/config.yaml.bak && sudo mv /tmp/config.yaml.new /opt/ydb/cfg/config.yaml && sudo chattr +i /opt/ydb/cfg/config.yaml'
```
