Снимите защиту от случайных изменений и замените новую конфигурацию исходной версией:

```bash
pssh -h hosts.txt 'sudo chattr -i /opt/ydb/cfg/config.yaml && sudo mv /opt/ydb/cfg/config.yaml.bak /opt/ydb/cfg/config.yaml
'```
