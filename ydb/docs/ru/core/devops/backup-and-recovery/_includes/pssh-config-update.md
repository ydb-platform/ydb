Скопируйте обновлённую конфигурацию во временный файл на каждом хосте:

```bash
pssh scp config.yaml L@hosts.txt:/tmp/config.yaml.new
```

Сделайте резервную копию текущей конфигурации, замените её новой версией и защитите от случайных изменений:

```bash
pssh run $'
sudo cp /Berkanavt/kikimr/cfg/config.yaml /Berkanavt/kikimr/cfg/config.yaml.bak &&
sudo mv /tmp/config.yaml.new /Berkanavt/kikimr/cfg/config.yaml &&
sudo chattr +i /Berkanavt/kikimr/cfg/config.yaml
' L@hosts.txt
```
