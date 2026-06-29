Снимите защиту от случайных изменений и замените новую конфигурацию исходной версией:

```bash
pssh run $'
sudo chattr -i /Berkanavt/kikimr/cfg/config.yaml &&
sudo mv /Berkanavt/kikimr/cfg/config.yaml.bak /Berkanavt/kikimr/cfg/config.yaml
' L@hosts.txt
```
