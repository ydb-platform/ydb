Пример:

```bash
pssh run 'sudo systemctl restart kikimr; sleep 10' L@hosts.txt
```

Если системные таблетки живут в отдельных процессах для системных таблеток, то перезапускать нужно только эти процессы с помощью отдельного systemd-юнита:

```bash
pssh run 'sudo systemctl restart kikimr@sys-tablets; sleep 10' L@hosts.txt
```
