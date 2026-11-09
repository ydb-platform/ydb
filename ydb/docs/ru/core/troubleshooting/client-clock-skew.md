# Расхождение времени между клиентскими хостами и кластером

Сдвиг системных часов на хосте, где запущено клиентское приложение (SDK или CLI), может приводить к ошибкам аутентификации. Особенно это проявляется при выдаче токена через IAM по [ключу сервисного аккаунта](../security/authentication.md#iam) (**Service Account Key**), когда в токен попадает время с хоста.

Проверка Healthcheck [NODES_TIME_DIFFERENCE](../reference/ydb-sdk/health-check-api.md#nodes_time_difference) сравнивает системное время узлов кластера между собой. Время на хостах клиентских приложений она не проверяет. Поэтому если Healthcheck не показывает проблем, а ошибки аутентификации возникают только у части клиентов, проверьте время на хостах этих клиентов.

## Симптомы {#symptoms}

- **Ошибки при аутентификации по [ключу сервисного аккаунта](../security/authentication.md#iam)**. Клиент (SDK или CLI) формирует [JWT](https://www.rfc-editor.org/rfc/rfc7519) для запроса токена и указывает в нём текущее время по системным часам хоста. Если часы спешат или отстают, сервис IAM может отклонить запрос на выдачу токена.
- **Ошибки аутентификации при обновлении токена**: если SDK определяет момент обновления по локальным часам, при их отставании клиент может отправить в кластер уже истёкший токен.

Часто такие ошибки возникают только на отдельных хостах — например на новых машинах или в окружениях без настроенной синхронизации времени.

## Диагностика {#diagnostics}

### С помощью chrony

[chrony](https://chrony-project.org/documentation) — клиент NTP.

Чтобы проверить синхронизацию времени на клиентском хосте, выполните:

```bash
chronyc tracking
chronyc sources -v
```

В выводе `chronyc tracking` проверьте:

- `System time` — текущее смещение системных часов относительно NTP-часов `chrony` (в секундах).
- `Leap status` — статус [leap second](*leap-second) и синхронизации часов. В штатном состоянии — `Normal` (предупреждений нет). Значение `Not synchronised` означает, что `chrony` не считает часы синхронизированными.
- `Reference ID` — ID и имя (или IP) источника синхронизации. Если `Reference ID = 7F7F0101` и имя или IP не указаны, `chrony` работает в локальном режиме без внешней синхронизации.

{% cut "Пример вывода chronyc tracking" %}

```text
Reference ID    : CB00710F (ntp1.example.net)
Stratum         : 3
Ref time (UTC)  : Fri Jan 23 09:49:17 2026
System time     : 0.000006523 seconds slow of NTP time
Last offset     : -0.000006747 seconds
RMS offset      : 0.000035822 seconds
Frequency       : 3.225 ppm slow
Residual freq   : -0.000 ppm
Skew            : 0.129 ppm
Root delay      : 0.013639022 seconds
Root dispersion : 0.001100737 seconds
Update interval : 64.2 seconds
Leap status     : Normal
```

{% endcut %}

В выводе `chronyc sources -v` найдите источник со знаком `*` в столбце `S`: он выбран для синхронизации. Проверьте, что это ожидаемый источник времени: сравните его с источником в конфигурации `chrony` на этом хосте.

{% cut "Пример вывода chronyc sources -v" %}

```text
MS Name/IP address         Stratum Poll Reach LastRx Last sample
===============================================================================
#* GPS0                          0   4   377    11   -479ns[ -621ns] +/-  134ns
^? ntp1.example.net              2   6   377    23   -923us[ -924us] +/-   43ms
^+ ntp2.example.net              1   6   377    21  -2629us[-2619us] +/-   86ms
```

{% endcut %}

### С помощью timedatectl

[timedatectl](https://www.freedesktop.org/software/systemd/man/latest/timedatectl.html) — утилита systemd для статуса системных часов.

На хостах с systemd выполните:

```bash
timedatectl status
```

В выводе проверьте строки и их значения:

- `System clock synchronized: yes`
- `NTP service: active`

{% cut "Пример вывода timedatectl status" %}

```text
               Local time: Thu 2026-09-17 16:08:56 CEST
           Universal time: Thu 2026-09-17 14:08:56 UTC
                 RTC time: Thu 2026-09-17 14:08:56
                Time zone: Europe/Warsaw (CEST, +0200)
System clock synchronized: yes
              NTP service: active
          RTC in local TZ: no
```

{% endcut %}

{% note info %}

Если недоступны ни `chrony`, ни `timedatectl`, проверьте синхронизацию времени средствами NTP-клиента или платформы вашей среды. Например, при установленном `ntpd`:

```bash
ntpq -p
```

Команда показывает список NTP-серверов, к которым подключён клиент, и их статус. Источник со знаком `*` — текущий источник синхронизации (system peer).

{% endnote %}

## Рекомендации {#recommendations}

**Основное решение — синхронизировать часы.**

Настройте на клиентских хостах синхронизацию времени с помощью `chrony` или `ntpd` и нескольких NTP-источников из вашей среды. Примеры конфигурации см. в статье [{#T}](performance/system/system-clock-drift.md#ntp-examples).

{% include [example_clock_skew_internal](_includes/example_clock_skew_internal.md) %}

## Смотрите также

- [{#T}](performance/system/system-clock-drift.md)

[*leap-second]: Разовая корректировка UTC на ±1 секунду, чтобы согласовать шкалу времени с вращением Земли.
