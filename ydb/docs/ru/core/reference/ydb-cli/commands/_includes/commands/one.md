## Получение детальной информации о подкомандах {{ ydb-short-name }} CLI {#one}

Для любой подкоманды также можно получить более подробное описание со списком доступных параметров:

```bash
{{ ydb-cli }} discovery whoami --help
```

Результат:

```text
Usage: ydb [global options...] discovery whoami [options...]

Description: Who am I?

Global options:
  {-e|--endpoint}, {-d|--database}, {-v|--verbose}, --ca-file, --iam-token-file, --yc-token-file, --use-metadata-credentials, --sa-key-file, --iam-endpoint, --profile, --license, --credits
  To get full description of these options run 'ydb --help'.

Options:
  {-?|-h|--help}      print usage
  --client-timeout ms Operation client timeout
  {-g|--groups}       With groups (default: 0)
```

Передаваемые параметры делятся на два типа:

* `Global options` — глобальные, указываются после `ydb`.
* `Options` — опции подкоманды, указываются после подкоманды.
