# Удаление профиля

{% include [profile-list](profile-list.md) %}

Удалите профиль `example`:

```bash
{{ ydb-cli }} config profile delete example
```

Результат:

```text
Profile "example" will be permanently removed. Continue? (y/n): 
```

Подтвердите удаление. Результат:

```text
Profile "example" was removed.
```
