# Удаление профиля

В настоящее время профиль удаляется только в интерактивном режиме следующей командой:

``` bash
{{ ydb-cli }} config profile delete <profile_name>
```

, где `<profile_name>` - имя профиля.

YDB CLI запросит подтверждение удаления профиля:

``` text
Profile "<profile_name>" will be permanently removed. Continue? (y/n): 
```

Ответьте `y` (Да) чтобы профиль был удален.

## Пример {#example}

Удаление профиля `mydb1`:

```bash
$ {{ ydb-cli }} config profile delete mydb1
Profile "mydb1" will be permanently removed. Continue? (y/n): y
Profile "mydb1" was removed.
```

## Удаление профиля без интерактивного ввода {#non-interactive}

Хотя данный режим не поддерживается YDB CLI, при необходимости вы можете использовать перенаправление ввода операционной системы для автоматического ответа `y` на вопрос подтверждения удаления:

``` bash
echo y | {{ ydb-cli }} config profile delete my_profile
```

Работоспособность данного способа никак не гарантируется.