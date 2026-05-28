# Аутентификация SQS API

SQS API для топиков поддерживает два способа авторизации:

- **OAuth-токен (user token)**.
- **IAM (AWS Signature V4)** — запрос подписывается алгоритмом AWS Signature Version 4 (как в AWS CLI).

## OAuth

### Настройка AWS CLI (OAuth)

AWS CLI всегда подписывает запросы. Для использования OAuth-токена в SQS API {{ ydb-short-name }} нужно:

- задать любые непустые значения `AWS_ACCESS_KEY_ID` и `AWS_SECRET_ACCESS_KEY` (они используются только для формирования подписи запроса);
- передать OAuth-токен как `AWS_SESSION_TOKEN`.

Пример:

```shell
export AWS_ACCESS_KEY_ID="dummy"
export AWS_SECRET_ACCESS_KEY="dummy"
export AWS_SESSION_TOKEN="<OAUTH_TOKEN>"
```

Также можно настроить профиль в файле `~/.aws/credentials`:

```ini
[default]
aws_access_key_id = dummy
aws_secret_access_key = dummy
aws_session_token = <OAUTH_TOKEN>
```

## IAM

Для IAM-аутентификации необходимо подписывать запросы по AWS Signature V4 с использованием пары ключей доступа:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`

### Настройка AWS CLI (IAM)

Пример настройки через переменные окружения:

```shell
export AWS_ACCESS_KEY_ID="<ACCESS_KEY_ID>"
export AWS_SECRET_ACCESS_KEY="<SECRET_ACCESS_KEY>"
unset AWS_SESSION_TOKEN
```

Либо через файл `~/.aws/credentials`:

```ini
[default]
aws_access_key_id = <ACCESS_KEY_ID>
aws_secret_access_key = <SECRET_ACCESS_KEY>
```

Либо через профиль AWS CLI:

```shell
aws configure set aws_access_key_id "<ACCESS_KEY_ID>"
aws configure set aws_secret_access_key "<SECRET_ACCESS_KEY>"
```
