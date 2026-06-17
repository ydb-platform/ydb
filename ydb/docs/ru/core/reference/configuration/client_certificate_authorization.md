# client_certificate_authorization

Секция `client_certificate_authorization` задаёт правила проверки клиентских SSL-сертификатов и назначения [SID](../../concepts/glossary.md#access-sid) пользователей. Настройки указываются в [статической конфигурации](./index.md) кластера.

Раздел `client_certificate_definitions` определяет требования к заполнению полей «Subject» и «Subject Alternative Name» клиентских сертификатов, а также список присваиваемых [SID](../../concepts/glossary.md#access-sid).

Поле «Subject» сертификата состоит из нескольких компонентов (например, `O` — организация, `OU` — подразделение в составе организации, `C` — страна, `CN` — имя собственное субъекта). Проверки могут быть настроены на соответствие одного или нескольких компонентов поля ожидаемым значениям.

Поле «Subject Alternative Name» сертификата представляет собой список сетевых имён или IP-адресов. Проверка может быть настроена на соответствие сетевых имён в сертификате ожидаемым значениям.

## Синтаксис

```yaml
client_certificate_authorization:
  request_client_certificate: Bool
  default_group: <SID по умолчанию>
  client_certificate_definitions:
    - member_groups: <массив SID>
      require_same_issuer: Bool
      subject_dns:
        suffixes: <массив разрешенных суффиксов>
        values: <массив допустимых значений>
      subject_terms:
      - short_name: <имя компонента Subject Name>
        suffixes: <массив разрешенных суффиксов>
        values: <массив допустимых значений>
    - member_groups: <массив SID>
    ...
```

Ключ | Описание
---- | ---
`request_client_certificate` | Запрос клиентского сертификата при TLS-handshake для gRPC (`grpcs://`).<br/>Допустимые значения:<br/><ul><li>`true` — сервер запрашивает и проверяет клиентский сертификат на TLS-уровне;</li><li>`false` — сервер не запрашивает клиентский сертификат на TLS-уровне; если сертификат передан клиентом и при этом не указан [аутентификационный токен](../../concepts/glossary.md#auth-token), то переданный сертификат будет использован для аутентификации запросов.</li></ul><br/>Значение по умолчанию: `false`.
`default_group` | SID, присваиваемый всем подключениям с доверенным клиентским сертификатом, если нет явно заданных настроек в секции `client_certificate_definitions`.<br/>Значение по умолчанию: `DefaultClientAuth@cert)`.
`client_certificate_definitions` | Блок настроек требований к клиентским сертификатам.
`member_groups` | Массив SID групп, присваиваемых подключениям, сертификаты которых соответствуют требованиям этого блока.
`require_same_issuer` | Требовать совпадение поля "Issuer" (наименование центра регистрации) для клиентского и серверного сертификатов.<br/>Допустимые значения:<br/><ul><li>`true` — совпадение требуется (применяется по умолчанию, если параметр не задан);</li><li>`false` — совпадение не требуется (клиентский и серверный сертификаты могут быть выпущены разными центрами регистрации).</li></ul>
`subject_dns` | Допустимые значения поля "Subject Alternative Name" в виде массива полных значений (ключ `values`) или массива суффиксов значений (ключ `suffixes`). Проверка считается успешной при совпадении одного из значений поля с любым полным именем либо при соответствии любому указанному суффиксу.
`subject_terms` | Требования к заполнению компонентов поля "Subject". Указывается имя компонента (ключ `short_name`), а также набор полных значений (ключ `values`) или набор суффиксов значений (ключ `suffixes`). Проверка считается успешной, если для каждого проверяемого компонента поля "Subject" его значение соответствует одному из разрешённых полных значений либо одному из разрешённых суффиксов.

## Примеры

Следующий фрагмент конфигурации требует клиентский сертификат с компонентом `O=YDB` в поле "Subject". Для сертификата с Subject `O=YDB` будет сформирован SID пользователя `O=YDB@cert`, и этот SID включится в группу `group@cert`:

```yaml
client_certificate_authorization:
  request_client_certificate: true
  client_certificate_definitions:
    - member_groups: ["group@cert"]
      subject_terms:
      - short_name: "O"
        values: ["YDB"]
```

Следующий фрагмент конфигурации требует клиентский сертификат с компонентами `OU=cluster1` и `O=YDB` в поле "Subject". Дополнительно проверяется, что поле "Subject Alternative Name" содержит имя хоста, заканчивающееся на суффикс `.cluster1.ydb.company.net`. Для сертификата с Subject `OU=cluster1, O=YDB` будет сформирован SID пользователя `OU=cluster1,O=YDB@cert`, и этот SID включится в группу `group@cert`:

```yaml
client_certificate_authorization:
  request_client_certificate: true
  client_certificate_definitions:
    - member_groups: ["group@cert"]
      subject_dns:
        suffixes: [".cluster1.ydb.company.net"]
      subject_terms:
      - short_name: "OU"
        values: ["cluster1"]
      - short_name: "O"
        values: ["YDB"]
```
