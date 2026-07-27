# client_certificate_authorization

Секция `client_certificate_authorization` задаёт правила проверки клиентских SSL-сертификатов и формирования [SID](../../concepts/glossary.md#access-sid) пользователей. Настройки указываются в [статической конфигурации](./index.md) кластера. Вложенный блок `client_certificate_definitions` задаёт требования к заполнению полей "Subject" и "Subject Alternative Name" клиентских сертификатов, а также список присваиваемых SID групп.

Клиентские сертификаты описываются стандартом [X.509](https://en.wikipedia.org/wiki/X.509). Поле "Subject" сертификата состоит из нескольких компонентов (например, `O` — организация, `OU` — подразделение в составе организации, `C` — страна, `CN` — имя собственное субъекта). Проверки могут быть настроены на соответствие одного или нескольких компонентов поля ожидаемым значениям.

Поле "Subject Alternative Name" сертификата представляет собой список сетевых имён или IP-адресов. Проверка может быть настроена на соответствие сетевых имён в сертификате ожидаемым значениям.

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
`request_client_certificate` | Запрос клиентского сертификата при TLS-handshake для gRPCs.<br/>Допустимые значения:<br/><ul><li>`true` — сервер запрашивает и проверяет клиентский сертификат на TLS-уровне. Если сертификат присутствует в TLS-контексте соединения и при этом не передан [аутентификационный токен](../../concepts/glossary.md#auth-token), сервер использует его для аутентификации запросов.</li><li>`false` — сервер не запрашивает клиентский сертификат при TLS-handshake, соответственно, аутентификация по сертификату при такой настройке недоступна.<br/>Значение по умолчанию: `false`.</li></ul>
`default_group` | SID, присваиваемый всем подключениям с доверенным клиентским сертификатом, если нет явно заданных настроек в секции `client_certificate_definitions`.<br/>Значение по умолчанию: `DefaultClientAuth@cert`.
`client_certificate_definitions` | Блок настроек требований к клиентским сертификатам.
`member_groups` | Массив SID групп, присваиваемых подключениям, сертификаты которых соответствуют требованиям этого блока.
`require_same_issuer` | Требовать совпадение поля "Issuer" (наименование центра регистрации) для клиентского и серверного сертификатов.<br/>Допустимые значения:<br/><ul><li>`true` — совпадение требуется (применяется по умолчанию, если параметр не задан);</li><li>`false` — совпадение не требуется (клиентский и серверный сертификаты могут быть выпущены разными центрами регистрации).</li></ul>
`subject_dns` | Допустимые значения поля "Subject Alternative Name" в виде массива полных значений (ключ `values`) или массива суффиксов значений (ключ `suffixes`). Проверка считается успешной при совпадении одного из значений поля с любым полным именем либо при соответствии любому указанному суффиксу.
`subject_terms` | Требования к заполнению компонентов поля "Subject". Указывается имя компонента (ключ `short_name`), а также набор полных значений (ключ `values`) или набор суффиксов значений (ключ `suffixes`). Проверка считается успешной, если для каждого проверяемого компонента поля "Subject" его значение соответствует одному из разрешённых полных значений либо одному из разрешённых суффиксов.

## Примеры

Следующий фрагмент конфигурации требует, чтобы в поле "Subject" клиентского сертификата были компоненты `O=YDB` и `CN=user1`. Для такого сертификата будет сформирован SID пользователя `O=YDB,CN=user1@cert` и будет назначена группа `group@cert`:

```yaml
client_certificate_authorization:
  request_client_certificate: true
  client_certificate_definitions:
    - member_groups: ["group@cert"]
      subject_terms:
      - short_name: "O"
        values: ["YDB"]
      - short_name: "CN"
        values: ["user1"]
```

В компоненте `CN` может указываться сетевое имя сервера, а не имя пользователя. Такой вариант целесообразно использовать при [регистрации динамических узлов](../../devops/deployment-options/manual/node-authorization.md#vklyuchenie-rezhima-autentifikacii-i-avtorizacii-uzlov). Следующий фрагмент конфигурации требует, чтобы в поле "Subject" клиентского сертификата узла были компоненты `O=YDB` и `CN=server1.internal.corp`. Для такого сертификата будет сформирован SID `O=YDB,CN=server1.internal.corp@cert` и будет назначена группа `registerNode@cert`:

```yaml
client_certificate_authorization:
  request_client_certificate: true
  client_certificate_definitions:
    - member_groups: ["registerNode@cert"]
      subject_terms:
      - short_name: "O"
        values: ["YDB"]
      - short_name: "CN"
        values: ["server1.internal.corp"]
```
