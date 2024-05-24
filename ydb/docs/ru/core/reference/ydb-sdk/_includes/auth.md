# Аутентификация в SDK

Как описано в статье о [подключении к серверу {{ ydb-short-name }}](../../../concepts/connect.md), клиент с каждым запросом должен отправить [аутентификационный токен](../../../concepts/auth.md). Аутентификационный токен проверяется сервером и, в случае успешной аутентификации, запрос авторизуется и выполняется, иначе возвращается ошибка `Unauthenticated`.

{{ ydb-short-name }} SDK использует объект, отвечающий за генерацию таких токенов. SDK предоставляет встроенные cпособы получения такого объекта:

1. Методы с явной передачей параметров, каждый из методов реализует один из [режимов аутентификации](../../../concepts/auth.md).
2. Метод определения режима аутентификации и необходимых параметров из переменных окружения.

Обычно объект генерации токенов создается перед инициализацией драйвера {{ ydb-short-name }} и передается параметром в его конструктор. C++ и Go SDK дополнительно позволяют через один драйвер работать с несколькими БД и объектами генерации токенов.

Если объект генерации токенов не определен, драйвер не будет добавлять в запросы какой-либо аутентификационной информации. Такой подход позволяет успешно соединиться с локально развернутыми кластерами {{ ydb-short-name }} без настроенной обязательной аутентификации. Если обязательная аутентификация настроена, то запросы к базе данных без аутентификационного токена будут отклоняться с выдачей ошибки аутентификации.

## Методы создания объекта генерации токенов {#auth-provider}

На приведенные ниже методы можно кликнуть, чтобы перейти к исходному коду релевантного примера в репозитории. Также можно ознакомиться с [рецептами кода Аутентификации](../../../recipes/ydb-sdk/auth.md).

{% list tabs %}

- Python

  Режим | Метод
  ----- | -----
  Anonymous | [ydb.AnonymousCredentials()](https://github.com/yandex-cloud/ydb-python-sdk/tree/master/examples/anonymous-credentials) |
  Access Token | [ydb.AccessTokenCredentials( token )](https://github.com/yandex-cloud/ydb-python-sdk/tree/master/examples/access-token-credentials) |
  Metadata | [ydb.iam.MetadataUrlCredentials()](https://github.com/yandex-cloud/ydb-python-sdk/tree/master/examples/metadata-credentials)
  Service Account Key | [ydb.iam.ServiceAccountCredentials.from_file(</br>key_file, iam_endpoint=None, iam_channel_credentials=None )](https://github.com/yandex-cloud/ydb-python-sdk/tree/master/examples/service-account-credentials) |
  Определяется по переменным окружения | `ydb.credentials_from_env_variables()` |

- Go

  Режим | Пакет | Метод
  ----- | ----- | ----
  Anonymous | [ydb-go-sdk/v3](https://github.com/ydb-platform/ydb-go-sdk/)| [ydb.WithAnonymousCredentials()](https://github.com/ydb-platform/ydb-go-examples/tree/master/auth/anonymous_credentials)
  Access Token | [ydb-go-sdk/v3](https://github.com/ydb-platform/ydb-go-sdk/) | [ydb.WithAccessTokenCredentials(token)](https://github.com/ydb-platform/ydb-go-examples/tree/master/auth/access_token_credentials)
  Metadata | [ydb-go-yc](https://github.com/ydb-platform/ydb-go-yc/) | [yc.WithMetadataCredentials(ctx)](https://github.com/ydb-platform/ydb-go-examples/tree/master/auth/metadata_credentials)
  Service Account Key | [ydb-go-yc](https://github.com/ydb-platform/ydb-go-yc/) | [yc.WithServiceAccountKeyFileCredentials(key_file)](https://github.com/ydb-platform/ydb-go-examples/tree/master/auth/service_account_credentials)
  Static Credentials | [ydb-go-sdk/v3](https://github.com/ydb-platform/ydb-go-sdk/) | [ydb.WithStaticCredentials(user, password)](https://github.com/ydb-platform/ydb-go-examples/tree/master/auth/static_credentials)
  Определяется по переменным окружения | [ydb-go-sdk-auth-environ](https://github.com/ydb-platform/ydb-go-sdk-auth-environ/) | [environ.WithEnvironCredentials(ctx)](https://github.com/ydb-platform/ydb-go-examples/tree/master/auth/environ)

- Java

  Режим | Метод
  ----- | -----
  Anonymous | [tech.ydb.core.auth.NopAuthProvider.INSTANCE](https://github.com/ydb-platform/ydb-java-examples/tree/master/auth/anonymous_credentials) |
  Access Token | [new tech.ydb.core.auth.TokenAuthProvider(accessToken);](https://github.com/ydb-platform/ydb-java-examples/tree/master/auth/access_token_credentials) |
  Metadata | [tech.ydb.auth.iam.CloudAuthHelper.getMetadataAuthProvider();](https://github.com/ydb-platform/ydb-java-examples/tree/master/auth/metadata_credentials) |
  Service Account Key | [tech.ydb.auth.iam.CloudAuthHelper.getServiceAccountFileAuthProvider(saKeyFile);](https://github.com/ydb-platform/ydb-java-examples/tree/master/auth/service_account_credentials) |
  Определяется по переменным окружения | [tech.ydb.auth.iam.CloudAuthHelper.getAuthProviderFromEnviron();](https://github.com/ydb-platform/ydb-java-examples/tree/master/auth/environ) |

- Node.js

  Режим | Метод
  ----- | -----
  Anonymous | [AnonymousAuthService()](https://github.com/ydb-platform/ydb-nodejs-sdk/tree/main/examples/auth/anonymous-credentials)
  Access Token | [TokenAuthService( accessToken, database )](https://github.com/ydb-platform/ydb-nodejs-sdk/tree/main/examples/auth/access-token-credentials)
  Metadata | [MetadataAuthService( database )](https://github.com/ydb-platform/ydb-nodejs-sdk/tree/main/examples/auth/metadata-credentials)
  Service Account Key | [getSACredentialsFromJson( saKeyFile )](https://github.com/ydb-platform/ydb-nodejs-sdk/tree/main/examples/auth/service-account-credentials)
  Static Credentials | [StaticCredentialsAuthService( user, password, endpoint )](https://github.com/ydb-platform/ydb-nodejs-sdk/tree/main/examples/auth/static-credentials)
  Определяется по переменным окружения | [getCredentialsFromEnv( entryPoint, database, logger )](https://github.com/ydb-platform/ydb-nodejs-sdk/tree/main/examples/auth/environ)

- Rust

  Режим | Метод
  ----- | -----
  Anonymous | ydb::StaticToken("")
  Access Token | ydb::StaticToken(token)
  Metadata | ydb::GCEMetadata, ydb::YandexMetadata
  Service Account Key | не поддерживается
  Static Credentials | [ydb::StaticCredentialsAuth](https://github.com/ydb-platform/ydb-rs-sdk/blob/master/ydb/examples/auth-static-credentials.rs)
  Определяется по переменным окружения | не поддерживается
  Выполнение внешней команды | ydb.CommandLineYcToken (например, для авторизации с помощью [IAM-токена]{% if lang == "ru"%}(https://cloud.yandex.ru/docs/iam/concepts/authorization/iam-token){% endif %}{% if lang == "en" %}(https://cloud.yandex.com/en/docs/iam/concepts/authorization/iam-token){% endif %} {{ yandex-cloud }} с компьютера разработчика ```ydb::CommandLineYcToken.from_cmd("yc iam create-token")```)

- PHP

  Режим | Метод
  ----- | -----
  Anonymous | [AnonymousAuthentication()](https://github.com/ydb-platform/ydb-php-sdk#anonymous)
  Access Token | [AccessTokenAuthentication( $accessToken )](https://github.com/ydb-platform/ydb-php-sdk#access-token)
  Oauth Token | [OAuthTokenAuthentication( $oauthToken )](https://github.com/ydb-platform/ydb-php-sdk#oauth-token)
  Metadata | [MetadataAuthentication()](https://github.com/ydb-platform/ydb-php-sdk#metadata-url)
  Service Account Key | [JwtWithJsonAuthentication($jsonFilePath)](https://github.com/ydb-platform/ydb-php-sdk#jwt--json-file)  или [JwtWithPrivateKeyAuthentication( $key_id, $service_account_id, $privateKeyFile )](https://github.com/ydb-platform/ydb-php-sdk#jwt--private-key)
  Определяется по переменным окружения | [EnvironCredentials()](https://github.com/ydb-platform/ydb-php-sdk#determined-by-environment-variables)
  Static Credentials | [StaticAuthentication($user, $password)](https://github.com/ydb-platform/ydb-php-sdk#static-credentials)

{% endlist %}

## Порядок определения режима и параметров аутентификации из окружения {#env}

Выполняется следующий алгоритм, одинаковый для всех SDK:

1. Если задано значение переменной окружения `YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS`, то используется режим аутентификации **System Account Key**, а ключ загружается из файла, имя которого указано в данной переменной
2. Иначе, если задано значение переменной окружения `YDB_ANONYMOUS_CREDENTIALS`, равное 1, то используется анонимный режим аутентификации
3. Иначе, если задано значение переменной окружения `YDB_METADATA_CREDENTIALS`, равное 1, то используется режим аутентификации **Metadata**
4. Иначе, если задано значение переменной окружения `YDB_ACCESS_TOKEN_CREDENTIALS`, то используется режим аутентификации **Access token**, в который передается значение данной переменной
5. Иначе используется режим аутентификации **Metadata**

Наличие последним пунктом алгоритма выбора режима **Metadata** позволяет развернуть рабочее приложение на виртуальных машинах и в Cloud Functions {{ yandex-cloud }} без задания каких-либо переменных окружения.

## Особенности {{ ydb-short-name }} Python SDK v2 (устаревшая версия)

{% note warning %}

Поведение {{ ydb-short-name }} Python SDK v2 (устаревшая версия) отличается от описанного выше.

{% endnote %}

* Алгоритм работы функции `construct_credentials_from_environ()` {{ ydb-short-name }} Python SDK v2:
   - Если задано значение переменной окружения `USE_METADATA_CREDENTIALS`, равное 1, то используется режим аутентификации **Metadata**
   - Иначе, если задано значение переменной окружения `YDB_TOKEN`, то используется режим аутентификации **Access Token**, в который передается значение данной переменной
   - Иначе, если задано значение переменной окружения `SA_KEY_FILE`, то используется режим аутентификации **System Account Key**, а ключ загружается из файла, имя которого указано в данной переменной
   - Иначе в запросах не будет добавлена информация об аутентификации.
* В случае, если при инициализации драйвера не передан никакой объект, отвечающий за генерацию токенов, то применяется [общий порядок](#env) чтения значений переменных окружения.

