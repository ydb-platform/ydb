# Аутентификация при помощи OIDC

Аутентификация [OIDC](../../concepts/glossary.md#oidc) позволяет приложению использовать токен доступа внешнего провайдера идентификации для подключения к {{ ydb-short-name }}. Ниже приведены рецепты для SDK: получение токена по секрету приложения, передача готового токена и вход пользователя по коду устройства. Для каждого способа показано создание драйвера с выбранным провайдером учётных данных. Дополнительно приведены [полный пример приложения](#client-application) и [реализация кеша токенов](#token-cache).

Перед подключением проверьте [настройку IdP и сервера](../../reference/ydb-sdk/auth.md#oidc-prerequisites). Описание параметров и правила обновления токенов приведены в разделе [аутентификации OIDC в SDK](../../reference/ydb-sdk/auth.md#oidc). Базовое подключение описано в рецепте [инициализации драйвера](init.md), другие способы аутентификации перечислены в [обзоре](auth.md).

В примерах `connectionString` содержит эндпоинт и путь базы данных, например `grpcs://ydb.example.com:2135/?database=/Root/mydb`. Параметр `issuer` содержит точный HTTPS-адрес издателя, например `https://idp.example.com/realms/ydb`. Получите `clientId`, секрет приложения или готовый токен средствами вашего окружения. Значения секретов не следует хранить в исходном коде.

В примерах используется C++20. Функция `CreateOidcProviderFactory()` создаёт фабрику учётных данных выбранного режима, а `SetCredentialsProviderFactory()` подключает её к драйверу. `SetDiscoveryMode(NYdb::EDiscoveryMode::Async)` включает асинхронное обнаружение узлов базы данных; получение токенов у IdP выполняется отдельно.

{% list tabs %}

- C++

  {% list tabs %}

  - Секрет приложения

    ## Подключение с секретом приложения {#client-secret}

    Для сервиса или автоматического задания используйте режим `TClientOidcConfig`. Параметр `clientSecret` содержит сам секрет зарегистрированного в IdP приложения, а не путь к файлу.

    ```cpp
    #include <ydb-cpp-sdk/client/driver/driver.h>
    #include <ydb-cpp-sdk/client/types/credentials/oidc/credentials.h>

    NYdb::TDriver CreateDriverWithOidcClient(
        const std::string& connectionString,
        const std::string& issuer,
        const std::string& clientId,
        const std::string& clientSecret)
    {
        NYdb::NOidc::TOidcConfig oidc;
        oidc.Issuer = issuer;
        oidc.FlowConfig = NYdb::NOidc::TClientOidcConfig{
            .ClientId = clientId,
            .ClientSecret = clientSecret,
            .Scopes = {},
        };

        auto config = NYdb::TDriverConfig(connectionString)
            .SetDiscoveryMode(NYdb::EDiscoveryMode::Async)
            .SetCredentialsProviderFactory(
                NYdb::NOidc::CreateOidcProviderFactory(oidc));
        return NYdb::TDriver(config);
    }
    ```

    IdP должен разрешать `client_credentials` и аутентификацию приложения способом `client_secret_basic`. SDK автоматически добавляет область доступа `openid` даже при пустом списке `Scopes`. Если нужны дополнительные области, задайте каждую отдельной строкой в `Scopes`.

    Провайдер получает токен без участия пользователя и обновляет его по [правилам обновления токенов](../../reference/ydb-sdk/auth.md#oidc-refresh). Созданный драйвер можно передать клиенту запросов так же, как при других способах аутентификации.

  - Готовый токен

    ## Подключение с готовым токеном {#static-token}

    Если токен получен другим инструментом, используйте `TStaticOidcConfig`. Параметр `accessToken` содержит только значение токена, без префикса `Bearer` и разделяющего пробела. Необязательный `expiresAt` задаёт момент истечения срока действия токена типа `TInstant`.

    ```cpp
    #include <ydb-cpp-sdk/client/types/credentials/oidc/credentials.h>

    std::shared_ptr<NYdb::ICredentialsProviderFactory> MakeStaticFactory(
        const std::string& issuer,
        const std::string& accessToken,
        std::optional<TInstant> expiresAt = std::nullopt)
    {
        NYdb::NOidc::TOidcConfig oidc;
        oidc.Issuer = issuer;
        oidc.FlowConfig = NYdb::NOidc::TStaticOidcConfig{
            .AccessToken = accessToken,
            .ExpiresAt = expiresAt,
        };
        return NYdb::NOidc::CreateOidcProviderFactory(oidc);
    }
    ```

    Передайте созданную фабрику в конфигурацию драйвера:

    ```cpp
    #include <ydb-cpp-sdk/client/driver/driver.h>

    NYdb::TDriver CreateDriverWithOidcToken(
        const std::string& connectionString,
        const std::string& issuer,
        const std::string& accessToken,
        std::optional<TInstant> expiresAt = std::nullopt)
    {
        auto config = NYdb::TDriverConfig(connectionString)
            .SetDiscoveryMode(NYdb::EDiscoveryMode::Async)
            .SetCredentialsProviderFactory(
                MakeStaticFactory(issuer, accessToken, expiresAt));
        return NYdb::TDriver(config);
    }
    ```

    При отсутствии `expiresAt` SDK пытается извлечь `exp` из JWT. Этот режим не обращается к IdP и не обновляет токен автоматически. После замены токена создайте новую фабрику и драйвер с новыми данными. Параметр `issuer` обязателен и в этом режиме.

  - Код устройства

    ## Подключение по коду устройства {#device-code}

    Для входа пользователя используйте `TDeviceOidcConfig` и обработчик `IAuthAcceptor`, который покажет ссылку и код. Следующий обработчик пишет инструкции в стандартный поток ошибок, защищает вывод от одновременных вызовов и заменяет непечатные байты и байты вне диапазона ASCII символом `?`.

    {% include [oidc-console-acceptor](_includes/oidc-console-acceptor.md) %}

    Метод `Accept()` быстро возвращает управление: он не ждёт входа пользователя. SDK самостоятельно опрашивает IdP. Пользователь открывает показанную ссылку в браузере и подтверждает вход; браузер автоматически не запускается.

    После определения обработчика создайте фабрику:

    ```cpp
    #include <ydb-cpp-sdk/client/types/credentials/oidc/credentials.h>

    std::shared_ptr<NYdb::ICredentialsProviderFactory> MakeDeviceFactory(
        const std::string& issuer,
        const std::string& clientId)
    {
        NYdb::NOidc::TOidcConfig oidc;
        oidc.Issuer = issuer;
        oidc.FlowConfig = NYdb::NOidc::TDeviceOidcConfig{
            .ClientId = clientId,
            .Scopes = {"offline_access"},
        };
        oidc.Acceptor(std::make_shared<TConsoleAcceptor>());
        return NYdb::NOidc::CreateOidcProviderFactory(oidc);
    }
    ```

    Передайте созданную фабрику в конфигурацию драйвера:

    ```cpp
    #include <ydb-cpp-sdk/client/driver/driver.h>

    NYdb::TDriver CreateDriverWithOidcDevice(
        const std::string& connectionString,
        const std::string& issuer,
        const std::string& clientId)
    {
        auto config = NYdb::TDriverConfig(connectionString)
            .SetDiscoveryMode(NYdb::EDiscoveryMode::Async)
            .SetCredentialsProviderFactory(MakeDeviceFactory(issuer, clientId));
        return NYdb::TDriver(config);
    }
    ```

    Область `offline_access` запрашивается явно: оставьте её, если она поддерживается вашим IdP и нужна для выдачи токена обновления. `openid` SDK добавляет автоматически. Секрет приложения для этого режима не требуется.

    Выполнение первого запроса к базе может задержаться до подтверждения входа. Учитывайте это при настройке тайм-аута запроса. После отказа пользователя или истечения срока действия кода создайте новый драйвер с новой фабрикой для повторной попытки.

    В примере токены сохраняются только в памяти провайдера. Для повторного использования между запусками подключите собственный [кеш `ITokenCacher`](../../reference/ydb-sdk/auth.md#oidc-cacher) к конфигурации до создания фабрики.

  {% endlist %}

{% endlist %}

## Полный пример приложения с секретом {#client-application}

Программа в следующем примере получает секрет из выбранной приложением переменной `APP_OIDC_CLIENT_SECRET`, создаёт драйвер и выполняет запрос `SELECT 1`. Аргументы программы: эндпоинт и путь базы данных, `issuer` и `client_id`. Секрет не передаётся аргументом командной строки.

```cpp
#include <ydb-cpp-sdk/client/query/client.h>
#include <ydb-cpp-sdk/client/types/credentials/oidc/credentials.h>

#include <cstdlib>
#include <exception>
#include <iostream>

int main(int argc, char** argv) {
    if (argc != 5) {
        std::cerr << "Укажите эндпоинт, базу данных, issuer и client_id\n";
        return 1;
    }

    const char* secret = std::getenv("APP_OIDC_CLIENT_SECRET");
    if (secret == nullptr || *secret == '\0') {
        std::cerr << "Не задана переменная APP_OIDC_CLIENT_SECRET\n";
        return 1;
    }

    try {
        NYdb::NOidc::TOidcConfig oidc;
        oidc.Issuer = argv[3];
        oidc.FlowConfig = NYdb::NOidc::TClientOidcConfig{
            .ClientId = argv[4],
            .ClientSecret = secret,
            .Scopes = {},
        };

        auto factory = NYdb::NOidc::CreateOidcProviderFactory(oidc);
        auto config = NYdb::TDriverConfig(argv[1])
            .SetDatabase(argv[2])
            .SetDiscoveryMode(NYdb::EDiscoveryMode::Async)
            .SetCredentialsProviderFactory(factory);

        NYdb::TDriver driver(config);
        NYdb::NQuery::TQueryClient client(driver);
        auto result = client.ExecuteQuery(
            "SELECT 1", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        if (!result.IsSuccess()) {
            std::cerr << result.GetIssues().ToString() << '\n';
            return 1;
        }
        std::cout << "Запрос выполнен\n";
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
```

`SetDiscoveryMode(NYdb::EDiscoveryMode::Async)` включает асинхронное обнаружение узлов базы данных. Оно отличается от OIDC Discovery, которое получает адреса IdP. `TQueryClient` выполняет запрос, `TTxControl::NoTx()` не задаёт явную транзакцию и оставляет её обработку неявным правилам {{ ydb-short-name }}, а `ExtractValueSync()` ожидает результат. Подробнее об управлении транзакциями см. [{#T}](tx-control.md).

Программа использует синтаксис инициализации C++20. Например, после сборки в исполняемый файл `oidc-client` и передачи секрета через окружение её можно запустить так:

```bash
./oidc-client grpcs://ydb.example.com:2135 /Root/mydb \
  https://idp.example.com/realms/ydb ydb-service
```

В примере заданы условные адреса базы данных и IdP, а также идентификатор `ydb-service`; замените их параметрами вашей установки. Пустой список `Scopes` всё равно приводит к запросу `openid`. Получение и последующее обновление токена выполняются провайдером автоматически по [правилам обновления токенов](../../reference/ydb-sdk/auth.md#oidc-refresh).

## Кеш токенов в памяти {#token-cache}

Для повторного использования токенов внутри одного процесса можно реализовать кеш в памяти:

```cpp
#include <ydb-cpp-sdk/client/types/credentials/oidc/credentials.h>

#include <mutex>

class TMemoryTokenCacher final : public NYdb::NOidc::ITokenCacher {
public:
    std::optional<NYdb::NOidc::TTokenCache> Read() const override {
        std::lock_guard<std::mutex> guard(Mutex);
        return Tokens;
    }

    void Write(const NYdb::NOidc::TTokenCache& tokens) override {
        std::lock_guard<std::mutex> guard(Mutex);
        Tokens = tokens;
    }

private:
    mutable std::mutex Mutex;
    std::optional<NYdb::NOidc::TTokenCache> Tokens;
};
```

Перед созданием фабрики подключите один экземпляр кеша через `oidc.Cacher(std::make_shared<TMemoryTokenCacher>())`. Для совместного использования сохраните `shared_ptr` и передайте его нужным конфигурациям. Такой кеш не сохраняет токены после завершения процесса; для этого требуется собственное постоянное хранилище.

Требования к потокобезопасности и обработке ошибок описаны в [контракте `ITokenCacher`](../../reference/ydb-sdk/auth.md#oidc-cacher).
