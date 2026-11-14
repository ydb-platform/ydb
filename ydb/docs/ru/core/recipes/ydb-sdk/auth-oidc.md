# Аутентификация при помощи OIDC

В этой статье показано, как подключить приложение к {{ ydb-short-name }} через [OIDC](../../concepts/glossary.md#oidc) с помощью C++ SDK: по секрету приложения, с готовым токеном доступа или по коду устройства.

Базовое подключение описано в рецепте [инициализации драйвера](init.md), а другие способы аутентификации перечислены в [обзоре](auth.md).

Перед подключением проверьте [настройку IdP и сервера](../../reference/ydb-sdk/auth.md#oidc-prerequisites). Описание параметров и правила обновления токенов приведены в разделе [аутентификации OIDC в SDK](../../reference/ydb-sdk/auth.md#oidc).

Для каждого способа ниже показано создание драйвера с выбранным провайдером учётных данных. Дополнительно приведены [полный пример приложения](#client-application) и [реализация кеша токенов](#token-cache).

В примерах `connectionString` содержит эндпоинт и путь базы данных, например `grpcs://ydb.example.com:2135/?database=/Root/mydb`. Параметр `issuer` содержит точный HTTPS-адрес издателя, например `https://idp.example.com/realms/ydb`. Получите `clientId`, секрет приложения или готовый токен средствами вашего окружения. Значения секретов не следует хранить в исходном коде.

В примерах используется C++20. Функция `CreateOidcProviderFactory()` создаёт фабрику учётных данных выбранного режима, а `SetCredentialsProviderFactory()` подключает её к драйверу. `SetDiscoveryMode(NYdb::EDiscoveryMode::Async)` включает асинхронное обнаружение узлов базы данных; получение токенов у IdP выполняется отдельно.

## Подключение с секретом приложения {#client-secret}

Используйте этот способ, когда приложение подключается к базе данных от собственного имени без входа пользователя в браузере: например, серверный сервис или скрипт, запускаемый по расписанию. Задайте идентификатор и секрет зарегистрированного в IdP приложения в `TClientOidcConfig`. Параметр `clientSecret` содержит сам секрет зарегистрированного в IdP приложения, а не путь к файлу.

{% list tabs %}

- C++

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

{% endlist %}

## Подключение с готовым токеном {#static-token}

Используйте этот способ, если приложение уже получило токен доступа от другого инструмента или компонента и самостоятельно отвечает за его замену после истечения срока действия. Передайте готовый токен через `TStaticOidcConfig`. Параметр `accessToken` содержит только значение токена, без префикса `Bearer` и разделяющего пробела. Необязательный `expiresAt` задаёт момент истечения срока действия токена типа `TInstant`.

{% list tabs %}

- C++

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

{% endlist %}

## Подключение по коду устройства {#device-code}

Используйте этот способ, когда приложение должно подключиться от имени пользователя, который может подтвердить вход в браузере, в том числе на другом устройстве. Задайте `TDeviceOidcConfig` и обработчик `IAuthAcceptor`, который покажет ссылку и код.

{% list tabs %}

- C++

  Следующий обработчик пишет инструкции в стандартный поток ошибок, защищает вывод от одновременных вызовов и заменяет непечатные байты и байты вне диапазона ASCII символом `?`.

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

  В примере токены сохраняются только в памяти провайдера. Чтобы повторно использовать токены после завершения и нового запуска приложения, реализуйте [кеш](../../reference/ydb-sdk/auth.md#oidc-cacher) `ITokenCacher` с постоянным хранилищем и подключите его к конфигурации до создания фабрики. Если сохранённые токены пригодны для доступа или обновления, повторный вход в браузере не потребуется. [Кеш в памяти](#token-cache), приведённый ниже, токены между запусками приложения не сохраняет.

{% endlist %}

## Полный пример приложения с секретом {#client-application}

Программа в следующем примере читает секрет из переменной окружения `APP_OIDC_CLIENT_SECRET` вызовом `std::getenv()`, создаёт драйвер и выполняет запрос `SELECT 1`. Имя этой переменной задано в коде примера; SDK не читает её автоматически. Аргументы программы: эндпоинт и путь базы данных, `issuer` и `client_id`. Секрет не передаётся аргументом командной строки.

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

В режимах секрета приложения и кода устройства провайдер сам хранит полученные токены в памяти и обновляет их. Для запросов через один работающий провайдер дополнительный кеш не нужен.

Кеш `ITokenCacher` полезен, если приложение создаёт новый провайдер и хочет использовать токены, полученные предыдущим, вместо нового входа у IdP. Например, приложение может сохранить объект кеша при пересоздании фабрики и драйвера. SDK сам вызывает методы чтения и записи кеша; приложению не нужно извлекать и передавать токен при каждом запросе.

Ниже показан кеш в памяти. Он позволяет передать токены новому провайдеру в том же процессе, пока объект кеша существует:

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

Создайте объект кеша, сохраните его `shared_ptr` в приложении и передайте в `oidc.Cacher(...)` до создания фабрики. При пересоздании фабрики и драйвера передайте тот же объект в новую конфигурацию. Используйте его только для тех же настроек IdP, приложения и пользовательской сессии.

Такой кеш не сохраняет токены после завершения процесса. Чтобы повторно использовать их при следующем запуске приложения, нужна реализация `ITokenCacher` с постоянным хранилищем. Общий кеш не предотвращает одновременный вход нескольких провайдеров и не координирует обновление токенов между ними.

Требования к потокобезопасности и обработке ошибок описаны в [контракте](../../reference/ydb-sdk/auth.md#oidc-cacher) `ITokenCacher`.
