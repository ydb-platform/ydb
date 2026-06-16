# Подключение AI-ассистента в YDB EM

В этой инструкции описано, как включить AI-ассистента в {{ ydb-short-name }} Enterprise Manager (YDB EM). После настройки пользователи увидят ассистента в веб-интерфейсе YDB EM. Ассистент отправляет запросы к модели через Gateway и может использовать MCP-инструменты Gateway.

## Перед началом {#before-start}

Эту инструкцию можно использовать до первого развёртывания YDB EM или для уже работающей установки. При первом развёртывании добавьте переменные в inventory до запуска initial setup playbook. Инструкцию по развёртыванию см. в разделе [{#T}](initial-deployment.md).

Убедитесь, что у вас есть:

1. Доступ к Ansible inventory, который используется для развёртывания YDB EM.
1. OpenAI-compatible endpoint модели, который будет доступен с хоста Gateway.
1. Способ добавить именованную запись с учётными данными модели в token-файл Gateway. Tokenator в Gateway читает этот файл и использует запись, у которой поле `Name` равно `ydb_em_ai_model_token_name`, например `model-token`, как upstream-заголовок `Authorization`.
1. Если вы обновляете существующую установку, доступ к развёрнутому хосту Gateway.

{% note warning %}

Не помещайте API-ключи модели, OAuth-токены и другие секреты в `ydb_em_ai_assistant_client_runtime_config`. Gateway возвращает это значение в браузер через `GET /meta/ai_assistant_client_config`.

{% endnote %}

## Как это работает {#how-it-works}

Браузер работает с ассистентом через Gateway:

1. UI проверяет `GET /capabilities`. Кнопка ассистента отображается, если `Settings.Proxy.Model` равно `true` и включена пользовательская настройка AI-ассистента. Когда backend capability появляется впервые, YDB EM инициализирует эту пользовательскую настройку включённой.
1. UI получает runtime-настройки из `GET /meta/ai_assistant_client_config`.
1. UI отправляет запросы к модели в `/proxy/model/...`.
1. Gateway перенаправляет запросы к модели в `ydb_em_ai_model_endpoint` и добавляет заголовок Authorization из tokenator.
1. UI отправляет MCP-запросы в `/meta/mcp`. Gateway выполняет MCP-инструменты через API YDB EM и proxy endpoints кластеров.

## Настройте доступ к модели {#configure-model-access}

`ydb_em_ai_model_token_name` — это имя записи tokenator, которую Gateway использует для запросов к модели. Это не raw token. В token-файле это поле `Name` записи, у которой значение `Token` содержит полный HTTP-заголовок `Authorization`, ожидаемый endpoint модели.

Пример записи tokenator:

```textproto
StaticTokenInfo {
  Name: "model-token"
  Token: "Bearer <model-api-token>"
}
```

Способ доставки токена зависит от вашего процесса развёртывания. Главное требование: tokenator-файл Gateway содержит запись, у которой `Name` совпадает с `ydb_em_ai_model_token_name`.

{% note info %}

Стандартный Ansible-шаблон токенов YDB EM генерирует только запись `meta` для доступа к metabase. Если в вашей установке используется этот шаблон без изменений, расширьте или переопределите tokenator-файл через ваш процесс развёртывания, чтобы в нём также были `model-token` и, если он отличается, токен для embeddings.

{% endnote %}

## Настройте Ansible-переменные {#configure-ansible-variables}

Добавьте переменные AI-ассистента в inventory YDB EM, например в `examples/inventory/50-inventory.yaml` или другой inventory-файл вашего развёртывания.

```yaml
ydb_em_ai_assistant_enabled: true
ydb_em_ai_model_endpoint: "https://llm.example.com/"
ydb_em_ai_model_token_name: "model-token"

ydb_em_ai_assistant_client_runtime_config:
  llm:
    baseURL: "/proxy/model/v1"
    model: "<model-name>"
    temperature: 0.2
  mcp:
    - id: "ydb-meta"
      url: "/meta/mcp"
      transport: "streamable"
      toolCallTimeoutMs: 600000
```

Параметр | Описание
--- | ---
`ydb_em_ai_assistant_enabled` | Включает настройки AI-ассистента в Gateway.
`ydb_em_ai_model_endpoint` | Upstream model API endpoint, который использует Gateway.
`ydb_em_ai_model_token_name` | Имя записи tokenator для запросов к модели.
`ydb_em_ai_assistant_client_runtime_config.llm.baseURL` | URL модели, который видит браузер. Используйте Gateway proxy, обычно `/proxy/model/v1`.
`ydb_em_ai_assistant_client_runtime_config.llm.model` | Имя модели, которое отправляется в OpenAI-compatible API.
`ydb_em_ai_assistant_client_runtime_config.mcp` | MCP-серверы, доступные ассистенту. Для YDB EM используйте `/meta/mcp`.

Gateway добавляет путь из `/proxy/model/...` к `ydb_em_ai_model_endpoint`. При `baseURL: "/proxy/model/v1"` запрос chat completions будет перенаправлен в `/v1/chat/completions` на upstream endpoint. Настройте endpoint так, чтобы этот путь был корректен для вашего provider.

## Настройте поиск по документации {#configure-docs-search}

Этот шаг необязателен. Включайте его только если ассистенту нужен MCP-инструмент `search_docs`. Для этого инструмента Gateway вызывает OpenAI-compatible embeddings endpoint и добавляет `/embeddings` к настроенному base URL, если suffix отсутствует.

```yaml
ydb_em_docs_search_enabled: true
ydb_em_docs_search_embeddings_upstream_base_url: "https://llm.example.com/v1"
ydb_em_docs_search_embeddings_token_name: "model-token"
ydb_em_docs_search_embeddings_model: "<embeddings-model-name>"
ydb_em_docs_search_vector_size: 1536
ydb_em_docs_search_limit: 6
ydb_em_docs_search_score: 0.6
```

Параметр | Описание
--- | ---
`ydb_em_docs_search_enabled` | Настраивает backend семантического поиска по документации и публикует `search_docs` через MCP, если все обязательные настройки поиска валидны.
`ydb_em_docs_search_embeddings_upstream_base_url` | Base URL OpenAI-compatible embeddings endpoint.
`ydb_em_docs_search_embeddings_token_name` | Имя записи tokenator для embeddings-запросов.
`ydb_em_docs_search_embeddings_model` | Имя embeddings-модели.
`ydb_em_docs_search_vector_size` | Ожидаемый размер vector. Используйте `0`, чтобы отключить проверку размера.
`ydb_em_docs_search_limit` | Максимальное количество возвращаемых документов.
`ydb_em_docs_search_score` | Минимальный score поиска от `0` до `1`.

## Примените конфигурацию {#apply-configuration}

После обновления inventory запустите playbook YDB EM из каталога с вашим inventory. Один и тот же playbook используется для первого развёртывания и для применения этого изменения к существующей установке:

```bash
ansible-playbook ydb_platform.ydb_em.initial_setup
```

Если vault-файл зашифрован, добавьте vault-параметр, который используется в вашем развёртывании:

```bash
ansible-playbook ydb_platform.ydb_em.initial_setup --ask-vault-pass
```

Роль сгенерирует конфигурацию и стандартный token-файл Gateway и убедится, что сервис Gateway запущен. Если дополнительные учётные данные модели доставляются через переопределённый token-файл или отдельный шаг развёртывания, убедитесь, что развёрнутый token-файл содержит настроенное имя записи. Если вы применяете эти настройки к уже работающему развёртыванию, перезапустите Gateway по вашей операционной процедуре, если ваш Ansible-запуск не перезапускает сервис после изменения конфигурации или token-файла.

## Проверьте настройку {#verify}

Откройте веб-интерфейс YDB EM:

```text
https://<gateway-host>:8789/ui/clusters
```

Проверьте, что model proxy включён:

```bash
curl -k https://<gateway-host>:8789/capabilities
```

Ответ должен содержать:

```json
{
  "Settings": {
    "Proxy": {
      "Model": true
    }
  }
}
```

Проверьте runtime-конфигурацию, которую получает UI:

```bash
curl -k https://<gateway-host>:8789/meta/ai_assistant_client_config
```

Ответ должен содержать блок `llm` и не должен содержать секреты модели.

Проверьте model proxy:

```bash
curl -k https://<gateway-host>:8789/proxy/model/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"<model-name>","messages":[{"role":"user","content":"ping"}],"max_tokens":1}'
```

Если в вашем развёртывании Gateway требует аутентификацию пользователя, добавьте те же authentication headers, которые используются для UI YDB EM.

## Устранение неполадок {#troubleshooting}

### Кнопка ассистента не отображается {#button-not-shown}

Проверьте, что `ydb_em_ai_assistant_enabled` равно `true`, а `/capabilities` содержит `Settings.Proxy.Model: true`. Также проверьте пользовательскую настройку AI-ассистента в UI YDB EM: YDB EM инициализирует её включённой, когда backend capability присутствует, но пользователь может её выключить.

### Runtime-конфигурация невалидна {#runtime-config-invalid}

Проверьте `GET /meta/ai_assistant_client_config`. Для включённого ассистента ответ должен быть JSON-объектом со строковыми полями `llm.baseURL` и `llm.model`. Ответ `null` означает, что Gateway не загрузил включённую клиентскую конфигурацию AI-ассистента.

### Model proxy возвращает ошибку авторизации {#model-authorization-error}

Проверьте, что `ydb_em_ai_model_token_name` совпадает с именем записи tokenator, а запись возвращает заголовок Authorization, который ожидает endpoint модели. Если tokenator не находит запись, Gateway отправляет upstream-запрос без ожидаемого заголовка Authorization.

### Model proxy вызывает неправильный upstream path {#wrong-upstream-path}

Проверьте `ydb_em_ai_model_endpoint` вместе с `llm.baseURL`. Gateway добавляет путь из `/proxy/model/...` к настроенному endpoint. Дублирование `/v1` часто приводит к upstream `404`.

### MCP-инструменты недоступны {#mcp-tools-unavailable}

Проверьте, что runtime-конфигурация содержит `url: "/meta/mcp"` и что пользовательские запросы могут обращаться к `/meta/mcp` через Gateway. Gateway регистрирует `/meta/mcp` только если MCP включён; по умолчанию он включён, но custom Gateway config может его отключить.

### Поиск по документации недоступен {#docs-search-unavailable}

Проверьте настройки `ydb_em_docs_search_*`, путь embeddings endpoint и запись tokenator для embeddings-запросов. Также убедитесь, что documentation vectors доступны в metabase YDB EM. Если поиск по документации не настроен, инструмент `search_docs` не публикуется.
