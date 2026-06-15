# Рекомендации по безопасности Developer UI

Эта статья предназначена для разработчиков и контрибьюторов {{ ydb-short-name }} и описывает требования безопасности при написании на C++ страниц мониторинга ([Developer UI](../../reference/embedded-ui/index.md)). Такие страницы генерируются во время выполнения с помощью макросов `HTML(str) { ... }` и отдаются встроенным HTTP-сервером мониторинга.

Цель статьи — помочь безопасно добавлять и изменять страницы Developer UI: с корректной политикой Content Security Policy (CSP), защитой от межсайтовой подделки запросов (Cross-Site Request Forgery, CSRF) и безопасным выводом данных в HTML.

Пример Pull Request с реализацией CSRF и обработкой nonce в HTTP-ответах: [#36981](https://github.com/ydb-platform/ydb/pull/36981).

## Content Security Policy (CSP) и `nonce` {#csp-and-nonce}

{% note info %}

**Что реально обеспечивает текущая реализация.** В PR [#36981](https://github.com/ydb-platform/ydb/pull/36981) для ответов мониторинга задаётся ровно одна директива CSP:

```http
Content-Security-Policy: script-src 'nonce-AbCd…=='
```

В заголовке **нет** `style-src`, `font-src`, `connect-src`, `frame-src`, `img-src` и `default-src`. Сегодня браузер контролирует только выполнение `<script>`; правила ниже для остальных типов ресурсов — **рекомендации оборонительного программирования** для совместимости с будущими версиями, а не требования, принудительно обеспечиваемые браузером.

Преобразование nonce в заголовок CSP выполняется в [`THttpMonLegacyActorRequest::Handle(TEvHttpInfoRes…)`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/mon/mon.cpp) (legacy-путь мониторинга, который доставляет `TEvHttpInfoRes`/`TEvRemoteHttpInfoRes`). Обработчики, отвечающие «сырым» `THttpOutgoingResponse`, должны обеспечивать безопасность самостоятельно.

{% endnote %}

### Все встроенные теги `<script>` должны использовать `nonce` {#inline-script-nonce}

**Нельзя** — встроенный скрипт без nonce:

```cpp
// ydb/core/blobstorage/pdisk/blobstorage_pdisk_impl_http.cpp
str << R"___(
    <script>
        function sendRestartRequest() {
            $.ajax({ url: "", data: "restartPDisk=", method: "POST" });
        }
    </script>
)___";
```

**Правильно** — сгенерировать nonce для каждого ответа, прикрепить его к событию ответа и использовать в `<script>`:

Фреймворк мониторинга (см. [#36981](https://github.com/ydb-platform/ydb/pull/36981)) предоставляет [`NActors::NMon::GenerateCspNonce()`](https://github.com/ydb-platform/ydb/blob/main/ydb/library/actors/core/mon.h) — случайный GUID в кодировке base64. Рендерер генерирует nonce, подставляет его во все встроенные `<script>` и записывает в `res->Nonce` исходящего `TEvRemoteHttpInfoRes` / `TEvHttpInfoRes`. HTTP-слой автоматически добавляет соответствующий заголовок `Content-Security-Policy: script-src 'nonce-<value>'` — заголовок CSP **не нужно** формировать вручную.

```cpp
#include <ydb/library/actors/core/mon.h>

bool OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext&) override {
    TStringStream s;
    TString nonce = NActors::NMon::GenerateCspNonce();
    RenderMainPage(s, nonce);

    auto* res = new NMon::TEvRemoteHttpInfoRes(s.Str());
    res->Nonce = nonce; // framework will emit the CSP header with this nonce
    Send(ev->Sender, res, 0, ev->Cookie);
    return true;
}

void RenderMainPage(IOutputStream& s, const TString& nonce) {
    HTML(s) {
        s << "<script nonce='" << nonce << "'>";
        s << R"(
            function sendRestartRequest() {
                fetch('', { method: 'POST', body: 'restartPDisk=' });
            }
        )";
        s << "</script>";
    }
}
```

Для страниц, отдаваемых через `TEvHttpInfoRes` (локальный mon, без проксирования через [таблетки](../concepts/glossary.md#tablet)), действует то же присваивание `res->Nonce = nonce` — см. `Notify(...)` в [`tablet_monitoring_proxy.cpp`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/tablet/tablet_monitoring_proxy.cpp). **Не переиспользуйте** nonce между ответами — генерируйте новый при каждом вызове `OnRenderAppHtmlPage`.

При пересылке ответа между узлами nonce сохраняется: [`TEvRemoteHttpInfoRes::SerializeToArcadiaStream`](https://github.com/ydb-platform/ydb/blob/main/ydb/library/actors/core/mon.cpp) упаковывает его вместе с HTML, поэтому тот же подход работает для удалённого мониторинга таблеток.

### Не ослабляйте CSP для `script-src` {#script-src-csp}

Не добавляйте в `script-src` `'unsafe-inline'`, `'unsafe-eval'` или внешние домены. Если скрипт не работает без `'unsafe-inline'`, перепишите его с использованием nonce (см. [раздел выше](#inline-script-nonce)).

**Нельзя** — ослабление `script-src`:

```cpp
response << "Content-Security-Policy: script-src 'unsafe-inline'\r\n";
response << "Content-Security-Policy: script-src 'self' https://cdn.example.com\r\n";
```

### Не добавляйте новые встроенные стили {#inline-styles}

В заголовке CSP из PR [#36981](https://github.com/ydb-platform/ydb/pull/36981) нет директивы `style-src`, поэтому встроенные стили (атрибуты `style="..."` и блоки `<style>`) сегодня браузером не блокируются. Они широко используются на существующих страницах Developer UI (hive monitoring, pdisk, tablet_flat, graph, cms, columnshard, tracing и т. д.); планируется миграция этих мест и последующее добавление более строгой `style-src` в заголовок.

Это **не** приглашение добавлять новые встроенные стили. Для нового кода:

**Избегайте** — новые встроенные стили:

```cpp
str << "<div style='color:red; margin:5px'>...</div>";
str << "<style>.my-table th { text-align: center; }</style>";
```

**Предпочтительно** — вынести стили в статический CSS-файл, отдаваемый с того же источника (origin):

```cpp
// In ydb/core/viewer/.../monitoring.css (served from /static/):
//   .mon-warning { color: red; margin: 5px; }
//   .mon-table th { text-align: center; }

str << "<div class='mon-warning'>...</div>";
```

Когда в заголовок будет добавлена более строгая `style-src`, **не ослабляйте** её с помощью `'unsafe-eval'` или внешних доменов.

## Запрет внешних ресурсов {#no-external-resources}

{% note info %}

**Статус принудительного применения.** Только строка `script-src` в таблице ниже обеспечивается заголовком CSP из PR [#36981](https://github.com/ydb-platform/ydb/pull/36981). Остальные строки описывают **целевую политику**, к которой движется кодовая база — следуйте им в новом коде, чтобы включение более строгого заголовка позже не сломало UI.

{% endnote %}

| Директива | Целевая политика | Применяется сейчас? |
| --- | --- | --- |
| `script-src` | `'self'` + nonce, без внешних скриптов | Да — `script-src 'nonce-…'` |
| `style-src` | только `'self'`, без внешних таблиц стилей | Нет — директивы нет в заголовке (см. [Content Security Policy (CSP) и nonce](#csp-and-nonce)) |
| `font-src` | `'self'`, без внешних шрифтов | Нет — директивы нет в заголовке |
| `connect-src` | `'self'`, без внешних `fetch()`/XMLHttpRequest (XHR) | Нет — директивы нет в заголовке |
| `frame-src` | `'self'`, без внешних iframe | Нет — директивы нет в заголовке |
| `img-src` | `'self'` и `data:`, без внешних URL | Нет — директивы нет в заголовке |

### Используйте только относительные ссылки в HTML, генерируемом из C++ {#relative-links}

Страницы мониторинга могут отдаваться под разными префиксами, поэтому в генерируемом HTML нельзя жёстко задавать абсолютные пути. В `href`, `src`, `action`, `formaction`, `fetch()`, `$.ajax()` и т. п. используйте только относительные ссылки. Не используйте:

- полные URL: `https://example.com/...`;
- protocol-relative URL: `//example.com/...`;
- пути от корня: `/get_blob`, `/static/js/...`.

**Нельзя** — абсолютные и root-relative ссылки:

```cpp
out << "<a href='https://ydb.tech/docs'>docs</a>\n";
out << "<button type='submit' formaction='/get_blob'>Query</button>\n";
out << "fetch('/api/data')\n";
```

**Правильно** — относительные ссылки:

```cpp
out << "<a href='docs'>docs</a>\n";
out << "<button type='submit' formaction='get_blob'>Query</button>\n";
out << "fetch('api/data')\n";
```

Если странице нужна ссылка на документацию продукта или другую внешнюю страницу, направьте пользователя через относительную внутреннюю страницу/редирект или выведите обычный текст вместо кликабельной внешней ссылки.

### Не загружайте скрипты, стили и шрифты с внешних URL {#no-external-scripts}

**Нельзя** — загрузка с внешних URL:

```cpp
out << "<script src='https://code.jquery.com/jquery-3.6.0.min.js'></script>\n";
out << "<link href='https://fonts.googleapis.com/css?family=Roboto' rel='stylesheet'>\n";
```

**Правильно** — используйте только ресурсы, отдаваемые с того же источника (origin):

Bootstrap, jQuery и tablesorter уже включены в bundle и отдаются обёрткой страницы мониторинга. Рендереры отдельных страниц на C++ обычно **не должны** добавлять для них дополнительные теги `<script>`/`<link>`.

Если рендереру страницы всё же нужно сослаться на встроенный ресурс, следуйте правилу об относительных ссылках: не зашивайте пути от корня вроде `/static/js/jquery.min.js` или `/jquery.tablesorter.js`.

Если нужна библиотека, которой ещё нет в bundle, добавьте её во встроенные ресурсы в [`ydb/core/viewer/`](https://github.com/ydb-platform/ydb/tree/main/ydb/core/viewer) и подключите через обёртку/хелпер мониторинга, не вводя внешние ссылки и пути от корня в C++ страницы.

### Не выполняйте `fetch()`/XHR-запросы по абсолютным ссылкам {#no-absolute-fetch}

То же правило для JavaScript-запросов: только относительные URL.

**Нельзя** — абсолютные и root-relative URL:

```cpp
str << "fetch('https://external-api.example.com/data')\n";
str << "fetch('/api/data')\n";
str << "$.ajax({ url: '/api/data' })\n";
```

**Правильно** — относительные URL:

```cpp
str << "fetch('')\n";           // same URL as the page
str << "fetch('api/data')\n";   // relative to the current page
str << "fetch('../api/data')\n"; // relative path to a sibling/parent endpoint
```

### Не встраивайте внешние iframe {#no-external-iframes}

**Нельзя** — внешний iframe:

```cpp
out << "<iframe src='https://external.example.com/widget'></iframe>\n";
```

## Защита от CSRF {#csrf-protection}

HTTP-слой мониторинга реализует защиту от CSRF по схеме double-submit cookie (см. PR [#36981](https://github.com/ydb-platform/ydb/pull/36981)):

- При любом ответе сервер устанавливает cookie `csrf_token` (случайный GUID, `SameSite=Strict; Path=/`), если она ещё не задана. Cookie намеренно **без** флага `HttpOnly` (схема double-submit cookie требует чтения значения из JS) и **без** флага `Secure` (HTTP-слой мониторинга не знает, работает ли он за TLS);
- Для методов, изменяющих состояние (POST/PUT/DELETE/PATCH), сервер сравнивает cookie `csrf_token` с заголовком запроса `X-CSRF-Token` или параметром формы `csrf_token`; при несовпадении возвращается `403 FORBIDDEN`;
- Проверка CSRF пропускается, если запрос не основан на cookie (нет cookie `ydb_session_id`) — например, для API-клиентов с заголовками `Authorization`;
- Если cookie `ydb_session_id` **есть**, а cookie `csrf_token` ещё не выдана (например, первый POST сразу после входа, без предшествующего GET), запрос отклоняется — убедитесь, что UI выполняет хотя бы один GET перед любым запросом, изменяющим состояние.

Общего JS-bundle мониторинга нет, поэтому каждый встроенный `<script>`, выполняющий POST, должен самостоятельно читать cookie `csrf_token`. Используйте небольшой хелпер `getCsrfToken()` с тем же именем cookie, что и на сервере в [#36981](https://github.com/ydb-platform/ydb/pull/36981):

```js
// Reads the `csrf_token` cookie set by the monitoring HTTP server.
// Returns an empty string when the cookie is absent (e.g. local deployment,
// or first request before any response has set the cookie) — the server will
// accept the request only if it does not require CSRF protection.
function getCsrfToken() {
  return document.cookie.match(/(?:^|;\s*)csrf_token=([^;]*)/)?.[1] || '';
}
```

Можно встроить эту функцию в каждый блок `<script nonce='...'>`, где она нужна, или — предпочтительно — выводить её один раз на страницу из общего хелпера (например, `RenderCsrfTokenHelper(str, nonce)` рядом с рендерером страницы).

### Каждый запрос, изменяющий состояние, должен содержать CSRF-токен {#csrf-token-required}

Сервер мониторинга принимает токен из одного из двух мест (см. `CheckCsrfToken` в [`mon.cpp`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/mon/mon.cpp)):

1. Заголовок запроса `X-CSRF-Token` — предпочтителен для вызовов `fetch`/`$.ajax`. **Обязателен** для любого тела, не закодированного как форма (в частности, для JSON-запросов): сервер разбирает тело как `TCgiParameters`, поэтому поле `csrf_token` внутри JSON не будет найдено;
2. Параметр формы `csrf_token` в теле POST — работает **только** при `Content-Type: application/x-www-form-urlencoded` (то есть для обычной `<form method="POST">` или тела `URLSearchParams`), поскольку формы не могут задавать произвольные заголовки запроса.

Оба подхода допустимы. Используйте форму только когда действительно нужен запасной вариант без JavaScript (страница «Disable Self-Heal» контроллера BlobStorage в PR — такой случай); иначе предпочитайте `fetch` с заголовком `X-CSRF-Token` — он лучше сочетается с динамическим UI и единственный для JSON-тел (как в [`state_storage_state.js`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/cms/ui/state_storage_state.js)).

**Нельзя** — POST без CSRF-токена:

```cpp
str << "<form method='POST' action=''>\n";
str << "  <input type='hidden' name='restartPDisk' value='1'>\n";
str << "  <button type='submit'>Restart</button>\n";  // ← no csrf_token field!
str << "</form>\n";
```

**Правильно (вариант A)** — `<form>` со скрытым полем `csrf_token`:

Серверный обработчик должен прочитать cookie `csrf_token` из входящего `TEvRemoteHttpInfo` (через `ev->Get()->GetCookie("csrf_token")`) и передать её в рендерер. Токен нужно экранировать для HTML при вставке в значение атрибута (используйте небольшой inline-эскейпер как в [`self_heal.cpp`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/mind/bscontroller/self_heal.cpp) или `HtmlEscape` — см. раздел [«Экранирование вывода»](#output-escaping)):

```cpp
void Handle(NMon::TEvRemoteHttpInfo::TPtr& ev) {
    TStringStream str;
    RenderMonPage(str, ev->Cookie, ev->Get()->GetCookie("csrf_token"));
    Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes(str.Str()));
}

void RenderMonPage(IOutputStream& out, bool selfHealEnabled, const TString& csrfToken) {
    out << "<form method='POST' action=''>";
    out << "  <input type='hidden' name='action' value='disableSelfHeal'>";
    out << "  <input type='hidden' name='csrf_token' value='" << HtmlEscape(csrfToken) << "'>";
    out << "  <input type='submit' value='DISABLE'/>";
    out << "</form>";
}
```

**Правильно (вариант B)** — `fetch` из блока `<script nonce='...'>` с заголовком `X-CSRF-Token`:

```cpp
str << "<button id='restartBtn'>Restart</button>\n";
str << "<script nonce='" << nonce << "'>\n";
str << R"js(
    document.getElementById('restartBtn').addEventListener('click', function() {
        const csrfToken = getCsrfToken();
        const headers = { 'Content-Type': 'application/x-www-form-urlencoded' };
        if (csrfToken) {
            headers['X-CSRF-Token'] = csrfToken;
        }
        fetch('', { method: 'POST', headers: headers, body: 'restartPDisk=1' });
    });
)js";
str << "</script>\n";
```

**Правильно (вариант B, вариант с `$.ajax`)** — та же идея с jQuery, если страница уже его использует:

```cpp
str << "<script nonce='" << nonce << "'>\n";
str << R"js(
    function sendRestartRequest() {
        const csrfToken = getCsrfToken();
        $.ajax({
            type: 'POST',
            url: '',
            data: 'restartPDisk=',
            headers: csrfToken ? { 'X-CSRF-Token': csrfToken } : {}
        });
    }
)js";
str << "</script>\n";
```

Пример этого паттерна в репозитории — [`state_storage_state.js`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/cms/ui/state_storage_state.js) (`loadDistconfStatus`): POST, который читает `csrf_token` из `document.cookie` и передаёт его как `X-CSRF-Token`.

### GET-обработчики не должны изменять состояние {#get-no-side-effects}

GET-запросы не защищены от CSRF: `CheckCsrfToken` проверяет токен только для `POST`/`PUT`/`DELETE`/`PATCH` (см. [`mon.cpp`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/mon/mon.cpp), `IsCsrfProtectedMethod`). Если странице нужно инициировать действие (перезапуск, остановка, переконфигурация), используйте один из защищённых методов — обычно POST.

**Нельзя** — побочный эффект в GET-обработчике:

```cpp
void RenderPage(IOutputStream& str, const TCgiParameters& params) {
    if (params.Has("action")) {
        DoSomethingDestructive(); // ← side effect triggered by GET!
    }
    // ... render HTML
}
```

**Правильно** — разделите GET (отображение) и POST (действие):

```cpp
// GET handler: render only
void HandleGet(NMon::TEvHttpInfo::TPtr& ev) {
    TStringStream html;
    RenderPage(html, ev->Get()->Request);
    ReplyAndPassAway(Viewer->GetHTTPOK(Request, "text/html; charset=utf-8", html.Str()));
}

// POST handler: action only, no rendering
void HandlePost(NMon::TEvHttpInfo::TPtr& ev) {
    const auto& params = ev->Get()->Request.GetParams();
    if (params.Get("action") == "restart") {
        DoRestart();
    }
    ReplyAndPassAway(Viewer->GetHTTPOK(Request, "text/html; charset=utf-8", "OK"));
}
```

## Запрет встроенных обработчиков `onclick` и `onXxx` {#no-inline-handlers}

Встроенные обработчики событий (`onclick="..."`, `onchange="..."` и т. д.) блокируются политикой CSP `script-src` даже при наличии nonce, поскольку nonce относится только к блокам `<script>`, а не к inline-атрибутам.

**Нельзя** — встроенный обработчик события:

```cpp
str << "<input type='checkbox' id='ignoreChecks' onchange='toggleButtonColor()'>";
str << "<button onclick='sendRestartRequest()'>Restart</button>";
```

**Правильно** — привяжите обработчики из блока `<script nonce='...'>`:

```cpp
str << "<input type='checkbox' id='ignoreChecks'>\n";
str << "<button id='restartOkButton'>Restart</button>\n";

str << "<script nonce='" << nonce << "'>\n";
str << R"js(
    document.getElementById('ignoreChecks').addEventListener('change', toggleButtonColor);
    document.getElementById('restartOkButton').addEventListener('click', sendRestartRequest);
)js";
str << "</script>\n";
```

## Экранирование вывода {#output-escaping}

Любые управляемые пользователем или полученные извне данные, выводимые в HTML, должны быть экранированы.

**Нельзя** — вывод без экранирования:

```cpp
TABLED() { str << pathName; }           // pathName may contain <, >, &, "
TABLED() { str << errorMessage; }       // error messages may contain HTML
```

**Правильно** — используйте `HtmlEscape`:

```cpp
#include <util/string/html.h>

TABLED() { str << HtmlEscape(pathName); }
TABLED() { str << HtmlEscape(errorMessage); }
```

Для URL в атрибутах `href` используйте URL-кодирование:

```cpp
str << "<a href='/tablets?TabletID=" << tabletId << "'>";  // numeric — safe
str << "<a href='/path?name=" << CGIEscapeRet(name) << "'>";  // string — must escape
```

### Не интерполируйте динамические значения в `<script>` {#no-script-interpolation}

У JavaScript свои правила экранирования, и `HtmlEscape` **не покрывает** их: не обрабатываются `'`, `\`, символы конца строки (`U+2028`, `U+2029`) и подстроки `</script>`. Значения вроде `O'Brien`, `foo\nbar` или `</script><script>alert(1)//` выходят из JS-литерала даже после `HtmlEscape`. Вместо самописного JS-эскейпинга на сервере оставьте тело скрипта полностью статичным и читайте динамические значения из атрибутов `data-*` через стандартный API `dataset` — для контекста атрибута `HtmlEscape` подходит корректно.

**Нельзя** — интерполяция значения в блок `<script>`:

```cpp
str << "<script nonce='" << nonce << "'>\n";
str << "  const tableName = '" << tableName << "';\n";              // raw: trivial XSS
str << "  const errorText = '" << HtmlEscape(errorText) << "';\n";  // still vulnerable:
                                                                    // a single ' or \ in the value breaks out.
str << "</script>";
```

**Правильно** — выведите значения в HTML-экранированных атрибутах `data-*` и прочитайте их из JS:

```cpp
str << "<div id='pageData'"
       " data-table-name='"  << HtmlEscape(tableName)  << "'"
       " data-error-text='"  << HtmlEscape(errorText)  << "'"
       " data-tablet-id='"   << tabletId               << "'"  // numeric — safe
    << "></div>\n";

str << "<script nonce='" << nonce << "'>\n";
str << R"js(
    const el = document.getElementById('pageData');
    const tableName = el.dataset.tableName;   // ← values come from DOM, not from source
    const errorText = el.dataset.errorText;
    const tabletId  = Number(el.dataset.tabletId);
    // ... use tableName, errorText, tabletId
)js";
str << "</script>";
```

Так тело скрипта остаётся фиксированной строкой без интерполяции, а каждое динамическое значение проходит через контекст HTML-атрибута, где `HtmlEscape` — правильный инструмент. Тот же подход работает для массивов и объектов — сериализуйте их в C++ в строку, положите в один атрибут `data-...`, на клиенте вызовите `JSON.parse(el.dataset.items)`.

Встроенные обработчики (`onclick="..."`) и API вроде `eval`/`setTimeout('...')` с eval строк не входят в эту рекомендацию — они уже запрещены в [разделе про inline-обработчики](#no-inline-handlers) и политикой `script-src` без `'unsafe-eval'`.

## Использование `GetHTTPOK()` для HTTP-ответов {#get-httpok}

Всегда используйте [`TViewer::GetHTTPOK()`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/viewer/viewer.cpp) и связанные методы для формирования HTTP-ответов — **не собирайте сырые HTTP-строки вручную**.

Всегда указывайте `charset=utf-8` в заголовке `Content-Type` — `GetHTTPOK()` не добавляет его автоматически.

**Правильно:**

```cpp
ReplyAndPassAway(Viewer->GetHTTPOK(Request, "text/html; charset=utf-8", htmlContent));
```

**Нельзя** — сырая HTTP-строка или отсутствие `charset`:

```cpp
Send(Sender, new NMon::TEvHttpInfoRes("HTTP/1.1 200 Ok\r\n\r\n" + html));  // raw string
ReplyAndPassAway(Viewer->GetHTTPOK(Request, "text/html", htmlContent));     // missing charset
```

## См. также {#see-also}

- [{#T}](../../reference/embedded-ui/index.md)
- [{#T}](../../security/index.md)
- [OWASP CSP Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Content_Security_Policy_Cheat_Sheet.html)
- [OWASP CSRF Prevention](https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html)
- [MDN: Content Security Policy](https://developer.mozilla.org/en-US/docs/Web/HTTP/CSP)
