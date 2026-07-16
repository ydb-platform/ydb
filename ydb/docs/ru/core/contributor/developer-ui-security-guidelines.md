# Рекомендации по безопасности Developer UI

Эта статья — чеклист требований безопасности для разработчиков и контрибьюторов {{ ydb-short-name }}, которые пишут на C++ страницы мониторинга ([Developer UI](../reference/embedded-ui/index.md)). Такие страницы генерируются во время выполнения с помощью макросов `HTML(str) { ... }` и отдаются встроенным HTTP-сервером мониторинга.

В статье рассмотрены [политика безопасности контента](https://developer.mozilla.org/en-US/docs/Web/HTTP/CSP) (Content Security Policy, CSP), защита от [межсайтовой подделки запросов](https://ru.wikipedia.org/wiki/Межсайтовая_подделка_запроса) (Cross-Site Request Forgery, CSRF) и безопасный вывод данных в HTML.

Механизмы CSP (`nonce`) и CSRF в HTTP-слое мониторинга описаны ниже по текущему поведению кода; они появились в pull-запросе [#36981](https://github.com/ydb-platform/ydb/pull/36981).

## Content Security Policy (CSP) и nonce {#csp-and-nonce}

{% note info %}

**Текущая реализация.** Для ответов мониторинга задаётся одна директива CSP:

```http
Content-Security-Policy: script-src 'nonce-AbCd…=='
```

В заголовке отсутствуют `style-src`, `font-src`, `connect-src`, `frame-src`, `img-src` и `default-src`. В текущей версии браузер контролирует только выполнение `<script>`; правила ниже для остальных типов ресурсов — рекомендации защитного программирования для совместимости с будущими версиями, а не требования, которые браузер уже принудительно обеспечивает.

Преобразование nonce в заголовок CSP выполняется в [`THttpMonLegacyActorRequest::Handle(TEvHttpInfoRes…)`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/mon/mon.cpp) (legacy-путь мониторинга, который доставляет `TEvHttpInfoRes`/`TEvRemoteHttpInfoRes`). Обработчики, отвечающие «сырым» `THttpOutgoingResponse`, обеспечивают безопасность самостоятельно.

{% endnote %}

### Встроенные теги `<script>` и nonce {#inline-script-nonce}

Во встроенных тегах `<script>` требуется атрибут `nonce`. Без него браузер блокирует выполнение скрипта согласно политике CSP.

{% note alert %}

Встроенный скрипт без атрибута `nonce` не выполняется при действующей политике CSP.

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

{% endnote %}

Для каждого ответа генерируется nonce, прикрепляется к событию ответа и подставляется во встроенные `<script>`. Фреймворк мониторинга предоставляет [`NActors::NMon::GenerateCspNonce()`](https://github.com/ydb-platform/ydb/blob/main/ydb/library/actors/core/mon.h) — случайный GUID в кодировке base64. Код отрисовки страницы генерирует nonce, подставляет его во все встроенные `<script>` и записывает в `res->Nonce` исходящего `TEvRemoteHttpInfoRes` / `TEvHttpInfoRes`. HTTP-слой автоматически добавляет соответствующий заголовок `Content-Security-Policy: script-src 'nonce-<value>'`; заголовок CSP формировать вручную не требуется.

```cpp
#include <ydb/library/actors/core/mon.h>

bool OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext&) override {
    TStringStream s;
    TString nonce = NActors::NMon::GenerateCspNonce();
    RenderMainPage(s, nonce);

    auto* res = new NMon::TEvRemoteHttpInfoRes(s.Str());
    res->Nonce = nonce; // HTTP-слой добавит заголовок CSP с этим nonce
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

Для страниц, отдаваемых через `TEvHttpInfoRes` (локальный mon, без проксирования через [таблетки](../concepts/glossary.md#tablet)), действует то же присваивание `res->Nonce = nonce` — см. `Notify(...)` в [`tablet_monitoring_proxy.cpp`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/tablet/tablet_monitoring_proxy.cpp). Nonce не переиспользуется между ответами: для каждого вызова `OnRenderAppHtmlPage` генерируется новое значение.

При пересылке ответа между узлами nonce сохраняется: [`TEvRemoteHttpInfoRes::SerializeToArcadiaStream`](https://github.com/ydb-platform/ydb/blob/main/ydb/library/actors/core/mon.cpp) упаковывает его вместе с HTML, поэтому тот же подход работает для удалённого мониторинга таблеток.

### Политика `script-src` {#script-src-csp}

В директиву `script-src` не добавляются `'unsafe-inline'`, `'unsafe-eval'` и внешние домены. Если скрипт не работает без `'unsafe-inline'`, его переписывают с использованием nonce (см. [{#T}](#inline-script-nonce)).

{% note alert %}

Нельзя ослаблять директиву `script-src`, добавляя `'unsafe-inline'`, `'unsafe-eval'` или внешние домены.

```cpp
response << "Content-Security-Policy: script-src 'unsafe-inline'\r\n";
response << "Content-Security-Policy: script-src 'self' https://cdn.example.com\r\n";
```

{% endnote %}

### Встроенные стили {#inline-styles}

В текущем заголовке CSP нет директивы `style-src`, поэтому встроенные стили (атрибуты `style="..."` и блоки `<style>`) браузером не блокируются. Они широко используются на существующих страницах Developer UI (hive monitoring, pdisk, tablet_flat, graph, cms, columnshard, tracing и т. д.); планируется миграция этих мест и последующее добавление более строгой `style-src` в заголовок.

Отсутствие блокировки встроенных стилей в текущей версии не означает, что их следует добавлять в новом коде.

{% note alert %}

Для нового кода не рекомендуется добавлять встроенные стили — атрибуты `style="..."` и блоки `<style>`.

```cpp
str << "<div style='color:red; margin:5px'>...</div>";
str << "<style>.my-table th { text-align: center; }</style>";
```

{% endnote %}

Предпочтительно выносить стили в статический CSS-файл, отдаваемый с того же источника (origin):

```cpp
// In ydb/core/viewer/.../monitoring.css (served from /static/):
//   .mon-warning { color: red; margin: 5px; }
//   .mon-table th { text-align: center; }

str << "<div class='mon-warning'>...</div>";
```

Когда в заголовок будет добавлена более строгая `style-src`, её не ослабляют с помощью `'unsafe-inline'` или внешних доменов.

## Внешние ресурсы {#no-external-resources}

{% note info %}

**Статус принудительного применения.** Только строка `script-src` в таблице ниже обеспечивается текущим заголовком CSP. Остальные строки описывают целевую политику, к которой движется кодовая база; её соблюдение в новом коде позволит включить более строгий заголовок позже без поломки UI.

{% endnote %}

| Директива | Целевая политика | Применяется сейчас? |
| --- | --- | --- |
| `script-src` | `'self'` + nonce, без внешних скриптов | Да — `script-src 'nonce-…'` |
| `style-src` | только `'self'`, без внешних таблиц стилей | Нет — директивы нет в заголовке (см. [{#T}](#csp-and-nonce)) |
| `font-src` | `'self'`, без внешних шрифтов | Нет — директивы нет в заголовке |
| `connect-src` | `'self'`, без внешних `fetch()`/XMLHttpRequest (XHR) | Нет — директивы нет в заголовке |
| `frame-src` | `'self'`, без внешних iframe | Нет — директивы нет в заголовке |
| `img-src` | `'self'` и `data:`, без внешних URL | Нет — директивы нет в заголовке |

### Относительные ссылки в HTML {#relative-links}

Страницы мониторинга могут отдаваться под разными префиксами, поэтому в генерируемом HTML не задаются абсолютные пути. В `href`, `src`, `action`, `formaction`, `fetch()`, `$.ajax()` и т. п. используются только относительные ссылки. Запрещены:

- полные URL: `https://example.com/...`;
- URL без схемы (protocol-relative): `//example.com/...`;
- пути от корня сайта: `/get_blob`, `/static/js/...`.

{% note alert %}

Нельзя использовать абсолютные URL и пути от корня сайта в генерируемом HTML.

```cpp
out << "<a href='https://ydb.tech/docs'>docs</a>\n";
out << "<button type='submit' formaction='/get_blob'>Query</button>\n";
out << "fetch('/api/data')\n";
```

{% endnote %}

Относительные ссылки:

```cpp
out << "<a href='docs'>docs</a>\n";
out << "<button type='submit' formaction='get_blob'>Query</button>\n";
out << "fetch('api/data')\n";
```

Если странице нужна ссылка на документацию продукта или другую внешнюю страницу, её оформляют через относительную внутреннюю страницу или редирект либо выводят обычный текст вместо кликабельной внешней ссылки.

### Загрузка скриптов, стилей и шрифтов {#no-external-scripts}

Ресурсы загружаются с того же источника (origin), без внешних URL.

{% note alert %}

Нельзя загружать скрипты, стили и шрифты с внешних URL.

```cpp
out << "<script src='https://code.jquery.com/jquery-3.6.0.min.js'></script>\n";
out << "<link href='https://fonts.googleapis.com/css?family=Roboto' rel='stylesheet'>\n";
```

{% endnote %}

Bootstrap, jQuery и tablesorter уже включены в набор встроенных ресурсов и отдаются обёрткой страницы мониторинга. Код отрисовки отдельных страниц на C++ обычно не добавляет для них дополнительные теги `<script>`/`<link>`.

Если странице нужно сослаться на встроенный ресурс, применяется правило об относительных ссылках: пути от корня вроде `/static/js/jquery.min.js` или `/jquery.tablesorter.js` не зашиваются в код.

Если нужна библиотека, которой ещё нет во встроенных ресурсах, её добавляют в [`ydb/core/viewer/`](https://github.com/ydb-platform/ydb/tree/main/ydb/core/viewer) и подключают через обёртку или вспомогательную функцию мониторинга, без внешних ссылок и путей от корня в C++ страницы.

### Запросы `fetch()` и XHR {#no-absolute-fetch}

Для JavaScript-запросов применяется то же правило: только относительные URL.

{% note alert %}

Нельзя выполнять `fetch()`/XHR-запросы по абсолютным URL или путям от корня сайта.

```cpp
str << "fetch('https://external-api.example.com/data')\n";
str << "fetch('/api/data')\n";
str << "$.ajax({ url: '/api/data' })\n";
```

{% endnote %}

Относительные URL:

```cpp
str << "fetch('')\n";            // тот же URL, что у страницы
str << "fetch('api/data')\n";    // относительно текущей страницы
str << "fetch('../api/data')\n"; // относительный путь к соседнему/родительскому endpoint
```

### Встраивание iframe {#no-external-iframes}

В iframe допускаются только ресурсы с того же источника.

{% note alert %}

Нельзя встраивать внешние iframe.

```cpp
out << "<iframe src='https://external.example.com/widget'></iframe>\n";
```

{% endnote %}

## Защита от CSRF {#csrf-protection}

HTTP-слой мониторинга реализует защиту от CSRF по схеме [double-submit cookie](https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html#alternative-using-a-double-submit-cookie-pattern):

- при любом ответе сервер устанавливает cookie `csrf_token` (случайный GUID, `SameSite=Strict; Path=/`), если она ещё не задана; cookie намеренно без флага `HttpOnly` (схема double-submit cookie требует чтения значения из JS) и без флага `Secure` (HTTP-слой мониторинга не знает, работает ли он за TLS);
- для методов, изменяющих состояние (POST/PUT/DELETE/PATCH), сервер сравнивает cookie `csrf_token` с заголовком запроса `X-CSRF-Token` или параметром формы `csrf_token`; при несовпадении возвращается `403 FORBIDDEN`;
- проверка CSRF пропускается, если запрос не основан на cookie (нет cookie `ydb_session_id`) — например, для API-клиентов с заголовками `Authorization`;
- если cookie `ydb_session_id` есть, а cookie `csrf_token` ещё не выдана (например, первый POST сразу после входа, без предшествующего GET), запрос отклоняется; перед запросом, изменяющим состояние, требуется хотя бы один GET.

Общего JS-набора мониторинга нет, поэтому каждый встроенный `<script>`, выполняющий POST, самостоятельно читает cookie `csrf_token`. Для этого подходит небольшая вспомогательная функция `getCsrfToken()` с тем же именем cookie, что и на сервере:

```js
// Читает cookie csrf_token, которую выставляет HTTP-сервер мониторинга.
// Возвращает пустую строку, если cookie нет (например, локальный стенд
// или первый запрос до того, как любой ответ успел её выставить) —
// сервер примет запрос только если CSRF-защита для него не требуется.
function getCsrfToken() {
  return document.cookie.match(/(?:^|;\s*)csrf_token=([^;]*)/)?.[1] || '';
}
```

Функцию можно встроить в каждый блок `<script nonce='...'>`, где она нужна, или — предпочтительнее — выводить один раз на страницу из общей вспомогательной функции (например, `RenderCsrfTokenHelper(str, nonce)` рядом с кодом отрисовки страницы).

### CSRF-токен в запросах, изменяющих состояние {#csrf-token-required}

Сервер мониторинга принимает токен из одного из двух мест (см. `CheckCsrfToken` в [`mon.cpp`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/mon/mon.cpp)):

1. заголовок запроса `X-CSRF-Token` — предпочтителен для вызовов `fetch`/`$.ajax`; обязателен для любого тела, не закодированного как форма (в частности, для JSON-запросов): сервер разбирает тело как `TCgiParameters`, поэтому поле `csrf_token` внутри JSON не будет найдено;
2. параметр формы `csrf_token` в теле POST — работает только при `Content-Type: application/x-www-form-urlencoded` (то есть для обычной `<form method="POST">` или тела `URLSearchParams`), поскольку формы не могут задавать произвольные заголовки запроса.

Оба подхода допустимы. Форма подходит, когда нужен запасной вариант без JavaScript (как на странице Disable Self-Heal контроллера BlobStorage — см. [`self_heal.cpp`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/mind/bscontroller/self_heal.cpp)); в остальных случаях предпочтителен `fetch` с заголовком `X-CSRF-Token` — он лучше сочетается с динамическим UI и единственный для JSON-тел (как в [`state_storage_state.js`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/cms/ui/state_storage_state.js)).

{% note alert %}

Нельзя отправлять POST-запрос, изменяющий состояние, без CSRF-токена.

```cpp
str << "<form method='POST' action=''>\n";
str << "  <input type='hidden' name='restartPDisk' value='1'>\n";
str << "  <button type='submit'>Restart</button>\n";  // ← нет поля csrf_token!
str << "</form>\n";
```

{% endnote %}

#### Вариант A: `<form>` со скрытым полем `csrf_token`

Серверный обработчик читает cookie `csrf_token` из входящего `TEvRemoteHttpInfo` (через `ev->Get()->GetCookie("csrf_token")`) и передаёт её в код отрисовки. Токен экранируется для HTML при вставке в значение атрибута (подходит небольшое inline-экранирование как в [`self_heal.cpp`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/mind/bscontroller/self_heal.cpp) или `HtmlEscape` — см. [{#T}](#output-escaping)):

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

#### Вариант B: `fetch` из блока `<script nonce='...'>` с заголовком `X-CSRF-Token`

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

#### Вариант B с `$.ajax`

Та же идея с jQuery, если страница уже его использует:

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

### GET-запросы и изменение состояния {#get-no-side-effects}

GET-запросы не защищены от CSRF: `CheckCsrfToken` проверяет токен только для `POST`/`PUT`/`DELETE`/`PATCH` (см. [`mon.cpp`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/mon/mon.cpp), `IsCsrfProtectedMethod`). Если странице нужно инициировать действие (перезапуск, остановка, переконфигурация), применяется один из защищённых методов — обычно POST.

{% note alert %}

Нельзя выполнять операции, изменяющие состояние, в GET-обработчике.

```cpp
void RenderPage(IOutputStream& str, const TCgiParameters& params) {
    if (params.Has("action")) {
        DoSomethingDestructive(); // ← побочный эффект из GET!
    }
    // ... render HTML
}
```

{% endnote %}

Разделение GET (отображение) и POST (действие):

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

## Встроенные обработчики событий {#no-inline-handlers}

Встроенные обработчики событий (`onclick="..."`, `onchange="..."` и т. д.) блокируются политикой CSP `script-src` даже при наличии nonce, поскольку nonce относится только к блокам `<script>`, а не к inline-атрибутам.

{% note alert %}

Нельзя использовать встроенные обработчики событий в атрибутах HTML (`onclick`, `onchange` и т. д.).

```cpp
str << "<input type='checkbox' id='ignoreChecks' onchange='toggleButtonColor()'>";
str << "<button onclick='sendRestartRequest()'>Restart</button>";
```

{% endnote %}

Привязка обработчиков из блока `<script nonce='...'>`:

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

Любые управляемые пользователем или полученные извне данные, выводимые в HTML, должны экранироваться.

{% note alert %}

Нельзя выводить в HTML пользовательские или внешние данные без экранирования.

```cpp
TABLED() { str << pathName; }           // pathName может содержать <, >, &, "
TABLED() { str << errorMessage; }       // сообщения об ошибках могут содержать HTML
```

{% endnote %}

Вывод с `HtmlEscape`:

```cpp
#include <util/string/html.h>

TABLED() { str << HtmlEscape(pathName); }
TABLED() { str << HtmlEscape(errorMessage); }
```

Для URL в атрибутах `href` применяется URL-кодирование. Пример ниже показывает только экранирование значений; сами пути должны оставаться относительными (см. [{#T}](#relative-links)):

```cpp
str << "<a href='tablets?TabletID=" << tabletId << "'>";       // число — безопасно
str << "<a href='path?name=" << CGIEscapeRet(name) << "'>";    // строка — нужно экранировать
```

### Динамические значения и `<script>` {#no-script-interpolation}

У JavaScript свои правила экранирования, и `HtmlEscape` их **не покрывает**: не обрабатываются `'`, `\`, символы конца строки (`U+2028`, `U+2029`) и подстроки `</script>`. Значения вроде `O'Brien`, `foo\nbar` или `</script><script>alert(1)//` выходят из JS-литерала даже после `HtmlEscape`. Вместо самописного JS-экранирования на сервере тело скрипта остаётся полностью статичным, а динамические значения читаются из атрибутов `data-*` через стандартный API `dataset` — для контекста атрибута `HtmlEscape` подходит корректно.

{% note alert %}

Нельзя интерполировать динамические значения непосредственно в блок `<script>`: это приводит к [межсайтовому скриптингу](https://ru.wikipedia.org/wiki/Межсайтовый_скриптинг) (XSS).

```cpp
str << "<script nonce='" << nonce << "'>\n";
str << "  const tableName = '" << tableName << "';\n";              // сырое значение: тривиальный XSS
str << "  const errorText = '" << HtmlEscape(errorText) << "';\n";  // всё ещё уязвимо:
                                                                    // ', \ в значении ломают литерал
str << "</script>";
```

{% endnote %}

Передача значений через HTML-экранированные атрибуты `data-*` и чтение их из JS:

```cpp
str << "<div id='pageData'"
       " data-table-name='"  << HtmlEscape(tableName)  << "'"
       " data-error-text='"  << HtmlEscape(errorText)  << "'"
       " data-tablet-id='"   << tabletId               << "'"  // число — безопасно
    << "></div>\n";

str << "<script nonce='" << nonce << "'>\n";
str << R"js(
    const el = document.getElementById('pageData');
    const tableName = el.dataset.tableName;   // значения из DOM, не из исходника скрипта
    const errorText = el.dataset.errorText;
    const tabletId  = Number(el.dataset.tabletId);
    // ... use tableName, errorText, tabletId
)js";
str << "</script>";
```

Тело скрипта остаётся фиксированной строкой без интерполяции, а каждое динамическое значение проходит через контекст HTML-атрибута, где `HtmlEscape` — подходящий инструмент. Тот же подход работает для массивов и объектов: их сериализуют в C++ в строку, помещают в один атрибут `data-...`, на клиенте вызывают `JSON.parse(el.dataset.items)`.

Встроенные обработчики (`onclick="..."`) и API вроде `eval`/`setTimeout('...')` со строковым кодом не входят в эту рекомендацию — они уже ограничены в [{#T}](#no-inline-handlers) и политикой `script-src` без `'unsafe-eval'`.

## HTTP-ответы и `GetHTTPOK()` {#get-httpok}

HTTP-ответы формируются через [`TViewer::GetHTTPOK()`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/viewer/viewer.cpp) и связанные методы; сырые HTTP-строки не собираются вручную.

В заголовке `Content-Type` указывается `charset=utf-8` — `GetHTTPOK()` не добавляет его автоматически.

```cpp
ReplyAndPassAway(Viewer->GetHTTPOK(Request, "text/html; charset=utf-8", htmlContent));
```

{% note alert %}

Нельзя формировать сырой HTTP-ответ вручную или опускать `charset=utf-8` в заголовке `Content-Type`.

```cpp
Send(Sender, new NMon::TEvHttpInfoRes("HTTP/1.1 200 Ok\r\n\r\n" + html));  // сырая строка
ReplyAndPassAway(Viewer->GetHTTPOK(Request, "text/html", htmlContent));     // нет charset
```

{% endnote %}

## См. также {#see-also}

- [{#T}](../reference/embedded-ui/index.md)
- [{#T}](../security/index.md)
- [OWASP CSP Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Content_Security_Policy_Cheat_Sheet.html)
- [OWASP CSRF Prevention](https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html)
- [MDN: Content Security Policy](https://developer.mozilla.org/en-US/docs/Web/HTTP/CSP)
