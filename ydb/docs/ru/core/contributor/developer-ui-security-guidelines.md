# Рекомендации по безопасности Developer UI

Эта статья — чеклист требований безопасности для разработчиков и контрибьюторов {{ ydb-short-name }}, которые пишут на C++ страницы мониторинга ([Developer UI](../reference/ydb-ui/index.md)). Такие страницы генерируются во время выполнения кода с помощью макросов `HTML(str) { ... }` и отдаются встроенным HTTP-сервером мониторинга.

Механизмы [политики безопасности контента](https://developer.mozilla.org/en-US/docs/Web/HTTP/CSP) (Content Security Policy, CSP), включая `nonce`, и защиты от [межсайтовой подделки запросов](https://ru.wikipedia.org/wiki/Межсайтовая_подделка_запроса) (Cross-Site Request Forgery, CSRF) в HTTP-слое мониторинга описаны ниже по текущему поведению кода. Они появились в pull-запросе [#36981](https://github.com/ydb-platform/ydb/pull/36981).

## Content Security Policy (CSP) и nonce {#csp-and-nonce}

{% note info %}

**Текущая реализация.** Для ответов мониторинга задаётся одна директива CSP:

```http
Content-Security-Policy: script-src 'nonce-AbCd…=='
```

В заголовке отсутствуют `style-src`, `font-src`, `connect-src`, `frame-src`, `img-src` и `default-src`. В текущей версии браузер контролирует только выполнение `<script>`; правила ниже для остальных типов ресурсов — рекомендации защитного программирования для совместимости с будущими версиями, а не требования, которые браузер уже принудительно обеспечивает.

Преобразование `nonce` в заголовок CSP выполняется в [THttpMonLegacyActorRequest::Handle(TEvHttpInfoRes…)](https://github.com/ydb-platform/ydb/blob/main/ydb/core/mon/mon.cpp). Это legacy-путь мониторинга, который доставляет `TEvHttpInfoRes` и `TEvRemoteHttpInfoRes`. Обработчики, отвечающие «сырым» `THttpOutgoingResponse`, обеспечивают безопасность самостоятельно.

{% endnote %}

### Встроенные теги `<script>` и nonce {#inline-script-nonce}

{% note alert %}

Встроенный скрипт без атрибута `nonce` не выполняется. Без него браузер блокирует выполнение скрипта согласно политике CSP.

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

Код отрисовки страницы генерирует nonce для каждого ответа, подставляет его во все встроенные `<script>` и записывает в поле `Nonce` исходящего события `TEvRemoteHttpInfoRes` / `TEvHttpInfoRes`. Сгенерировать такой токен можно через [NActors::NMon::GenerateCspNonce](https://github.com/ydb-platform/ydb/blob/main/ydb/library/actors/core/mon.h): это случайный [GUID](https://ru.wikipedia.org/wiki/GUID) в кодировке base64. HTTP-слой сам добавляет заголовок `Content-Security-Policy: script-src 'nonce-<value>'`; вручную его собирать не нужно.

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

Для страниц локального мониторинга, отдаваемых через `TEvHttpInfoRes` без проксирования через [таблетки](../concepts/glossary.md#tablet), действует то же присваивание `res->Nonce = nonce`. См. функцию `Notify(...)` в [tablet_monitoring_proxy.cpp](https://github.com/ydb-platform/ydb/blob/main/ydb/core/tablet/tablet_monitoring_proxy.cpp). Nonce не переиспользуется между ответами: для каждого вызова `OnRenderAppHtmlPage` генерируется новое значение.

При пересылке ответа между узлами nonce сохраняется: [TEvRemoteHttpInfoRes::SerializeToArcadiaStream](https://github.com/ydb-platform/ydb/blob/main/ydb/library/actors/core/mon.cpp) упаковывает его вместе с HTML, поэтому тот же подход работает для удалённого мониторинга таблеток.

### Политика `script-src` {#script-src-csp}

{% note alert %}

Ослабление `script-src` через `'unsafe-inline'`, `'unsafe-eval'` или внешние домены отключает защиту CSP. Если скрипт не работает без `'unsafe-inline'`, его переписывают с nonce, см. [{#T}](#inline-script-nonce).

```cpp
response << "Content-Security-Policy: script-src 'unsafe-inline'\r\n";
response << "Content-Security-Policy: script-src 'self' https://cdn.example.com\r\n";
```

{% endnote %}

### Встроенные стили {#inline-styles}

В текущем заголовке CSP нет директивы `style-src`, поэтому встроенные стили, такие как атрибуты `style="..."` и блоки `<style>`, браузером не блокируются. Они широко используются на существующих страницах Developer UI: hive monitoring, pdisk, tablet_flat, graph, cms, columnshard, tracing и т. д.

{% note info %}

Планируется миграция этих мест и последующее добавление более строгой директивы `style-src` в заголовок.

{% endnote %}

{% note warning %}

Не добавляйте встроенные стили в новом коде. Их отсутствие в текущей политике CSP не означает, что такое использование допустимо.

```cpp
str << "<div style='color:red; margin:5px'>...</div>";
str << "<style>.my-table th { text-align: center; }</style>";
```

{% endnote %}

Предпочтительно выносить стили в статический CSS-файл, отдаваемый с того же источника:

```cpp
// В ydb/core/viewer/.../monitoring.css (отдаётся из /static/):
//   .mon-warning { color: red; margin: 5px; }
//   .mon-table th { text-align: center; }
str << "<div class='mon-warning'>...</div>";
```

Когда в заголовок будет добавлена более строгая директива `style-src`, её не ослабляют с помощью `'unsafe-inline'` или внешних доменов.

## Внешние ресурсы {#no-external-resources}

{% note info %}

**Статус принудительного применения.** Только строка `script-src` в таблице ниже обеспечивается текущим заголовком CSP. Остальные строки описывают целевую политику, к которой движется кодовая база; её соблюдение в новом коде позволит включить более строгий заголовок позже без поломки UI.

{% endnote %}

| Директива | Целевая политика | Применяется сейчас? |
| --- | --- | --- |
| `script-src` | `'self'` + nonce, без внешних скриптов | Да — `script-src 'nonce-…'` |
| `style-src` | только `'self'`, без внешних таблиц стилей | Нет |
| `font-src` | `'self'`, без внешних шрифтов | Нет |
| `connect-src` | `'self'`, без внешних `fetch()`/XMLHttpRequest (XHR) | Нет |
| `frame-src` | `'self'`, без внешних iframe | Нет |
| `img-src` | `'self'`, `data:` и `https:` — внешние URL допустимы | Нет |

### Относительные ссылки в HTML {#relative-links}

Страницы мониторинга могут отдаваться под разными префиксами, поэтому в генерируемом HTML не задаются абсолютные пути. В `href`, `src`, `action`, `formaction`, `fetch()`, `$.ajax()` и т. п. используются только относительные ссылки. Не используются:

- полные URL: `https://example.com/...`;
- URL без схемы, protocol-relative: `//example.com/...`;
- пути от корня сайта: `/get_blob`, `/static/js/...`.

{% note alert %}

Абсолютные URL и пути от корня сайта в генерируемом HTML не используются.

```cpp
out << "<a href='https://ydb.tech/docs'>docs</a>\n";
out << "<button type='submit' formaction='/get_blob'>Query</button>\n";
out << "fetch('/api/data')\n";
```

{% endnote %}

Если странице нужна ссылка на документацию продукта или другую внешнюю страницу, её оформляют через относительную внутреннюю страницу или редирект либо выводят обычный текст вместо кликабельной внешней ссылки.

```cpp
out << "<a href='docs'>docs</a>\n";
out << "<button type='submit' formaction='get_blob'>Query</button>\n";
out << "fetch('api/data')\n";
```

### Загрузка скриптов, стилей и шрифтов {#no-external-scripts}

Ресурсы загружаются с того же источника, без внешних ссылок.

{% note alert %}

Внешние URL для скриптов, стилей и шрифтов несовместимы с целевой политикой CSP.

```cpp
out << "<script src='https://code.jquery.com/jquery-3.6.0.min.js'></script>\n";
out << "<link href='https://fonts.googleapis.com/css?family=Roboto' rel='stylesheet'>\n";
```

{% endnote %}

Bootstrap, jQuery и tablesorter уже включены в набор встроенных ресурсов и отдаются обёрткой страницы мониторинга. Повторные теги `<script>`/`<link>` для них не добавляют. У библиотек разные префиксы: Bootstrap и jQuery — из `/static/`, tablesorter — из корня (`/jquery.tablesorter.js`, `/jquery.tablesorter.css`), а не из `/static/js/jquery.tablesorter.js`.

Если странице всё же нужно сослаться на встроенный ресурс из C++, действуют относительные ссылки: пути от корня в генерируемый HTML не зашиваются.

Если нужна библиотека, которой ещё нет во встроенных ресурсах, её добавляют в [ydb/core/viewer/](https://github.com/ydb-platform/ydb/tree/main/ydb/core/viewer) и подключают через обёртку или вспомогательную функцию мониторинга с того же источника.

### Запросы fetch() и XHR {#no-absolute-fetch}

{% note alert %}

Для `fetch()` и XHR-запросов не используются абсолютные URL и пути от корня сайта.

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

Внешние iframe несовместимы с целевой политикой `frame-src`.

```cpp
out << "<iframe src='https://external.example.com/widget'></iframe>\n";
```

{% endnote %}

## Защита от CSRF {#csrf-protection}

HTTP-слой мониторинга реализует защиту от CSRF по схеме [double-submit cookie](https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html#alternative-using-a-double-submit-cookie-pattern):

- При любом ответе сервер устанавливает cookie `csrf_token`, если она ещё не задана. Значение cookie — случайный [GUID](https://ru.wikipedia.org/wiki/GUID); параметры cookie: `SameSite=Strict` и `Path=/`. Флаг `HttpOnly` намеренно не выставляется: схема double-submit cookie требует чтения значения из JavaScript. Флаг `Secure` также не выставляется: HTTP-слой мониторинга не знает, работает ли он за TLS.
- Для методов POST, PUT, DELETE и PATCH сервер сравнивает cookie `csrf_token` с заголовком `X-CSRF-Token` или параметром формы `csrf_token`. При несовпадении возвращается `403 FORBIDDEN`.
- Проверка CSRF пропускается, если запрос не основан на cookie: отсутствует cookie `ydb_session_id`. Так работают, например, API-клиенты с заголовками `Authorization`.
- Если cookie `ydb_session_id` есть, а cookie `csrf_token` ещё не выдана, запрос отклоняется. Так бывает при первом POST сразу после входа без предшествующего GET. Перед запросом, изменяющим состояние, нужен хотя бы один GET.

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

Функцию можно встроить в каждый блок `<script nonce='...'>`, где она нужна. Предпочтительнее выводить её один раз на страницу из общей вспомогательной функции рядом с кодом отрисовки страницы, например `RenderCsrfTokenHelper(str, nonce)`.

### CSRF-токен в запросах, изменяющих состояние {#csrf-token-required}

Сервер мониторинга принимает токен двумя способами. Реализация — `CheckCsrfToken` в [mon.cpp](https://github.com/ydb-platform/ydb/blob/main/ydb/core/mon/mon.cpp):

1. Заголовок запроса `X-CSRF-Token` предпочтителен для `fetch` и `$.ajax`. Он обязателен, если тело запроса не закодировано как форма, в том числе для JSON: сервер разбирает тело как `TCgiParameters`, поэтому поле `csrf_token` внутри JSON не будет найдено.
2. Параметр формы `csrf_token` в теле POST работает только при `Content-Type: application/x-www-form-urlencoded`. Это обычная `<form method="POST">` или тело `URLSearchParams`. Формы не могут задавать произвольные заголовки.

Оба подхода допустимы. Форма подходит, когда нужен запасной вариант без JavaScript. Пример — страница отключения Self-Heal контроллера BlobStorage в [self_heal.cpp](https://github.com/ydb-platform/ydb/blob/main/ydb/core/mind/bscontroller/self_heal.cpp). В остальных случаях предпочтителен `fetch` с заголовком `X-CSRF-Token`: он лучше сочетается с динамическим UI и единственный для JSON-тел. Пример — [state_storage_state.js](https://github.com/ydb-platform/ydb/blob/main/ydb/core/cms/ui/state_storage_state.js).

{% note alert %}

POST-запрос без CSRF-токена будет отклонён сервером.

```cpp
str << "<form method='POST' action=''>\n";
str << "  <input type='hidden' name='restartPDisk' value='1'>\n";
str << "  <button type='submit'>Restart</button>\n";  // ← нет поля csrf_token
str << "</form>\n";
```

{% endnote %}

#### Вариант A: <form> со скрытым полем csrf_token

Серверный обработчик читает cookie `csrf_token` из входящего `TEvRemoteHttpInfo` через `ev->Get()->GetCookie("csrf_token")` и передаёт её в код отрисовки. Токен экранируется для HTML при вставке в значение атрибута. Подходит небольшое inline-экранирование, как в [self_heal.cpp](https://github.com/ydb-platform/ydb/blob/main/ydb/core/mind/bscontroller/self_heal.cpp), или `HtmlEscape`, см. [{#T}](#output-escaping):

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

#### Вариант B: fetch из блока <script nonce='...'> с заголовком X-CSRF-Token

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

#### Вариант B с $.ajax

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

Пример этого паттерна в репозитории — функция `loadDistconfStatus` в [state_storage_state.js](https://github.com/ydb-platform/ydb/blob/main/ydb/core/cms/ui/state_storage_state.js): POST, который читает `csrf_token` из `document.cookie` и передаёт его как `X-CSRF-Token`.

### GET-запросы и изменение состояния {#get-no-side-effects}

GET-запросы не защищены от CSRF: `CheckCsrfToken` проверяет токен только для `POST`, `PUT`, `DELETE` и `PATCH`. См. [mon.cpp](https://github.com/ydb-platform/ydb/blob/main/ydb/core/mon/mon.cpp), функция `IsCsrfProtectedMethod`. Если странице нужно инициировать действие — перезапуск, остановку, переконфигурацию — применяется один из защищённых методов, обычно POST.

{% note alert %}

Выполнение операции, изменяющей состояние, в GET-обработчике приводит к уязвимости CSRF.

```cpp
void RenderPage(IOutputStream& str, const TCgiParameters& params) {
    if (params.Has("action")) {
        DoSomethingDestructive(); // ← побочный эффект из GET!
    }
    // ... render HTML
}
```

{% endnote %}

Разделение GET и POST:

```cpp
// GET-обработчик: только отрисовка
void HandleGet(NMon::TEvHttpInfo::TPtr& ev) {
    TStringStream html;
    RenderPage(html, ev->Get()->Request);
    ReplyAndPassAway(Viewer->GetHTTPOK(Request, "text/html; charset=utf-8", html.Str()));
}

// POST-обработчик: только действие, без отрисовки
void HandlePost(NMon::TEvHttpInfo::TPtr& ev) {
    const auto& params = ev->Get()->Request.GetParams();
    if (params.Get("action") == "restart") {
        DoRestart();
    }
    ReplyAndPassAway(Viewer->GetHTTPOK(Request, "text/html; charset=utf-8", "OK"));
}
```

## Встроенные обработчики событий {#no-inline-handlers}

Встроенные обработчики событий, такие как `onclick="..."` и `onchange="..."`, блокируются политикой CSP `script-src` даже при наличии nonce, поскольку nonce относится только к блокам `<script>`, а не к inline-атрибутам.

{% note alert %}

Атрибуты HTML вроде `onclick` и `onchange` не используются для привязки обработчиков событий.

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

Любые управляемые пользователем или полученные извне данные, выводимые в HTML, экранируются.

{% note alert %}

Пользовательские и внешние данные без экранирования могут привести к инъекции разметки.

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

В HTML-текст и в значения HTML-атрибутов подставляют `HtmlEscape`. Для значений в query-части `href` — URL-кодирование через `CGIEscapeRet`. Числовой идентификатор в query можно не экранировать; строку — нужно. Сами пути остаются относительными, см. [{#T}](#relative-links):

```cpp
str << "<a href='tablets?TabletID=" << tabletId << "'>";       // число — безопасно
str << "<a href='path?name=" << CGIEscapeRet(name) << "'>";    // строка — нужно экранировать
```

### Динамические значения и <script> {#no-script-interpolation}

У JavaScript свои правила экранирования, и `HtmlEscape` их **не покрывает**: не обрабатываются `'`, `\`, символы конца строки `U+2028` и `U+2029` и подстроки `</script>`. Значения вроде `O'Brien`, `foo\nbar` или `</script><script>alert(1)//` выходят из JS-литерала даже после `HtmlEscape`.

Вместо самописного JS-экранирования на сервере тело скрипта остаётся полностью статичным, а динамические значения читаются из атрибутов `data-*` через стандартный API `dataset` — для контекста атрибута `HtmlEscape` подходит корректно.

{% note alert %}

Интерполяция динамических значений в блок `<script>` приводит к [межсайтовому скриптингу](https://ru.wikipedia.org/wiki/Межсайтовый_скриптинг) (XSS).

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
    // ... используйте tableName, errorText, tabletId
)js";
str << "</script>";
```

Текст самого `<script>` лучше оставлять неизменным: не вставлять в него значения через конкатенацию или шаблоны. Все динамические данные передавайте через HTML-атрибуты — там их безопасно экранирует `HtmlEscape`.

Так же поступают с массивами и объектами: на стороне C++ сериализуют их в JSON-строку, кладут в один атрибут `data-...`, а в браузере читают через `JSON.parse(el.dataset.items)`.

Встроенные обработчики вроде `onclick="..."` и API со строковым кодом, такие как `eval` и `setTimeout('...')`, не входят в эту рекомендацию: они уже ограничены в [{#T}](#no-inline-handlers) и политикой `script-src` без `'unsafe-eval'`.

## HTTP-ответы и GetHTTPOK() {#get-httpok}

HTTP-ответы формируются через [TViewer::GetHTTPOK()](https://github.com/ydb-platform/ydb/blob/main/ydb/core/viewer/viewer.cpp) и связанные методы; сырые HTTP-строки не собираются вручную.

В заголовке `Content-Type` указывается `charset=utf-8` — `GetHTTPOK()` не добавляет его автоматически.

```cpp
ReplyAndPassAway(Viewer->GetHTTPOK(Request, "text/html; charset=utf-8", htmlContent));
```

{% note alert %}

Ответы собираются через `GetHTTPOK()` с `charset=utf-8` в `Content-Type`. Сырая HTTP-строка или ответ без charset обходят этот путь.

```cpp
Send(Sender, new NMon::TEvHttpInfoRes("HTTP/1.1 200 Ok\r\n\r\n" + html));  // сырая строка
ReplyAndPassAway(Viewer->GetHTTPOK(Request, "text/html", htmlContent));     // нет charset
```

{% endnote %}

## См. также {#see-also}

- [{#T}](../reference/ydb-ui/index.md)
- [{#T}](../security/index.md)
- [OWASP CSP Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Content_Security_Policy_Cheat_Sheet.html)
- [OWASP CSRF Prevention](https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html)
- [MDN: Content Security Policy](https://developer.mozilla.org/en-US/docs/Web/HTTP/CSP)
