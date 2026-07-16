# Developer UI Security Guidelines

This article is a security checklist for {{ ydb-short-name }} developers and contributors who write C++ monitoring pages ([Developer UI](../reference/embedded-ui/index.md)). These pages are generated at runtime using `HTML(str) { ... }` macros and are served by the built-in monitoring HTTP server.

This article covers [Content Security Policy](https://developer.mozilla.org/en-US/docs/Web/HTTP/CSP) (CSP), [Cross-Site Request Forgery](https://en.wikipedia.org/wiki/Cross-site_request_forgery) (CSRF) protection, and safe HTML output.

The CSP (`nonce`) and CSRF mechanisms in the monitoring HTTP layer are described below based on the current code behavior; they were introduced in pull request [#36981](https://github.com/ydb-platform/ydb/pull/36981).

## Content Security Policy (CSP) and nonce {#csp-and-nonce}

{% note info %}

**Current implementation.** Monitoring responses include one CSP directive:

```http
Content-Security-Policy: script-src 'nonce-AbCdвЂ¦=='
```

The header does not include `style-src`, `font-src`, `connect-src`, `frame-src`, `img-src`, or `default-src`. In the current version, the browser enforces restrictions only for `<script>` execution; rules below for other resource types are defensive coding recommendations for forward compatibility, not browser-enforced requirements yet.

Nonce-to-CSP-header translation happens in [`THttpMonLegacyActorRequest::Handle(TEvHttpInfoResвЂ¦)`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/mon/mon.cpp) (the legacy monitoring path that delivers `TEvHttpInfoRes`/`TEvRemoteHttpInfoRes`). Handlers that reply with raw `THttpOutgoingResponse` must ensure security on their own.

{% endnote %}

### Inline `<script>` tags and nonce {#inline-script-nonce}

Inline `<script>` tags require a `nonce` attribute. Without it, the browser blocks script execution under the active CSP policy.

{% note alert %}

An inline script without a `nonce` attribute does not execute under the active CSP policy.

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

A nonce is generated for every response, attached to the response event, and injected into inline `<script>` tags. The monitoring framework provides [`NActors::NMon::GenerateCspNonce()`](https://github.com/ydb-platform/ydb/blob/main/ydb/library/actors/core/mon.h), which returns a random base64-encoded GUID. Page rendering code generates the nonce, uses it in all inline `<script>` tags, and writes it to `res->Nonce` of outgoing `TEvRemoteHttpInfoRes` / `TEvHttpInfoRes`. The HTTP layer automatically adds the matching `Content-Security-Policy: script-src 'nonce-<value>'` header; the CSP header must not be built manually.

```cpp
#include <ydb/library/actors/core/mon.h>

bool OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext&) override {
    TStringStream s;
    TString nonce = NActors::NMon::GenerateCspNonce();
    RenderMainPage(s, nonce);

    auto* res = new NMon::TEvRemoteHttpInfoRes(s.Str());
    res->Nonce = nonce; // the HTTP layer adds the CSP header with this nonce
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

For pages served via `TEvHttpInfoRes` (local mon, without proxying through [tablets](../concepts/glossary.md#tablet)), the same `res->Nonce = nonce` assignment applies; see `Notify(...)` in [`tablet_monitoring_proxy.cpp`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/tablet/tablet_monitoring_proxy.cpp). Nonce values must not be reused between responses: generate a new value for each `OnRenderAppHtmlPage` call.

When a response is forwarded between nodes, nonce is preserved: [`TEvRemoteHttpInfoRes::SerializeToArcadiaStream`](https://github.com/ydb-platform/ydb/blob/main/ydb/library/actors/core/mon.cpp) serializes it together with HTML, so the same approach works for remote tablet monitoring.

### `script-src` policy {#script-src-csp}

Do not add `'unsafe-inline'`, `'unsafe-eval'`, or external domains to `script-src`. If a script does not work without `'unsafe-inline'`, rewrite it using nonce (see [{#T}](#inline-script-nonce)).

{% note alert %}

Do not weaken `script-src` by adding `'unsafe-inline'`, `'unsafe-eval'`, or external domains.

```cpp
response << "Content-Security-Policy: script-src 'unsafe-inline'\r\n";
response << "Content-Security-Policy: script-src 'self' https://cdn.example.com\r\n";
```

{% endnote %}

### Inline styles {#inline-styles}

The current CSP header has no `style-src` directive, so inline styles (`style="..."` attributes and `<style>` blocks) are not blocked by the browser. They are widely used on existing Developer UI pages (hive monitoring, pdisk, tablet_flat, graph, cms, columnshard, tracing, etc.); those usages are planned for migration, followed by adding stricter `style-src` to the header.

Lack of blocking in the current version does not mean new inline styles should be added.

{% note alert %}

For new code, avoid inline styles (`style="..."` attributes and `<style>` blocks).

```cpp
str << "<div style='color:red; margin:5px'>...</div>";
str << "<style>.my-table th { text-align: center; }</style>";
```

{% endnote %}

Prefer moving styles into a static CSS file served from the same origin:

```cpp
// In ydb/core/viewer/.../monitoring.css (served from /static/):
//   .mon-warning { color: red; margin: 5px; }
//   .mon-table th { text-align: center; }

str << "<div class='mon-warning'>...</div>";
```

When stricter `style-src` is added to the header, do not weaken it with `'unsafe-inline'` or external domains.

## External resources {#no-external-resources}

{% note info %}

**Enforcement status.** Only the `script-src` row in the table below is currently enforced by the CSP header. Other rows describe the target policy the codebase is moving to; following it in new code allows enabling stricter headers later without breaking the UI.

{% endnote %}

| Directive | Target policy | Enforced now? |
| --- | --- | --- |
| `script-src` | `'self'` + nonce, no external scripts | Yes: `script-src 'nonce-вЂ¦'` |
| `style-src` | `'self'` only, no external stylesheets | No: directive is not in the header (see [{#T}](#csp-and-nonce)) |
| `font-src` | `'self'`, no external fonts | No: directive is not in the header |
| `connect-src` | `'self'`, no external `fetch()`/XMLHttpRequest (XHR) | No: directive is not in the header |
| `frame-src` | `'self'`, no external iframes | No: directive is not in the header |
| `img-src` | `'self'` and `data:`, no external URLs | No: directive is not in the header |

### Relative links in generated HTML {#relative-links}

Monitoring pages may be served under different prefixes, so generated HTML must not hardcode absolute paths. Use only relative links in `href`, `src`, `action`, `formaction`, `fetch()`, `$.ajax()`, etc. Forbidden:

- full URLs: `https://example.com/...`;
- URL without scheme (protocol-relative): `//example.com/...`;
- root-relative paths: `/get_blob`, `/static/js/...`.

{% note alert %}

Do not use absolute URLs or root-relative paths in generated HTML.

```cpp
out << "<a href='https://ydb.tech/docs'>docs</a>\n";
out << "<button type='submit' formaction='/get_blob'>Query</button>\n";
out << "fetch('/api/data')\n";
```

{% endnote %}

Relative links:

```cpp
out << "<a href='docs'>docs</a>\n";
out << "<button type='submit' formaction='get_blob'>Query</button>\n";
out << "fetch('api/data')\n";
```

If a page needs a link to product docs or any external page, route it through a relative internal docs page/redirect, or render plain text instead of a clickable external link.

### Loading scripts, styles, and fonts {#no-external-scripts}

Resources must be loaded from the same origin, without external URLs.

{% note alert %}

Do not load scripts, styles, or fonts from external URLs.

```cpp
out << "<script src='https://code.jquery.com/jquery-3.6.0.min.js'></script>\n";
out << "<link href='https://fonts.googleapis.com/css?family=Roboto' rel='stylesheet'>\n";
```

{% endnote %}

Bootstrap, jQuery, and tablesorter are already included in the embedded resource set and served by the monitoring page wrapper. Page-specific C++ rendering code usually should not emit extra `<script>`/`<link>` tags for them.

If a page still needs to reference an embedded resource, the relative-link rule still applies: do not hardcode root-relative paths such as `/static/js/jquery.min.js` or `/jquery.tablesorter.js`.

If you need a library that is not yet in embedded resources, add it in [`ydb/core/viewer/`](https://github.com/ydb-platform/ydb/tree/main/ydb/core/viewer) and connect it via the monitoring wrapper or helper, without external URLs and without root-relative paths in page C++.

### `fetch()` and XHR requests {#no-absolute-fetch}

Same rule applies to JavaScript requests: use only relative URLs.

{% note alert %}

Do not make `fetch()`/XHR requests to absolute URLs or root-relative paths.

```cpp
str << "fetch('https://external-api.example.com/data')\n";
str << "fetch('/api/data')\n";
str << "$.ajax({ url: '/api/data' })\n";
```

{% endnote %}

Relative URLs:

```cpp
str << "fetch('')\n";            // same URL as the page
str << "fetch('api/data')\n";    // relative to the current page
str << "fetch('../api/data')\n"; // relative path to sibling/parent endpoint
```

### Embedding iframes {#no-external-iframes}

Only same-origin iframe sources are allowed.

{% note alert %}

Do not embed external iframes.

```cpp
out << "<iframe src='https://external.example.com/widget'></iframe>\n";
```

{% endnote %}

## CSRF protection {#csrf-protection}

The monitoring HTTP layer implements CSRF protection using the [double-submit cookie](https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html#alternative-using-a-double-submit-cookie-pattern) pattern:

- for any response, the server sets `csrf_token` cookie (random GUID, `SameSite=Strict; Path=/`) if it is not present yet; the cookie is intentionally without `HttpOnly` (double-submit requires JS read access) and without `Secure` (the monitoring HTTP layer does not know whether it runs behind TLS);
- for state-changing methods (POST/PUT/DELETE/PATCH), the server compares `csrf_token` cookie to `X-CSRF-Token` request header or `csrf_token` form field; on mismatch it returns `403 FORBIDDEN`;
- CSRF checks are skipped when request is not cookie-based (no `ydb_session_id` cookie), for example API clients with `Authorization` headers;
- if `ydb_session_id` exists but `csrf_token` has not been issued yet (for example first POST immediately after login without previous GET), the request is rejected; do at least one GET before a state-changing request.

There is no shared monitoring JS bundle, so each inline `<script>` that makes POST requests reads `csrf_token` cookie itself. A small `getCsrfToken()` helper with the same cookie name as on the server is sufficient:

```js
// Reads the csrf_token cookie set by the monitoring HTTP server.
// Returns empty string if cookie is absent (for example, local setup
// or first request before any response has set it) -
// the server accepts the request only when CSRF protection is not required.
function getCsrfToken() {
  return document.cookie.match(/(?:^|;\s*)csrf_token=([^;]*)/)?.[1] || '';
}
```

The function can be embedded into each `<script nonce='...'>` block that needs it, or preferably emitted once per page from a shared helper (for example, `RenderCsrfTokenHelper(str, nonce)` near page rendering code).

### CSRF token in state-changing requests {#csrf-token-required}

Monitoring server accepts token from one of two places (see `CheckCsrfToken` in [`mon.cpp`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/mon/mon.cpp)):

1. `X-CSRF-Token` request header: preferred for `fetch`/`$.ajax`; required for any non-form-encoded body (especially JSON), because server parses body as `TCgiParameters`, so `csrf_token` inside JSON is not found;
2. `csrf_token` form field in POST body: works only with `Content-Type: application/x-www-form-urlencoded` (plain `<form method="POST">` or `URLSearchParams` body), because forms cannot set custom request headers.

Both approaches are valid. A form is useful when a JavaScript-free fallback is required (for example, Disable Self-Heal page in BlobStorage Controller, see [`self_heal.cpp`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/mind/bscontroller/self_heal.cpp)); in other cases, prefer `fetch` with `X-CSRF-Token` header because it fits dynamic UI better and is the only option for JSON bodies (as in [`state_storage_state.js`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/cms/ui/state_storage_state.js)).

{% note alert %}

Do not send state-changing POST requests without a CSRF token.

```cpp
str << "<form method='POST' action=''>\n";
str << "  <input type='hidden' name='restartPDisk' value='1'>\n";
str << "  <button type='submit'>Restart</button>\n";  // no csrf_token field
str << "</form>\n";
```

{% endnote %}

#### Option A: `<form>` with hidden `csrf_token` field

Server handler reads `csrf_token` cookie from incoming `TEvRemoteHttpInfo` (via `ev->Get()->GetCookie("csrf_token")`) and passes it to rendering code. Token must be escaped for HTML attribute context (a small inline escaper as in [`self_heal.cpp`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/mind/bscontroller/self_heal.cpp) or `HtmlEscape`; see [{#T}](#output-escaping)):

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

#### Option B: `fetch` from `<script nonce='...'>` with `X-CSRF-Token` header

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

#### Option B with `$.ajax`

Same approach with jQuery, if the page already uses it:

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

Example of this pattern in repository: [`state_storage_state.js`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/cms/ui/state_storage_state.js) (`loadDistconfStatus`), where POST reads `csrf_token` from `document.cookie` and sends it via `X-CSRF-Token`.

### GET requests and state changes {#get-no-side-effects}

GET requests are not CSRF-protected: `CheckCsrfToken` validates token only for `POST`/`PUT`/`DELETE`/`PATCH` (see [`mon.cpp`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/mon/mon.cpp), `IsCsrfProtectedMethod`). If a page needs to trigger action (restart, stop, reconfigure), use a protected method, typically POST.

{% note alert %}

Do not perform state-changing operations in a GET handler.

```cpp
void RenderPage(IOutputStream& str, const TCgiParameters& params) {
    if (params.Has("action")) {
        DoSomethingDestructive(); // side effect from GET
    }
    // ... render HTML
}
```

{% endnote %}

Separate GET (render) and POST (action):

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

## Inline event handlers {#no-inline-handlers}

Inline event handlers (`onclick="..."`, `onchange="..."`, etc.) are blocked by CSP `script-src` even when nonce is present, because nonce applies to `<script>` blocks only, not inline attributes.

{% note alert %}

Do not use inline event handlers in HTML attributes (`onclick`, `onchange`, etc.).

```cpp
str << "<input type='checkbox' id='ignoreChecks' onchange='toggleButtonColor()'>";
str << "<button onclick='sendRestartRequest()'>Restart</button>";
```

{% endnote %}

Attach handlers from `<script nonce='...'>` block:

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

## Output escaping {#output-escaping}

Any user-controlled or externally sourced data rendered into HTML must be escaped.

{% note alert %}

Do not render user or external data into HTML without escaping.

```cpp
TABLED() { str << pathName; }           // pathName may contain <, >, &, "
TABLED() { str << errorMessage; }       // error messages may contain HTML
```

{% endnote %}

Escaped output with `HtmlEscape`:

```cpp
#include <util/string/html.h>

TABLED() { str << HtmlEscape(pathName); }
TABLED() { str << HtmlEscape(errorMessage); }
```

Use URL-encoding for values in `href`. Example below focuses only on escaping values; paths themselves should remain relative (see [{#T}](#relative-links)):

```cpp
str << "<a href='tablets?TabletID=" << tabletId << "'>";       // numeric value is safe
str << "<a href='path?name=" << CGIEscapeRet(name) << "'>";    // string value must be escaped
```

### Dynamic values and `<script>` {#no-script-interpolation}

JavaScript has its own escaping rules, and `HtmlEscape` does **not** cover them: it does not process `'`, `\`, line terminators (`U+2028`, `U+2029`), and `</script>` substrings. Values such as `O'Brien`, `foo\nbar`, or `</script><script>alert(1)//` break out of JS literals even after `HtmlEscape`. Instead of custom JS escaping on server side, keep script body fully static and read dynamic values from `data-*` attributes via standard `dataset` API; for attribute context, `HtmlEscape` is suitable.

{% note alert %}

Do not interpolate dynamic values directly into `<script>` blocks: this leads to [Cross-site scripting](https://en.wikipedia.org/wiki/Cross-site_scripting) (XSS).

```cpp
str << "<script nonce='" << nonce << "'>\n";
str << "  const tableName = '" << tableName << "';\n";              // raw value: trivial XSS
str << "  const errorText = '" << HtmlEscape(errorText) << "';\n";  // still vulnerable:
                                                                    // ', \ in value break literal
str << "</script>";
```

{% endnote %}

Pass values through HTML-escaped `data-*` attributes and read them in JS:

```cpp
str << "<div id='pageData'"
       " data-table-name='"  << HtmlEscape(tableName)  << "'"
       " data-error-text='"  << HtmlEscape(errorText)  << "'"
       " data-tablet-id='"   << tabletId               << "'"  // numeric value is safe
    << "></div>\n";

str << "<script nonce='" << nonce << "'>\n";
str << R"js(
    const el = document.getElementById('pageData');
    const tableName = el.dataset.tableName;   // values come from DOM, not script source
    const errorText = el.dataset.errorText;
    const tabletId  = Number(el.dataset.tabletId);
    // ... use tableName, errorText, tabletId
)js";
str << "</script>";
```

Script body remains a fixed string without interpolation, and each dynamic value travels through HTML attribute context where `HtmlEscape` is the right tool. The same method works for arrays and objects: serialize in C++ to a string, put into a single `data-...` attribute, then call `JSON.parse(el.dataset.items)` on the client.

Inline handlers (`onclick="..."`) and APIs such as `eval`/`setTimeout('...')` with string code are outside this specific recommendation; they are already restricted in [{#T}](#no-inline-handlers) and by `script-src` without `'unsafe-eval'`.

## HTTP responses and `GetHTTPOK()` {#get-httpok}

Build HTTP responses via [`TViewer::GetHTTPOK()`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/viewer/viewer.cpp) and related methods; do not assemble raw HTTP strings manually.

Set `charset=utf-8` in `Content-Type`; `GetHTTPOK()` does not add it automatically.

```cpp
ReplyAndPassAway(Viewer->GetHTTPOK(Request, "text/html; charset=utf-8", htmlContent));
```

{% note alert %}

Do not build raw HTTP response strings manually, and do not omit `charset=utf-8` in `Content-Type`.

```cpp
Send(Sender, new NMon::TEvHttpInfoRes("HTTP/1.1 200 Ok\r\n\r\n" + html));  // raw string
ReplyAndPassAway(Viewer->GetHTTPOK(Request, "text/html", htmlContent));     // missing charset
```

{% endnote %}

## See also {#see-also}

- [{#T}](../reference/embedded-ui/index.md)
- [{#T}](../security/index.md)
- [OWASP CSP Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Content_Security_Policy_Cheat_Sheet.html)
- [OWASP CSRF Prevention](https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html)
- [MDN: Content Security Policy](https://developer.mozilla.org/en-US/docs/Web/HTTP/CSP)
