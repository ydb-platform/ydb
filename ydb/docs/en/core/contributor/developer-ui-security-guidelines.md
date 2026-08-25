# Developer UI Security Guidelines

This article is a security checklist for {{ ydb-short-name }} developers and contributors who write C++ monitoring pages ([Developer UI](../../reference/embedded-ui/index.md)). These pages are generated at runtime using `HTML(str) { ... }` macros and are served by the built-in monitoring HTTP server.

This article covers [Content Security Policy](https://developer.mozilla.org/en-US/docs/Web/HTTP/CSP) (CSP), [Cross-Site Request Forgery](https://en.wikipedia.org/wiki/Cross-site_request_forgery) (CSRF) protection, and safe HTML output.

The CSP (`nonce`) and CSRF mechanisms in the monitoring HTTP layer are described below based on the current code behavior; they were introduced in pull request [#36981](https://github.com/ydb-platform/ydb/pull/36981).

## Content Security Policy (CSP) and nonce {#csp-and-nonce}

{% note info %}

**Current implementation.** Monitoring responses include one CSP directive:

```http
Content-Security-Policy: script-src 'nonce-AbCd…=='
```

The header does not include `style-src`, `font-src`, `connect-src`, `frame-src`, `img-src`, or `default-src`. In the current version, the browser enforces restrictions only for `<script>` execution; rules below for other resource types are defensive coding recommendations for forward compatibility, not browser-enforced requirements yet.

Nonce-to-CSP-header translation happens in [`THttpMonLegacyActorRequest::Handle(TEvHttpInfoRes…)`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/mon/mon.cpp) (the legacy monitoring path that delivers `TEvHttpInfoRes`/`TEvRemoteHttpInfoRes`). Handlers that reply with raw `THttpOutgoingResponse` must ensure security on their own.

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

Page rendering code generates a nonce for every response, injects it into all inline `<script>` tags, and writes it to `res->Nonce` of the outgoing `TEvRemoteHttpInfoRes` / `TEvHttpInfoRes`. The monitoring framework provides [`NActors::NMon::GenerateCspNonce()`](https://github.com/ydb-platform/ydb/blob/main/ydb/library/actors/core/mon.h) — a random base64-encoded GUID. The HTTP layer automatically adds the `Content-Security-Policy: script-src 'nonce-<value>'` header; the CSP header does not need to be built manually.

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

For pages served via `TEvHttpInfoRes` (local mon, without proxying through [tablets](../../concepts/glossary.md#tablet)), the same `res->Nonce = nonce` assignment applies — see `Notify(...)` in [`tablet_monitoring_proxy.cpp`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/tablet/tablet_monitoring_proxy.cpp). Nonce values are not reused between responses: a new value is generated for each `OnRenderAppHtmlPage` call.

When a response is forwarded between nodes, the nonce is preserved: [`TEvRemoteHttpInfoRes::SerializeToArcadiaStream`](https://github.com/ydb-platform/ydb/blob/main/ydb/library/actors/core/mon.cpp) packs it together with the HTML, so the same approach works for remote tablet monitoring.

### `script-src` policy {#script-src-csp}

`'unsafe-inline'`, `'unsafe-eval'`, and external domains are not added to `script-src`. If a script does not work without `'unsafe-inline'`, rewrite it using a nonce (see [{#T}](#inline-script-nonce)).

{% note alert %}

Weakening `script-src` with `'unsafe-inline'`, `'unsafe-eval'`, or external domains disables CSP protection.

```cpp
response << "Content-Security-Policy: script-src 'unsafe-inline'\r\n";
response << "Content-Security-Policy: script-src 'self' https://cdn.example.com\r\n";
```

{% endnote %}

### Inline styles {#inline-styles}

The current CSP header has no `style-src` directive, so inline styles (`style="..."` attributes and `<style>` blocks) are not blocked by the browser. They are widely used on existing Developer UI pages (hive monitoring, pdisk, tablet_flat, graph, cms, columnshard, tracing, and so on); those usages are planned for migration, followed by adding a stricter `style-src` to the header.

Lack of blocking in the current version does not mean new inline styles should be added.

{% note warning %}

For new code, avoid adding inline styles — `style="..."` attributes and `<style>` blocks.

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

When a stricter `style-src` is added to the header, it is not weakened with `'unsafe-inline'` or external domains.

## External resources {#no-external-resources}

{% note info %}

**Enforcement status.** Only the `script-src` row in the table below is currently enforced by the CSP header. The other rows describe the target policy the codebase is moving toward; following it in new code allows enabling a stricter header later without breaking the UI.

{% endnote %}

| Directive | Target policy | Enforced now? |
| --- | --- | --- |
| `script-src` | `'self'` + nonce, no external scripts | Yes — `script-src 'nonce-…'` |
| `style-src` | `'self'` only, no external stylesheets | No — directive is not in the header (see [{#T}](#csp-and-nonce)) |
| `font-src` | `'self'`, no external fonts | No — directive is not in the header |
| `connect-src` | `'self'`, no external `fetch()`/XMLHttpRequest (XHR) | No — directive is not in the header |
| `frame-src` | `'self'`, no external iframes | No — directive is not in the header |
| `img-src` | `'self'` and `data:`, no external URLs | No — directive is not in the header |

### Relative links in HTML {#relative-links}

Monitoring pages may be served under different prefixes, so generated HTML does not hardcode absolute paths. Only relative links are used in `href`, `src`, `action`, `formaction`, `fetch()`, `$.ajax()`, and similar attributes. The following are not used:

- full URLs: `https://example.com/...`;
- scheme-less (protocol-relative) URLs: `//example.com/...`;
- root-relative paths: `/get_blob`, `/static/js/...`.

{% note alert %}

Absolute URLs and root-relative paths are not used in generated HTML.

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

If a page needs a link to product documentation or another external page, route it through a relative internal page or redirect, or render plain text instead of a clickable external link.

### Loading scripts, styles, and fonts {#no-external-scripts}

Resources are loaded from the same origin, without external links.

{% note alert %}

External URLs for scripts, styles, and fonts are incompatible with the target CSP policy.

```cpp
out << "<script src='https://code.jquery.com/jquery-3.6.0.min.js'></script>\n";
out << "<link href='https://fonts.googleapis.com/css?family=Roboto' rel='stylesheet'>\n";
```

{% endnote %}

Bootstrap, jQuery, and tablesorter are already included in the bundled resources and served by the monitoring page wrapper. C++ page rendering code usually does not add extra `<script>`/`<link>` tags for them.

If a page needs to reference a bundled resource, the relative-link rule applies: root-relative paths such as `/static/js/jquery.min.js` or `/jquery.tablesorter.js` are not hard-coded.

If a library is not yet among the bundled resources, add it under [`ydb/core/viewer/`](https://github.com/ydb-platform/ydb/tree/main/ydb/core/viewer) and wire it through the monitoring wrapper or a helper, without external links or root-relative paths in the C++ page.

### `fetch()` and XHR requests {#no-absolute-fetch}

The same rule applies to JavaScript requests: only relative URLs.

{% note alert %}

Absolute URLs and root-relative paths are not used for `fetch()` and XHR requests.

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
str << "fetch('../api/data')\n"; // relative path to a sibling/parent endpoint
```

### Embedding iframes {#no-external-iframes}

Only same-origin resources are allowed in iframes.

{% note alert %}

External iframes are incompatible with the target `frame-src` policy.

```cpp
out << "<iframe src='https://external.example.com/widget'></iframe>\n";
```

{% endnote %}

## CSRF protection {#csrf-protection}

The monitoring HTTP layer implements CSRF protection using the [double-submit cookie](https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html#alternative-using-a-double-submit-cookie-pattern) pattern:

- on any response, the server sets a `csrf_token` cookie (random GUID, `SameSite=Strict; Path=/`) if it is not already set. The cookie intentionally has no `HttpOnly` flag (the double-submit cookie pattern requires reading the value from JS) and no `Secure` flag (the monitoring HTTP layer does not know whether it runs behind TLS);
- for state-changing methods (POST/PUT/DELETE/PATCH), the server compares the `csrf_token` cookie with the `X-CSRF-Token` request header or the `csrf_token` form parameter. On mismatch, it returns `403 FORBIDDEN`;
- CSRF checks are skipped when the request is not cookie-based (no `ydb_session_id` cookie) — for example, for API clients with `Authorization` headers;
- if the `ydb_session_id` cookie is present but the `csrf_token` cookie has not been issued yet (for example, the first POST right after login with no preceding GET), the request is rejected. At least one GET is required before a state-changing request.

There is no shared monitoring JS bundle, so each inline `<script>` that performs a POST reads the `csrf_token` cookie itself. A small `getCsrfToken()` helper with the same cookie name as on the server works for this:

```js
// Reads the csrf_token cookie set by the monitoring HTTP server.
// Returns an empty string if the cookie is missing (for example, a local
// stand or the first request before any response has set it) —
// the server accepts the request only if CSRF protection is not required for it.
function getCsrfToken() {
  return document.cookie.match(/(?:^|;\s*)csrf_token=([^;]*)/)?.[1] || '';
}
```

The function can be inlined in every `<script nonce='...'>` block that needs it, or — preferably — emitted once per page from a shared helper (for example, `RenderCsrfTokenHelper(str, nonce)` next to the page rendering code).

### CSRF token in state-changing requests {#csrf-token-required}

The monitoring server accepts the token from one of two places (see `CheckCsrfToken` in [`mon.cpp`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/mon/mon.cpp)):

1. The `X-CSRF-Token` request header — preferred for `fetch`/`$.ajax`. Required for any body that is not form-encoded (in particular, for JSON): the server parses the body as `TCgiParameters`, so a `csrf_token` field inside JSON is not found.
2. The `csrf_token` form parameter in the POST body — works only with `Content-Type: application/x-www-form-urlencoded` (a plain `<form method="POST">` or a `URLSearchParams` body), because forms cannot set arbitrary request headers.

Both approaches are valid. A form is appropriate when a no-JavaScript fallback is needed (as on the BlobStorage controller Disable Self-Heal page — see [`self_heal.cpp`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/mind/bscontroller/self_heal.cpp)). Otherwise prefer `fetch` with the `X-CSRF-Token` header — it fits dynamic UI better and is the only option for JSON bodies (as in [`state_storage_state.js`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/cms/ui/state_storage_state.js)).

{% note alert %}

A POST without a CSRF token is rejected by the server.

```cpp
str << "<form method='POST' action=''>\n";
str << "  <input type='hidden' name='restartPDisk' value='1'>\n";
str << "  <button type='submit'>Restart</button>\n";  // ← no csrf_token field
str << "</form>\n";
```

{% endnote %}

#### Option A: `<form>` with a hidden `csrf_token` field

The server handler reads the `csrf_token` cookie from the incoming `TEvRemoteHttpInfo` (via `ev->Get()->GetCookie("csrf_token")`) and passes it to the rendering code. The token is HTML-escaped when inserted into the attribute value (a small inline escape as in [`self_heal.cpp`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/mind/bscontroller/self_heal.cpp) or `HtmlEscape` — see [{#T}](#output-escaping)):

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

#### Option B: `fetch` from a `<script nonce='...'>` block with the `X-CSRF-Token` header

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

The same idea with jQuery if the page already uses it:

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

An example of this pattern in the repository is [`state_storage_state.js`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/cms/ui/state_storage_state.js) (`loadDistconfStatus`): a POST that reads `csrf_token` from `document.cookie` and sends it as `X-CSRF-Token`.

### GET requests and state changes {#get-no-side-effects}

GET requests are not CSRF-protected: `CheckCsrfToken` validates the token only for `POST`/`PUT`/`DELETE`/`PATCH` (see [`mon.cpp`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/mon/mon.cpp), `IsCsrfProtectedMethod`). If a page needs to trigger an action (restart, stop, reconfiguration), one of the protected methods is used — usually POST.

{% note alert %}

Performing a state-changing operation in a GET handler leads to a CSRF vulnerability.

```cpp
void RenderPage(IOutputStream& str, const TCgiParameters& params) {
    if (params.Has("action")) {
        DoSomethingDestructive(); // ← side effect from GET!
    }
    // ... render HTML
}
```

{% endnote %}

Separating GET (render) and POST (action):

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

Inline event handlers (`onclick="..."`, `onchange="..."`, and so on) are blocked by the CSP `script-src` policy even when a nonce is present, because the nonce applies only to `<script>` blocks, not to inline attributes.

{% note alert %}

HTML attributes such as `onclick` and `onchange` are not used to attach event handlers.

```cpp
str << "<input type='checkbox' id='ignoreChecks' onchange='toggleButtonColor()'>";
str << "<button onclick='sendRestartRequest()'>Restart</button>";
```

{% endnote %}

Attaching handlers from a `<script nonce='...'>` block:

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

Any user-controlled or externally obtained data rendered into HTML is escaped.

{% note alert %}

User-controlled and external data without escaping can lead to markup injection.

```cpp
TABLED() { str << pathName; }           // pathName may contain <, >, &, "
TABLED() { str << errorMessage; }       // error messages may contain HTML
```

{% endnote %}

Output with `HtmlEscape`:

```cpp
#include <util/string/html.h>

TABLED() { str << HtmlEscape(pathName); }
TABLED() { str << HtmlEscape(errorMessage); }
```

For URLs in `href` attributes, URL encoding is applied. The example below shows value escaping only; the paths themselves must stay relative (see [{#T}](#relative-links)):

```cpp
str << "<a href='tablets?TabletID=" << tabletId << "'>";       // number — safe
str << "<a href='path?name=" << CGIEscapeRet(name) << "'>";    // string — must be escaped
```

### Dynamic values and `<script>` {#no-script-interpolation}

JavaScript has its own escaping rules, and `HtmlEscape` does **not** cover them: it does not handle `'`, `\`, line terminators (`U+2028`, `U+2029`), or `</script>` substrings. Values such as `O'Brien`, `foo\nbar`, or `</script><script>alert(1)//` break out of a JS literal even after `HtmlEscape`.

Instead of inventing JS escaping on the server, keep the script body fully static and read dynamic values from `data-*` attributes via the standard `dataset` API — the attribute context is handled correctly by `HtmlEscape`.

{% note alert %}

Interpolating dynamic values into a `<script>` block leads to [cross-site scripting](https://en.wikipedia.org/wiki/Cross-site_scripting) (XSS).

```cpp
str << "<script nonce='" << nonce << "'>\n";
str << "  const tableName = '" << tableName << "';\n";              // raw value: trivial XSS
str << "  const errorText = '" << HtmlEscape(errorText) << "';\n";  // still vulnerable:
                                                                    // ', \ in the value break the literal
str << "</script>";
```

{% endnote %}

Passing values through HTML-escaped `data-*` attributes and reading them from JS:

```cpp
str << "<div id='pageData'"
       " data-table-name='"  << HtmlEscape(tableName)  << "'"
       " data-error-text='"  << HtmlEscape(errorText)  << "'"
       " data-tablet-id='"   << tabletId               << "'"  // number — safe
    << "></div>\n";

str << "<script nonce='" << nonce << "'>\n";
str << R"js(
    const el = document.getElementById('pageData');
    const tableName = el.dataset.tableName;   // values from the DOM, not from the script source
    const errorText = el.dataset.errorText;
    const tabletId  = Number(el.dataset.tabletId);
    // ... use tableName, errorText, tabletId
)js";
str << "</script>";
```

The script body stays a fixed string with no interpolation, and each dynamic value goes through the HTML attribute context where `HtmlEscape` is the right tool. The same approach works for arrays and objects: serialize them to a string in C++, put them in one `data-...` attribute, and call `JSON.parse(el.dataset.items)` on the client.

Inline handlers (`onclick="..."`) and APIs such as `eval`/`setTimeout('...')` with string code are outside this recommendation — they are already covered in [{#T}](#no-inline-handlers) and by the `script-src` policy without `'unsafe-eval'`.

## HTTP responses and `GetHTTPOK()` {#get-httpok}

HTTP responses are built through [`TViewer::GetHTTPOK()`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/viewer/viewer.cpp) and related methods; raw HTTP strings are not assembled manually.

The `Content-Type` header includes `charset=utf-8` — `GetHTTPOK()` does not add it automatically.

```cpp
ReplyAndPassAway(Viewer->GetHTTPOK(Request, "text/html; charset=utf-8", htmlContent));
```

{% note alert %}

Responses are built through `GetHTTPOK()` with `charset=utf-8` in `Content-Type`. A raw HTTP string or a response without charset bypasses this path.

```cpp
Send(Sender, new NMon::TEvHttpInfoRes("HTTP/1.1 200 Ok\r\n\r\n" + html));  // raw string
ReplyAndPassAway(Viewer->GetHTTPOK(Request, "text/html", htmlContent));     // no charset
```

{% endnote %}

## See also {#see-also}

- [{#T}](../../reference/embedded-ui/index.md)
- [{#T}](../../security/index.md)
- [OWASP CSP Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Content_Security_Policy_Cheat_Sheet.html)
- [OWASP CSRF Prevention](https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html)
- [MDN: Content Security Policy](https://developer.mozilla.org/en-US/docs/Web/HTTP/CSP)
