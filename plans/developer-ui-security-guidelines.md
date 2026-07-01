# Security Guidelines for C++ Developer UI (Monitoring Pages)

This document describes security requirements for C++ developers writing monitoring pages (Developer UI) in YDB. These pages are generated at runtime using `HTML(str) { ... }` macros and served by the built-in HTTP monitoring server.

Example of the pull request with CSRF protection and nonce handling in HTTP responses: [#36981](https://github.com/ydb-platform/ydb/pull/36981).

---

## 1. Content Security Policy (CSP) and `nonce`

> **What the PR actually enforces.** PR [#36981](https://github.com/ydb-platform/ydb/pull/36981) sets exactly one CSP directive on monitoring responses:
>
> ```http
> Content-Security-Policy: script-src 'nonce-AbCd…=='
> ```
>
> There is **no** `style-src`, `font-src`, `connect-src`, `frame-src`, `img-src`, or `default-src` in the emitted header. So today only `<script>` execution is policed by the browser; rules below for other resource types are **defensive coding guidelines** for forward-compatibility, not browser-enforced.
>
> The nonce → CSP-header translation happens in [`THttpMonLegacyActorRequest::Handle(TEvHttpInfoRes…)`](../ydb/core/mon/mon.cpp) (the legacy monitoring path that delivers `TEvHttpInfoRes`/`TEvRemoteHttpInfoRes`). Handlers that reply with a raw `THttpOutgoingResponse` are on their own.

### Rule: All inline `<script>` tags MUST use `nonce`

**❌ FORBIDDEN — inline script without nonce:**

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

**✅ CORRECT — generate a nonce per response, attach it to the response event, and use it in `<script>`:**

The monitoring framework (see [#36981](https://github.com/ydb-platform/ydb/pull/36981)) provides [`NActors::NMon::GenerateCspNonce()`](../ydb/library/actors/core/mon.h) — a base64-encoded random GUID. The renderer generates the nonce, uses it in every inline `<script>`, and assigns it to `res->Nonce` on the outgoing `TEvRemoteHttpInfoRes` / `TEvHttpInfoRes`. The HTTP layer then automatically emits a matching `Content-Security-Policy: script-src 'nonce-<value>'` header — you do **not** write the CSP header by hand.

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

For pages served via `TEvHttpInfoRes` (local mon, not forwarded through tablets), the same `res->Nonce = nonce` assignment applies — see `Notify(...)` in [`tablet_monitoring_proxy.cpp`](../ydb/core/tablet/tablet_monitoring_proxy.cpp). Do **not** reuse a nonce across responses — generate a fresh one each time `OnRenderAppHtmlPage` is invoked.

The nonce is preserved when the response is forwarded across nodes: [`TEvRemoteHttpInfoRes::SerializeToArcadiaStream`](../ydb/library/actors/core/mon.cpp) packs it alongside the HTML, so the same pattern works for remote tablet monitoring.

### Rule: NEVER weaken the `script-src` CSP

Do not add `'unsafe-inline'`, `'unsafe-eval'`, or external domains to `script-src`. If a script doesn't work without `'unsafe-inline'`, rewrite it to use a nonce (see the rule above).

**❌ FORBIDDEN — weakening `script-src`:**

```cpp
response << "Content-Security-Policy: script-src 'unsafe-inline'\r\n";
response << "Content-Security-Policy: script-src 'self' https://cdn.example.com\r\n";
```

### Rule: Avoid new inline styles even though CSP does not block them today

There is no `style-src` directive in the CSP header set by PR [#36981](https://github.com/ydb-platform/ydb/pull/36981), so inline styles (`style="..."` attributes and inline `<style>` blocks) are not blocked by the browser today. They are used pervasively across existing Developer UI pages (hive monitoring, pdisk, tablet_flat, graph, cms, columnshard, tracing, etc.); the plan is to migrate those usages and later add a stricter `style-src` to the header.

This is **not** an invitation to add new inline styles. For new code:

**❌ AVOID — new inline styles:**

```cpp
str << "<div style='color:red; margin:5px'>...</div>";
str << "<style>.my-table th { text-align: center; }</style>";
```

**✅ PREFER — put styles into a static CSS file served from the same origin:**

```cpp
// In ydb/core/viewer/.../monitoring.css (served from /static/):
//   .mon-warning { color: red; margin: 5px; }
//   .mon-table th { text-align: center; }

str << "<div class='mon-warning'>...</div>";
```

When a stricter `style-src` is eventually added to the header, do **not** weaken it with `'unsafe-eval'` or external domains.

---

## 2. No External Resources

> **Enforcement status.** Only the `script-src` row below is enforced by the CSP header from PR [#36981](https://github.com/ydb-platform/ydb/pull/36981). The other rows describe the **target policy** the codebase is moving toward — follow them in new code so that turning on the stricter header later does not break the UI.

| Directive     | Target policy                               | Enforced today?                    |
| ------------- | ------------------------------------------- | ---------------------------------- |
| `script-src`  | `'self'` + nonce, no external scripts       | ✅ Yes — `script-src 'nonce-…'`    |
| `style-src`   | `'self'` only, no external stylesheets      | ❌ No directive in header (see §1) |
| `font-src`    | `'self'`, no external fonts                 | ❌ No directive in header          |
| `connect-src` | `'self'`, no external `fetch()`/XHR         | ❌ No directive in header          |
| `frame-src`   | `'self'`, no external iframes               | ❌ No directive in header          |
| `img-src`     | `'self'` and `data:` only, no external URLs | ❌ No directive in header          |

### Rule: Use only relative links in HTML generated from C++

Monitoring pages may be served under different prefixes, so generated HTML must not hardcode absolute locations. In `href`, `src`, `action`, `formaction`, `fetch()`, `$.ajax()`, etc. use only relative links. Do not use:

- full URLs: `https://example.com/...`;
- protocol-relative URLs: `//example.com/...`;
- root-relative paths: `/get_blob`, `/static/js/...`.

**❌ FORBIDDEN:**

```cpp
out << "<a href='https://ydb.tech/docs'>docs</a>\n";
out << "<button type='submit' formaction='/get_blob'>Query</button>\n";
out << "fetch('/api/data')\n";
```

**✅ CORRECT:**

```cpp
out << "<a href='docs'>docs</a>\n";
out << "<button type='submit' formaction='get_blob'>Query</button>\n";
out << "fetch('api/data')\n";
```

If a page must reference product documentation or any other external page, route it through a relative internal documentation page/redirect, or render plain text instead of a clickable external link.

### Rule: NEVER load scripts, styles, or fonts from external URLs

**❌ FORBIDDEN:**

```cpp
out << "<script src='https://code.jquery.com/jquery-3.6.0.min.js'></script>\n";
out << "<link href='https://fonts.googleapis.com/css?family=Roboto' rel='stylesheet'>\n";
```

**✅ CORRECT — use only resources served from the same origin**

Bootstrap, jQuery, and tablesorter are already bundled and served by the monitoring page wrapper. Page-specific C++ renderers normally must not emit additional `<script>`/`<link>` tags for them.

If a page-specific renderer still needs to reference a bundled resource, follow the relative-link rule above: do not hardcode root-relative paths such as `/static/js/jquery.min.js` or `/jquery.tablesorter.js`.

If you need a library that is not yet bundled, add it to the embedded resources in [`ydb/core/viewer/`](../ydb/core/viewer/) and expose it through the monitoring wrapper/helper without introducing external or root-relative links in page C++.

### Rule: NEVER make `fetch()`/XHR requests to absolute links

This is the same rule for JavaScript requests: use relative URLs only.

**❌ FORBIDDEN:**

```cpp
str << "fetch('https://external-api.example.com/data')\n";
str << "fetch('/api/data')\n";
str << "$.ajax({ url: '/api/data' })\n";
```

**✅ CORRECT:**

```cpp
str << "fetch('')\n";           // same URL as the page
str << "fetch('api/data')\n";   // relative to the current page
str << "fetch('../api/data')\n"; // relative path to a sibling/parent endpoint
```

### Rule: NEVER embed external iframes

**❌ FORBIDDEN:**

```cpp
out << "<iframe src='https://external.example.com/widget'></iframe>\n";
```

---

## 3. CSRF Protection

The monitoring HTTP layer implements CSRF protection via the double-submit cookie pattern (see PR [#36981](https://github.com/ydb-platform/ydb/pull/36981)):

- On any response the server sets a `csrf_token` cookie (random GUID, `SameSite=Strict; Path=/`) if it is not already present. The cookie is intentionally **not** `HttpOnly` (the double-submit pattern requires JS to read it) and **not** `Secure` (the monitoring HTTP layer does not know whether it is behind TLS).
- For state-changing methods (POST/PUT/DELETE/PATCH), the server compares the `csrf_token` cookie with either the `X-CSRF-Token` request header or a `csrf_token` form parameter; a mismatch results in `403 FORBIDDEN`.
- CSRF check is skipped when the request is not cookie-based (no `ydb_session_id` cookie) — e.g. API clients using `Authorization` headers.
- If `ydb_session_id` **is** present but the `csrf_token` cookie has not been issued yet (e.g. first POST immediately after login, with no preceding GET), the request is rejected — make sure the UI performs at least one GET before any state-changing request.

Since there is no shared monitoring JS bundle, each inline `<script>` that performs a POST must read the `csrf_token` cookie itself. Use this small `getCsrfToken()` helper, matching the cookie name used by the server in [#36981](https://github.com/ydb-platform/ydb/pull/36981):

```js
// Reads the `csrf_token` cookie set by the monitoring HTTP server.
// Returns an empty string when the cookie is absent (e.g. local deployment,
// or first request before any response has set the cookie) — the server will
// accept the request only if it does not require CSRF protection.
function getCsrfToken() {
  return document.cookie.match(/(?:^|;\s*)csrf_token=([^;]*)/)?.[1] || '';
}
```

You can either inline this function into every `<script nonce='...'>` block that needs it, or — preferred — emit it once per page from a shared helper (e.g. a `RenderCsrfTokenHelper(str, nonce)` function placed next to your page renderer).

### Rule: Every state-changing request MUST carry a CSRF token — header _or_ hidden form field

The monitoring server accepts the token from either of two places (see `CheckCsrfToken` in [`mon.cpp`](../ydb/core/mon/mon.cpp)):

1. The `X-CSRF-Token` request header — preferred for `fetch`/`$.ajax` calls. **Required for any non-form-encoded body** (in particular, JSON requests): the server parses the body as `TCgiParameters`, so a `csrf_token` field inside a JSON payload will not be found.
2. A `csrf_token` form parameter in the POST body — works **only** for `Content-Type: application/x-www-form-urlencoded` (i.e. a plain `<form method="POST">` or `URLSearchParams` body), because forms cannot set custom request headers.

Either approach is acceptable. Use a form only when you genuinely need a no-JS fallback (the BlobStorage Controller's "Disable Self-Heal" page in the PR is one such case); otherwise prefer `fetch` with the `X-CSRF-Token` header — it composes better with dynamic UI and is the only option for JSON bodies (as in [`state_storage_state.js`](../ydb/core/cms/ui/state_storage_state.js)).

**❌ FORBIDDEN — POST without any CSRF token:**

```cpp
str << "<form method='POST' action=''>\n";
str << "  <input type='hidden' name='restartPDisk' value='1'>\n";
str << "  <button type='submit'>Restart</button>\n";  // ← no csrf_token field!
str << "</form>\n";
```

**✅ CORRECT (option A) — `<form>` with a hidden `csrf_token` field:**

The server-side handler must read the `csrf_token` cookie from the incoming `TEvRemoteHttpInfo` (via `ev->Get()->GetCookie("csrf_token")`) and pass it into the renderer. The token must be HTML-escaped when embedded into an attribute value (use a small inline escaper as in [`self_heal.cpp`](../ydb/core/mind/bscontroller/self_heal.cpp), or `HtmlEscape` — see §5):

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

**✅ CORRECT (option B) — `fetch` from a `<script nonce='...'>` block with the `X-CSRF-Token` header:**

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

**✅ CORRECT (option B, `$.ajax` variant) — same idea with jQuery, if the page already uses it:**

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

For an in-repo example of this pattern see [`state_storage_state.js`](../ydb/core/cms/ui/state_storage_state.js) (`loadDistconfStatus`) — a `POST` that reads `csrf_token` from `document.cookie` and forwards it as `X-CSRF-Token`.

### Rule: GET handlers MUST NOT perform any state-changing operations

GET requests are not CSRF-protected: `CheckCsrfToken` only validates the token for `POST`/`PUT`/`DELETE`/`PATCH` (see [`mon.cpp`](../ydb/core/mon/mon.cpp), `IsCsrfProtectedMethod`). If your page needs to trigger an action (restart, stop, reconfigure), use one of the protected methods — POST is the conventional choice.

**❌ FORBIDDEN — side effect in GET handler:**

```cpp
void RenderPage(IOutputStream& str, const TCgiParameters& params) {
    if (params.Has("action")) {
        DoSomethingDestructive(); // ← side effect triggered by GET!
    }
    // ... render HTML
}
```

**✅ CORRECT — separate GET (render) from POST (action):**

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

---

## 4. No `onclick` and `onXxx` Inline Event Handlers

Inline event handlers (`onclick="..."`, `onchange="..."`, etc.) are blocked by CSP `script-src` policy even with a nonce, because the nonce applies only to `<script>` blocks, not to inline attributes.

**❌ FORBIDDEN — inline event handler:**

```cpp
str << "<input type='checkbox' id='ignoreChecks' onchange='toggleButtonColor()'>";
str << "<button onclick='sendRestartRequest()'>Restart</button>";
```

**✅ CORRECT — attach event listeners from a `<script nonce='...'>` block:**

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

---

## 5. Output Escaping

Any user-controlled or externally-sourced data rendered into HTML must be escaped.

**❌ FORBIDDEN — unescaped output:**

```cpp
TABLED() { str << pathName; }           // pathName may contain <, >, &, "
TABLED() { str << errorMessage; }       // error messages may contain HTML
```

**✅ CORRECT — use `HtmlEscape`:**

```cpp
#include <util/string/html.h>

TABLED() { str << HtmlEscape(pathName); }
TABLED() { str << HtmlEscape(errorMessage); }
```

For URLs in `href` attributes, use URL encoding:

```cpp
str << "<a href='/tablets?TabletID=" << tabletId << "'>";  // numeric — safe
str << "<a href='/path?name=" << CGIEscapeRet(name) << "'>";  // string — must escape
```

### Rule: NEVER interpolate dynamic values into `<script>` — pass them through `data-*` attributes

JavaScript has its own escaping rules, and `HtmlEscape` does **not** cover them: it does not touch `'`, `\`, line terminators (`U+2028`, `U+2029`), or `</script>` substrings. A value like `O'Brien`, `foo\nbar`, or `</script><script>alert(1)//` breaks out of a JS string literal even after `HtmlEscape`. Instead of reinventing JS-escaping on the server, keep the script body fully static and read dynamic values from `data-*` attributes via the standard `dataset` API — the attribute context is correctly handled by `HtmlEscape`.

**❌ FORBIDDEN — interpolating a value into a `<script>` block:**

```cpp
str << "<script nonce='" << nonce << "'>\n";
str << "  const tableName = '" << tableName << "';\n";              // raw: trivial XSS
str << "  const errorText = '" << HtmlEscape(errorText) << "';\n";  // still vulnerable:
                                                                    // a single ' or \ in the value breaks out.
str << "</script>";
```

**✅ CORRECT — emit values as HTML-escaped `data-*` attributes and read them from JS:**

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

This way the script body is a fixed string with no interpolation, and every dynamic value travels through an HTML-attribute context where `HtmlEscape` is the correct tool. The same approach works for arrays/objects — serialize them in C++ to a string and put into a single `data-...` attribute, then `JSON.parse(el.dataset.items)` on the client.

Inline event handlers (`onclick="..."`) and `eval`/`setTimeout('...')`-style string-eval APIs are out of scope for this rule — they are already forbidden by §4 and by `script-src` not allowing `'unsafe-eval'`.

---

## 6. Use `GetHTTPOK()` for HTTP Responses

Always use [`TViewer::GetHTTPOK()`](../ydb/core/viewer/viewer.cpp) and related methods to build HTTP responses — **do not build raw HTTP response strings manually**.

Always include `charset=utf-8` in the content type — `GetHTTPOK()` does not add it automatically.

**✅ CORRECT:**

```cpp
ReplyAndPassAway(Viewer->GetHTTPOK(Request, "text/html; charset=utf-8", htmlContent));
```

**❌ FORBIDDEN — raw HTTP string or missing charset:**

```cpp
Send(Sender, new NMon::TEvHttpInfoRes("HTTP/1.1 200 Ok\r\n\r\n" + html));  // raw string
ReplyAndPassAway(Viewer->GetHTTPOK(Request, "text/html", htmlContent));     // missing charset
```

---

## References

- [OWASP CSP Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Content_Security_Policy_Cheat_Sheet.html)
- [OWASP CSRF Prevention](https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html)
- [MDN: Content Security Policy](https://developer.mozilla.org/en-US/docs/Web/HTTP/CSP)
