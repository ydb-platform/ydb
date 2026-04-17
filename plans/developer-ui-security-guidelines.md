# Security Guidelines for C++ Developer UI (Monitoring Pages)

This document describes security requirements for C++ developers writing monitoring pages (Developer UI) in YDB. These pages are generated at runtime using `HTML(str) { ... }` macros and served by the built-in HTTP monitoring server.

Example of the pull request with CSRF protection and nonce handling in HTTP responses: [#36981](https://github.com/ydb-platform/ydb/pull/36981).

---

## 1. Content Security Policy (CSP) and `nonce`

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

**✅ CORRECT — pass nonce from request context and use it:**

```cpp
void RenderState(IOutputStream& str, THttpInfo& httpInfo) {
    const TString& nonce = httpInfo.Nonce; // obtained from request

    str << "<script nonce='" << nonce << "'>\n";
    str << "    function sendRestartRequest() {\n";
    str << "        fetch('', { method: 'POST', body: 'restartPDisk=' });\n";
    str << "    }\n";
    str << "</script>\n";
}
```

### Rule: NEVER weaken the CSP

Do not add `'unsafe-inline'`, `'unsafe-eval'`, or external domains to `script-src` or `style-src`. If a script doesn't work without `'unsafe-inline'`, rewrite it to use a nonce.

**❌ FORBIDDEN — weakening CSP:**

```cpp
response << "Content-Security-Policy: script-src 'unsafe-inline'\r\n";
response << "Content-Security-Policy: script-src 'self' https://cdn.example.com\r\n";
```

---

## 2. No External Resources

| Directive     | Allowed                      | Blocked                            |
| ------------- | ---------------------------- | ---------------------------------- |
| `script-src`  | `'self'` + nonce             | Any external script                |
| `style-src`   | `'self'` + `'unsafe-inline'` | External stylesheets               |
| `font-src`    | `'self'`                     | External fonts (Google Fonts etc.) |
| `connect-src` | `'self'` (same origin only)  | External `fetch()`/XHR             |
| `frame-src`   | `'self'`                     | External iframes                   |
| `img-src`     | `'self'`, `data:`, `https:`  | ✅ External images are allowed     |

### Rule: NEVER load scripts, styles, or fonts from external URLs

**❌ FORBIDDEN:**

```cpp
out << "<script src='https://code.jquery.com/jquery-3.6.0.min.js'></script>\n";
out << "<link href='https://fonts.googleapis.com/css?family=Roboto' rel='stylesheet'>\n";
```

**✅ CORRECT — use only resources served from the same origin (`/static/`)**

Bootstrap, jQuery, and tablesorter are already bundled and served from `/static/`:

```cpp
// These are already included by the monitoring page wrapper — do NOT add them again:
// /static/css/bootstrap.min.css
// /static/js/jquery.min.js
// /static/js/bootstrap.min.js
```

If you need a library that is not yet bundled, add it to the embedded resources in [`ydb/core/viewer/`](../ydb/core/viewer/) and serve it from `/static/`.

### Rule: NEVER make `fetch()`/XHR requests to external URLs

**❌ FORBIDDEN:**

```cpp
str << "fetch('https://external-api.example.com/data')\n";
str << "$.ajax({ url: 'https://external-api.example.com/data' })\n";
```

**✅ CORRECT — only relative URLs or same-origin absolute URLs:**

```cpp
str << "fetch('')\n";           // relative — same URL as the page
str << "fetch('/api/data')\n";  // absolute path — same origin
```

### Rule: NEVER embed external iframes

**❌ FORBIDDEN:**

```cpp
out << "<iframe src='https://external.example.com/widget'></iframe>\n";
```

---

## 3. CSRF Protection

### Rule: Use `fetch` for state-changing requests — NOT HTML forms

HTML `<form method="POST">` cannot set custom request headers. The BFF validates the `X-CSRF-Token` **header** (not a form field), so HTML forms are incompatible with CSRF protection and are forbidden for any state-changing operation.

**❌ FORBIDDEN — HTML form POST:**

```cpp
str << "<form method='POST' action=''>\n";
str << "  <input type='hidden' name='restartPDisk' value='1'>\n";
str << "  <button type='submit'>Restart</button>\n";
str << "</form>\n";
```

**✅ CORRECT — use `fetch` from a `<script nonce='...'>` block:**

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

### Rule: All state-changing requests MUST include a CSRF token

Include the `X-CSRF-Token` header in every POST/PUT/DELETE request. If the `CSRF-TOKEN` cookie is absent (e.g., local deployment without BFF), skip the header — do not block the request.

**❌ FORBIDDEN — POST without CSRF token:**

```cpp
str << "<script nonce='" << nonce << "'>\n";
str << R"js(
    function sendRestartRequest() {
        fetch('', {
            method: 'POST',
            body: 'restartPDisk='
            // ← missing X-CSRF-Token header!
        });
    }
)js";
str << "</script>\n";
```

**✅ CORRECT — use the global `getCsrfToken()` helper with `fetch`:**

```cpp
str << "<script nonce='" << nonce << "'>\n";
str << R"js(
    function sendRestartRequest() {
        // getCsrfToken() is provided globally by /static/js/monitoring.js
        const csrfToken = getCsrfToken();
        const headers = { 'Content-Type': 'application/x-www-form-urlencoded' };
        if (csrfToken) {
            headers['X-CSRF-Token'] = csrfToken;
        }
        // Use a relative URL ('') so the request is always same-origin.
        fetch('', {
            method: 'POST',
            headers: headers,
            body: 'restartPDisk='
        });
    }
)js";
str << "</script>\n";
```

**✅ CORRECT — same with `$.ajax` (if jQuery is already used on the page):**

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

### Rule: GET handlers MUST NOT perform any state-changing operations

GET requests are not CSRF-protected. If your page needs to trigger an action (restart, stop, reconfigure), use POST.

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

---

## 6. Use `GetHTTPOK()` for HTTP Responses

Always use [`TViewer::GetHTTPOK()`](../ydb/core/viewer/viewer.cpp:917) and related methods to build HTTP responses — **do not build raw HTTP response strings manually**.

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
