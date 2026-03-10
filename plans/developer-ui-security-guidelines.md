# Security Guidelines for C++ Developer UI (Monitoring Pages)

This document describes security requirements for C++ developers writing monitoring pages (Developer UI) in YDB. These pages are generated at runtime using `HTML(str) { ... }` macros and served by the built-in HTTP monitoring server.

## Background

### How legacy pages are served

Legacy C++ HTML pages are generated at runtime using `HTML(str) { ... }` macros and sent as `NMon::TEvHttpInfoRes` responses. The page is rendered as a **standalone HTML document** returned directly to the browser.

There are two deployment modes with different security implications:

**Mode 1: Direct access** (local development, internal tools)

The browser connects directly to YDB's built-in HTTP server. No BFF is involved.

- No `CSRF-TOKEN` cookie is set → CSRF header is not required
- No `Content-Security-Policy` header is added by default → inline scripts work without nonce
- Rules about nonce and CSRF still apply as **future-proofing** — the infrastructure to enforce them will be added

**Mode 2: BFF proxy** (production, cloud deployments)

The browser connects to an Express-based BFF (e.g., `https://ydb.example.com`). The BFF proxies requests to YDB and adds security headers to all responses.

- The BFF sets `Content-Security-Policy` with nonce → inline scripts **must** use nonce or they are blocked
- The BFF sets `CSRF-TOKEN` cookie on authentication → POST requests **must** include `X-CSRF-Token` header
- The BFF validates the CSRF token before forwarding the request to YDB

### Boundary of responsibility

| Concern                              | Who is responsible                | Where                                        |
| ------------------------------------ | --------------------------------- | -------------------------------------------- |
| Generate CSP nonce                   | Infrastructure (HTTP server team) | `ydb/core/viewer/viewer.cpp` ⚠️ not yet done |
| Set `Content-Security-Policy` header | Infrastructure (HTTP server team) | `ydb/core/viewer/viewer.cpp` ⚠️ not yet done |
| Set `getCsrfToken()` global helper   | Infrastructure (HTTP server team) | `/static/js/monitoring.js` ⚠️ not yet done   |
| Validate CSRF token                  | BFF (external)                    | Express middleware                           |
| Set `CSRF-TOKEN` cookie              | BFF (external)                    | Express middleware                           |
| Use nonce in `<script>` tags         | **C++ page author (you)**         | Your rendering function                      |
| Send `X-CSRF-Token` in POST          | **C++ page author (you)**         | Your inline JS via `getCsrfToken()`          |
| Escape HTML output                   | **C++ page author (you)**         | Your rendering function                      |
| Use `GetHTTPOK()` for responses      | **C++ page author (you)**         | Your actor handler                           |

---

## 1. Content Security Policy (CSP) and `nonce`

### What is CSP nonce?

The HTTP server sets a `Content-Security-Policy` header that blocks all inline scripts by default. To allow a specific inline `<script>` block, it must carry a `nonce` attribute matching the value in the CSP header:

```
Content-Security-Policy: script-src 'self' 'nonce-<random_value>'
```

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
// The nonce value must be obtained from the HTTP request context
// and passed into the rendering function
void RenderState(IOutputStream& str, THttpInfo& httpInfo) {
    const TString& nonce = httpInfo.Nonce; // obtained from request

    str << "<script nonce='" << nonce << "'>\n";
    str << "    function sendRestartRequest() {\n";
    str << "        fetch('', { method: 'POST', body: 'restartPDisk=' });\n";
    str << "    }\n";
    str << "</script>\n";
}
```

### How to get the nonce

There are two roles here with different responsibilities:

**1. Infrastructure owner (viewer / HTTP server team) — ⚠️ not yet implemented**

Responsible for generating the nonce and making it available to rendering code. This work needs to be done in [`ydb/core/viewer/viewer.cpp`](../ydb/core/viewer/viewer.cpp) and [`ydb/core/mon/`](../ydb/core/mon/). The nonce must be:

- Generated once per HTTP request as a cryptographically random base64 string
- Included in the `Content-Security-Policy` response header
- Stored in the request context (`THttpInfo` or equivalent) so page authors can use it

```cpp
// TODO: add to viewer/HTTP server infrastructure:
TString nonce = GenerateCspNonce(); // random base64, e.g. "r4nd0mB4se64=="
response << "Content-Security-Policy: script-src 'self' 'nonce-" << nonce << "'\r\n";
httpInfo.Nonce = nonce; // pass to rendering functions
```

**2. C++ page author (you)**

Once the infrastructure above is in place, you receive the nonce through the `THttpInfo` context passed to your rendering function. Your only responsibility is to put it in every `<script>` tag you write:

```cpp
void MyComponent::RenderHtml(IOutputStream& str, const THttpInfo& httpInfo) {
    str << "<script nonce='" << httpInfo.Nonce << "'>\n";
    str << "    document.getElementById('btn').addEventListener('click', doAction);\n";
    str << "</script>\n";
}
```

You do **not** need to generate the nonce or set headers — just use `httpInfo.Nonce` in your `<script>` tags.

### Rule: NEVER weaken the CSP

Do not add `'unsafe-inline'`, `'unsafe-eval'`, or external domains to `script-src` or `style-src` in an attempt to "fix" a broken script. These directives are set by the BFF and must not be overridden or bypassed.

**❌ FORBIDDEN — weakening CSP:**

```cpp
// Never do this — it defeats the entire purpose of CSP:
response << "Content-Security-Policy: script-src 'unsafe-inline'\r\n";
response << "Content-Security-Policy: script-src 'self' https://cdn.example.com\r\n";
```

If a script doesn't work without `'unsafe-inline'`, the fix is to rewrite it to use a nonce — not to weaken the policy.

---

## 2. No External Resources

The BFF sets a `Content-Security-Policy` header on all responses. The relevant directives for legacy pages are:

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

`connect-src` only allows `'self'`. Any `fetch()` or `$.ajax()` to an external domain will be blocked.

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

`frame-src` only allows `'self'`. External iframes will be blocked.

**❌ FORBIDDEN:**

```cpp
out << "<iframe src='https://external.example.com/widget'></iframe>\n";
```

---

## 3. CSRF Protection

### What is CSRF?

Cross-Site Request Forgery (CSRF) allows a malicious website to make authenticated requests to YDB on behalf of a logged-in user. Any state-changing operation (POST, PUT, DELETE) must be protected.

### When is CSRF protection required?

CSRF protection is only relevant when YDB is accessed through an **external BFF (Backend-For-Frontend)** or reverse proxy that manages user sessions via cookies. In that deployment:

- The BFF sets a `CSRF-TOKEN` cookie when the user authenticates
- The BFF validates the `X-CSRF-Token` header on every state-changing request

When YDB is run **locally and serves static files directly** (e.g., during development), there are no session cookies from an external BFF, so CSRF protection is not needed and the `CSRF-TOKEN` cookie will not be present.

### How it works: Double Submit Cookie

YDB uses the **Double Submit Cookie** pattern:

1. The BFF sets a cookie on authentication: `Set-Cookie: CSRF-TOKEN=<random_value>; SameSite=Strict`
2. The browser stores this cookie for the domain
3. JavaScript reads the token from `document.cookie` and sends it as a request header: `X-CSRF-Token: <random_value>`
4. The BFF compares the header value with the cookie value — if they match, the request is legitimate

A malicious site on `evil.com` cannot read cookies from another domain, so it cannot forge the matching header value.

If the `CSRF-TOKEN` cookie is absent (local/direct deployment), the header is simply not sent and the request proceeds normally.

### Rule: Use `fetch` for state-changing requests — NOT HTML forms

HTML `<form method="POST">` cannot set custom request headers. Since the BFF validates the `X-CSRF-Token` **header** (not a form field), HTML forms are incompatible with CSRF protection and are forbidden for any state-changing operation.

**❌ FORBIDDEN — HTML form POST:**

```cpp
str << "<form method='POST' action=''>\n";
str << "  <input type='hidden' name='restartPDisk' value='1'>\n";
str << "  <button type='submit'>Restart</button>\n";
str << "</form>\n";
// ← cannot send X-CSRF-Token header, BFF will reject this
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

If the `CSRF-TOKEN` cookie is present, include it in every POST/PUT/DELETE request. If the cookie is absent (e.g., local deployment without BFF), skip the header — do not block the request.

The helper function `getCsrfToken()` is provided globally by the monitoring page framework (bundled in `/static/js/monitoring.js`) — **you do not need to define it yourself**.

> ⚠️ **Not yet implemented.** The `getCsrfToken()` helper needs to be added to the bundled JS in [`ydb/core/viewer/`](../ydb/core/viewer/).
>
> Target implementation (for reference only):
>
> ```javascript
> // In /static/js/monitoring.js — provided by the framework, not your code:
> function getCsrfToken() {
>   const match = document.cookie.match(/(?:^|;\s*)CSRF-TOKEN=([^;]*)/);
>   return match ? decodeURIComponent(match[1]) : null;
> }
> ```

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
        // fetch() sends cookies by default for same-origin requests (credentials: 'same-origin').
        // Do NOT use an absolute URL or credentials: 'include' unless you have a specific reason.
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

### Server-side validation

CSRF token validation is performed by the **external BFF**, not by YDB's C++ server. The BFF compares the `X-CSRF-Token` header with the `CSRF-TOKEN` cookie before forwarding the request to YDB.

**As a C++ page author, you do not need to add any CSRF validation code to your handler.**

### GET requests must be read-only

**Rule: HTTP GET handlers MUST NOT perform any state-changing operations.**

GET requests are not CSRF-protected. If your page needs to trigger an action (restart, stop, reconfigure), use POST.

This also means: **do not perform side effects inside the page rendering function itself**. A common legacy pattern is to check for a CGI parameter and execute an action during rendering:

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
// ydb/core/blobstorage/pdisk/blobstorage_pdisk_impl_http.cpp (line 188)
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

**✅ CORRECT — use `EscapeC` or `HtmlEscape`:**

```cpp
#include <util/string/escape.h>
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

Always use [`TViewer::GetHTTPOK()`](../ydb/core/viewer/viewer.cpp:917) and related methods to build HTTP responses — **do not build raw HTTP response strings manually**. These methods automatically add `X-Worker-Name` and CORS headers via [`FillCORS()`](../ydb/core/viewer/viewer.cpp:815).

**✅ CORRECT — always include `charset=utf-8` in the content type:**

```cpp
// In your request actor:
ReplyAndPassAway(Viewer->GetHTTPOK(Request, "text/html; charset=utf-8", htmlContent));
```

Note: `GetHTTPOK()` passes the content type as-is — it does **not** add `charset=utf-8` automatically. Always specify it explicitly to prevent the browser from guessing the encoding.

**❌ FORBIDDEN — raw HTTP string or missing charset:**

```cpp
Send(Sender, new NMon::TEvHttpInfoRes("HTTP/1.1 200 Ok\r\n\r\n" + html));  // raw string
ReplyAndPassAway(Viewer->GetHTTPOK(Request, "text/html", htmlContent));     // missing charset
```

---

## 7. Summary Checklist

Before submitting a monitoring page, verify:

- [ ] All `<script>` tags have `nonce='<nonce>'` attribute
- [ ] No scripts or styles loaded from external URLs (CDN, googleapis, etc.)
- [ ] State-changing operations use `fetch` — NOT `<form method="POST">`
- [ ] All POST/PUT/DELETE `fetch` requests include `X-CSRF-Token` header (via `getCsrfToken()`)
- [ ] GET handlers perform no state-changing operations
- [ ] No `onclick=`, `onchange=`, `onXxx=` inline event attributes
- [ ] All user-controlled strings in HTML output are escaped with `HtmlEscape()`
- [ ] HTTP response built via `Viewer->GetHTTPOK()` (not raw string)

---

## 8. Complete Page Template

The following is a canonical example combining all rules: nonce, no inline handlers, CSRF-aware POST, and output escaping. Use this as a starting point for new monitoring pages.

**C++ rendering function:**

```cpp
#include <util/string/html.h>

void TMyComponent::RenderHtml(IOutputStream& str, const THttpInfo& httpInfo) {
    const TString& nonce = httpInfo.Nonce;

    HTML(str) {
        // 1. HTML structure — no inline event handlers
        TAG(TH3) { str << "My Component"; }

        TABLE_CLASS("table") {
            TABLEHEAD() {
                TABLER() {
                    TABLED() { str << "Name"; }
                    TABLED() { str << "Value"; }
                }
            }
            TABLEBODY() {
                for (const auto& item : Items) {
                    TABLER() {
                        // 2. Output escaping — always escape string data
                        TABLED() { str << HtmlEscape(item.Name); }
                        TABLED() { str << item.NumericValue; }  // numeric — safe
                    }
                }
            }
        }

        // 3. Button — no onclick attribute
        str << "<button id='actionBtn'>Perform Action</button>\n";

        // 4. Single <script nonce='...'> block — all JS here
        str << "<script nonce='" << nonce << "'>\n";
        str << R"js(
            // Attach event listeners — never use onclick="..." attributes
            document.getElementById('actionBtn').addEventListener('click', function() {
                // CSRF: read token from cookie, send as header
                // getCsrfToken() is provided globally by /static/js/monitoring.js
                const csrfToken = getCsrfToken();
                const headers = { 'Content-Type': 'application/x-www-form-urlencoded' };
                if (csrfToken) {
                    headers['X-CSRF-Token'] = csrfToken;
                }
                // Use relative URL — always same-origin, cookies sent automatically
                fetch('', {
                    method: 'POST',
                    headers: headers,
                    body: 'action=doSomething'
                }).then(function(response) {
                    if (response.ok) {
                        location.reload();
                    }
                });
            });
        )js";
        str << "</script>\n";
    }
}
```

**Actor handler (sending the response):**

```cpp
void TMyActor::Handle(NMon::TEvHttpInfo::TPtr& ev) {
    TStringStream html;
    RenderHtml(html, ev->Get()->Request);
    // Use GetHTTPOK — never build raw HTTP strings; always include charset
    ReplyAndPassAway(Viewer->GetHTTPOK(Request, "text/html; charset=utf-8", html.Str()));
}
```

---

## References

- [OWASP CSP Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Content_Security_Policy_Cheat_Sheet.html)
- [OWASP CSRF Prevention](https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html)
- [MDN: Content Security Policy](https://developer.mozilla.org/en-US/docs/Web/HTTP/CSP)
- [`library/cpp/monlib/service/pages/templates.h`](../library/cpp/monlib/service/pages/templates.h) — HTML macro definitions
- [`ydb/core/viewer/viewer.cpp`](../ydb/core/viewer/viewer.cpp) — HTTP response helpers (`GetHTTPOK`, `FillCORS`)
