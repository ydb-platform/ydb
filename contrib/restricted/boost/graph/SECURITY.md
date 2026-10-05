# Security Policy

Boost.Graph is a header-only C++ template library. Most issues that look
like security problems are caused by misuse of the API, undefined behavior
in caller code, or violating a documented precondition. Those should be
filed as regular GitHub issues.

If you believe you have found a genuine vulnerability in the library
itself, please report it privately through GitHub's
[private vulnerability reporting](https://github.com/boostorg/graph/security/advisories/new)
rather than opening a public issue.

Fixes land on the `develop` branch and ship with the next Boost release.
No back-porting to older releases is guaranteed.
