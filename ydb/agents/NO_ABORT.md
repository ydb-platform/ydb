# Do not crash the node on check failures

Do **not** use:

- `Y_ABORT`
- `Y_ABORT_UNLESS`
- `Y_VERIFY`

These terminate the process. Prefer:

- `Y_ENSURE` — throws; caller can catch and recover
- return / reply with an error to the user or peer — keep the node running

When writing code, do not introduce these macros.

**PR review:** always check the diff for new `Y_ABORT` / `Y_ABORT_UNLESS` /
`Y_VERIFY` and request replacing them with `Y_ENSURE` or a non-fatal error reply.
