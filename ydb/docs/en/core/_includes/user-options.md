* `user_name` — the name of the user. It may contain lowercase Latin letters and digits.
* `option` — command option:
  * `PASSWORD 'password'` — sets a password to `password`; you can't use it together with `HASH`.
  * `PASSWORD NULL` — sets an empty password; you can't use it together with `HASH`; default value.
  * `HASH 'hash'` — sets the user's password, the hash of which is contained in the `'hash'` JSON structure (see below); you can't use it together with `PASSWORD`.
  * `NOLOGIN` — disallows user to [log in](../security/authentication.md); you can't use it together with `LOGIN`.
  * `LOGIN` — allows user to [log in](../security/authentication.md); you can't use it together with `NOLOGIN`.