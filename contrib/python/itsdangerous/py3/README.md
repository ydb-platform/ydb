# ItsDangerous

... so better sign this

Various helpers to pass data to untrusted environments and to get it
back safe and sound. Data is cryptographically signed to ensure that a
token has not been tampered with.

It's possible to customize how data is serialized. Data is compressed as
needed. A timestamp can be added and verified automatically while
loading a token.


## A Simple Example

Here's how you could generate a token for transmitting a user's id and
name between web requests.

```python
from itsdangerous import URLSafeSerializer
auth_s = URLSafeSerializer("secret key", "auth")
token = auth_s.dumps({"id": 5, "name": "itsdangerous"})

print(token)
# eyJpZCI6NSwibmFtZSI6Iml0c2Rhbmdlcm91cyJ9.6YP6T0BaO67XP--9UzTrmurXSmg

data = auth_s.loads(token)
print(data["name"])
# itsdangerous
```


## Donate

The Pallets organization develops and supports ItsDangerous and other
popular packages. In order to grow the community of contributors and
users, and allow the maintainers to devote more time to the projects,
[please donate today][].

[please donate today]: https://palletsprojects.com/donate
