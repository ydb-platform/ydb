Overview
===
This library is binding for C++ library with Cython.
It provides ability to operate with TVM. Library is fast enough to get or check tickets for every request without burning CPU.
___
[Home page of project](https://wiki.yandex-team.ru/passport/tvm2/)

You can find:
* common part of lib [here](https://a.yandex-team.ru/arc/trunk/arcadia/library/python/tvmauth/tvmauth/__init__.py).
* examples in [examples dir](https://a.yandex-team.ru/arc/trunk/arcadia/library/python/tvmauth/examples).
* mocks for tests [here](https://a.yandex-team.ru/arc/trunk/arcadia/library/python/tvmauth/tvmauth/mock.py) - check docstrings to figure out way to use them.
* some utils [here](https://a.yandex-team.ru/arc/trunk/arcadia/library/python/tvmauth/tvmauth/utils.py).
* some utils for unittests [here](https://a.yandex-team.ru/arc/trunk/arcadia/library/python/tvmauth/tvmauth/unittest.py).

You can ask questions: [PASSPORTDUTY](https://st.yandex-team.ru/createTicket?queue=PASSPORTDUTY&_form=77618)

__
WARNING!
Do not import anything from `tvmauth.tvmauth_pymodule`. This is internal part of library - so it is not the public API of library.
It could be changed without changing of major version.


TvmClient
===
Don't forget to collect logs from client.

If you don't need an instance of client anymore but your process would keep running, please `stop()` this instance.
___
`TvmClient` allowes:
1. `get_service_ticket_for()` - to fetch ServiceTicket for outgoing request
2. `check_service_ticket()` - to check ServiceTicket from incoming request
3. `check_user_ticket()` - to check UserTicket from incoming request
4. `get_roles()` - to get roles from IDM

All methods are thread-safe.

Status of `CheckedServiceTicket` or `CheckedUserTicket` can be only 'Ok': otherwise exception will be thrown.
___
You should check status of client with property `status`:
* `OK` - nothing to do here
* `Warn` - **you should trigger your monitoring alert**

      Normal operation of TvmClient is still possible but there are problems with refreshing cache, so it is expiring.
      Is tvm-api.yandex.net accessible?
      Have you changed your TVM-secret or your backend (dst) deleted its TVM-client?

* `Error` - **you should trigger your monitoring alert and close this instance for user-traffic**

      TvmClient's cache is already invalid (expired) or soon will be: you can't check valid ServiceTicket or be authenticated by your backends (dsts)

___
Constructor creates system thread for refreshing cache - so do not fork your proccess after creating `TvmClient` instance. Constructor leads to network I/O. Other methods always use memory.

Exceptions maybe thrown from constructor:
* `RetriableException` - maybe some network trouble: you can try to create client one more time.
* `NonRetriableException` - settings are bad: fix them.

Other methods can throw exception only if you try to use unconfigured abilities (for example, you try to get fetched ServiceTicket for some dst but you didn't configured it in settings).
___
You can choose way for fetching data for your service operation:
* http://localhost:{port}/tvm - recomended way
* https://tvm-api.yandex.net

TvmTool
------------
`TvmClient` uses local http-interface (tvmtool) to get state. This interface can be provided with tvmtool (local daemon) or Qloud/YP (local http api in container).
See more: https://wiki.yandex-team.ru/passport/tvm2/tvm-daemon/.

`TvmClient` fetches configuration from tvmtool, so you need only to tell client how to connect to it and tell which alias of tvm id should be used for this `TvmClient` instance.

TvmApi
------------
`TvmClient` uses https://tvm-api.yandex.net to get state.
First of all: please use `disk_cache_dir` - it provides reliability for your service and for tvm-api.
Please check restrictions of this method.

Roles
===
[Example](https://a.yandex-team.ru/arc/trunk/arcadia/library/python/tvmauth/examples/create_with_tvmapi/__init__.py?rev=r9238823#L108)

You need to configure roles fetching
------------
1. Enable disk cache: [disk_cache_dir](https://a.yandex-team.ru/arc/trunk/arcadia/library/python/tvmauth/tvmauth/__init__.py?rev=r9238823#L316)

2. Enable ServiceTicket fetching:
    [self_tvm_id](https://a.yandex-team.ru/arc/trunk/arcadia/library/python/tvmauth/tvmauth/__init__.py?rev=r9238823#L311) + [self_secret](https://a.yandex-team.ru/arc/trunk/arcadia/library/python/tvmauth/tvmauth/__init__.py?rev=r9238823#L314)
3. Enable roles fetching from tirole:
    [fetch_roles_for_idm_system_slug](https://a.yandex-team.ru/arc/trunk/arcadia/library/python/tvmauth/tvmauth/__init__.py?rev=r9238823#L321)

You need to use roles for request check
------------
1. Check ServiceTicket and/or UserTicket - as usual:
     [check_service_ticket()](https://a.yandex-team.ru/arc/trunk/arcadia/library/cpp/tvmauth/client/facade.h?rev=r7890770#L91)/[check_user_ticket()](https://a.yandex-team.ru/arc/trunk/arcadia/library/cpp/tvmauth/client/facade.h?rev=r7890770#L99)

2. Get actual roles from `TvmClient`: [get_roles()](https://a.yandex-team.ru/arc/trunk/arcadia/library/python/tvmauth/tvmauth/__init__.py?rev=r9238823#L178)

3. Use roles:
     [check_service_role()](https://a.yandex-team.ru/arc/trunk/arcadia/library/python/tvmauth/tvmauth/__init__.py?rev=r9238823#L561)/[check_user_role()](https://a.yandex-team.ru/arc/trunk/arcadia/library/python/tvmauth/tvmauth/__init__.py?rev=r9238823#L572)

4. If consumer (service or user) has required role, you can perform request.
     If consumer doesn't have required role, you should show error message with useful message.
