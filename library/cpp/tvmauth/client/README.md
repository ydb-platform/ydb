Overview
===
This library provides ability to operate with TVM. Library is fast enough to get or check tickets for every request without burning CPU.

[Home page of project](https://wiki.yandex-team.ru/passport/tvm2/)
You can find some examples in [here](https://a.yandex-team.ru/arc/trunk/arcadia/library/cpp/tvmauth/client/examples).

You can ask questions: [PASSPORTDUTY](https://st.yandex-team.ru/createTicket?queue=PASSPORTDUTY&_form=77618)

TvmClient
===
Don't forget to collect logs from client.
___
`TvmClient` allowes:
1. `GetServiceTicketFor()` - to fetch ServiceTicket for outgoing request
2. `CheckServiceTicket()` - to check ServiceTicket from incoming request
3. `CheckUserTicket()` - to check UserTicket from incoming request
4. `GetRoles()` - to get roles from IDM

All methods are thread-safe.

You should check status of `CheckedServiceTicket` or `CheckedUserTicket` for equality 'Ok'. You can get ticket fields (src/uids/scopes) only for correct ticket. Otherwise exception will be thrown.
___
You should check status of client with `GetStatus()`:
* `OK` - nothing to do here
* `Warning` - **you should trigger your monitoring alert**

      Normal operation of TvmClient is still possible but there are problems with refreshing cache, so it is expiring.
      Is tvm-api.yandex.net accessible?
      Have you changed your TVM-secret or your backend (dst) deleted its TVM-client?

* `Error` - **you should trigger your monitoring alert and close this instance for user-traffic**

      TvmClient's cache is already invalid (expired) or soon will be: you can't check valid ServiceTicket or be authenticated by your backends (dsts)

___
Constructor creates system thread for refreshing cache - so do not fork your proccess after creating `TTvmClient` instance. Constructor leads to network I/O. Other methods always use memory.

Exceptions maybe thrown from constructor:
* `TRetriableException` - maybe some network trouble: you can try to create client one more time.
* `TNonRetriableException` - settings are bad: fix them.
___
You can choose way for fetching data for your service operation:
* http://localhost:{port}/tvm - recomended way
* https://tvm-api.yandex.net

TvmTool
------------
`TTvmClient` uses local http-interface to get state. This interface can be provided with tvmtool (local daemon) or Qloud/YP (local http api in container).
See more: https://wiki.yandex-team.ru/passport/tvm2/tvm-daemon/.

`TTvmClient` fetches configuration from tvmtool, so you need only to tell client how to connect to it and tell which alias of tvm id should be used for this `TvmClient` instance.

TvmApi
------------
First of all: please use `DiskCacheDir` - it provides reliability for your service and for tvm-api.
Please check restrictions of this field.

Roles
===
[Example](https://a.yandex-team.ru/arc/trunk/arcadia/library/cpp/tvmauth/client/examples/create_with_tvmapi/create.cpp?rev=r8888584#L84)

You need to configure roles fetching
------------
1. Enable disk cache: [DiskCacheDir](https://a.yandex-team.ru/arc/trunk/arcadia/library/cpp/tvmauth/client/misc/api/settings.h?rev=r9001419#L54)

2. Enable ServiceTicket fetching:
     [SelfTvmId](https://a.yandex-team.ru/arc/trunk/arcadia/library/cpp/tvmauth/client/misc/api/settings.h?rev=r9001419#L57) + [Secret](https://a.yandex-team.ru/arc/trunk/arcadia/library/cpp/tvmauth/client/misc/api/settings.h?rev=r9001419#L60)
3. Enable roles fetching from tirole:
    [FetchRolesForIdmSystemSlug](https://a.yandex-team.ru/arc/trunk/arcadia/library/cpp/tvmauth/client/misc/api/settings.h?rev=r9001419#L78)

You need to use roles for request check
------------
1. Check ServiceTicket and/or UserTicket - as usual:
     [CheckServiceTicket()](https://a.yandex-team.ru/arc/trunk/arcadia/library/cpp/tvmauth/client/facade.h?rev=r7890770#L91)/[CheckUserTicket()](https://a.yandex-team.ru/arc/trunk/arcadia/library/cpp/tvmauth/client/facade.h?rev=r7890770#L99)

2. Get actual roles from `TvmClient`: [GetRoles()](https://a.yandex-team.ru/arc/trunk/arcadia/library/cpp/tvmauth/client/facade.h?rev=r7890770#L105)

3. Use roles
     - case#1: [get](https://a.yandex-team.ru/arc/trunk/arcadia/library/cpp/tvmauth/client/misc/roles/roles.h?rev=r7890770#L37-46) role list for service or user and check for the exact role you need.
     - case#2: use [shortcuts](https://a.yandex-team.ru/arc/trunk/arcadia/library/cpp/tvmauth/client/misc/roles/roles.h?rev=r7890770#L50) - they are wrappers for case#1

4. If consumer (service or user) has required role, you can perform request.
     If consumer doesn't have required role, you should show error message with useful message.
