Overview
===
This library provides ability to operate with TVM. Library is fast enough to get or check tickets for every request without burning CPU.

[Home page of project](https://wiki.yandex-team.ru/passport/tvm2/)

You can ask questions: [PASSPORTDUTY](https://st.yandex-team.ru/createTicket?queue=PASSPORTDUTY&_form=77618)

TvmClient
===
Don't forget to collect logs from client.
___
`TvmClient` allowes:
1. `GetServiceTicketFor()` - to fetch ServiceTicket for outgoing request
2. `CheckServiceTicket()` - to check ServiceTicket from incoming request
3. `CheckUserTicket()` - to check UserTicket from incoming request

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

Other methods can throw exception only if you try to use unconfigured abilities (for example, you try to get fetched ServiceTicket for some dst but you didn't configured it in settings).
___
You can choose way for fetching data for your service operation:
* http://localhost:{port}/tvm
* https://tvm-api.yandex.net

TvmTool
------------
`TTvmClient` uses local http-interface to get state. This interface can be provided with tvmtool (local daemon) or Qloud/YD (local http api in container).
See more: https://wiki.yandex-team.ru/passport/tvm2/tvm-daemon/.

`TTvmClient` fetches configuration from tvmtool, so you need only to tell client how to connect to it and tell which alias of tvm id should be used for this `TvmClient` instance.

TvmApi
------------
`TvmClient` uses https://tvm-api.yandex.net to get state.
First of all: please use `SetDiskCacheDir()` - it provides reliability for your service and for tvm-api.
Please check restrictions of this method.
