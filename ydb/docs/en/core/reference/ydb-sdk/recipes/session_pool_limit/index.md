# Setting the session pool size

{% include [work in progress message](../_includes/addition.md) %}

The client's session pool size affects resource consumption (RAM, CPU) on the server side of {{ ydb-short-name }}.
Simple math: if `1000` clients of the same DB have `1000` sessions each, `1000000` actors (workers, session performers) are created on the server side. If you don't limit the number of sessions on the client, this may result in a slow cluster that is close to a failure.
By default, the {{ ydb-short-name }} SDK limits the number of sessions to `50`.
A good recommendation is to set the limit on the number of client sessions to the minimum required for the normal operation of the client app. Keep in mind that sessions are single-threaded both on the server and client side. So if the application needs to make `1000` simultaneous (`inflight`) requests to {{ ydb-short-name }} for its estimated load, the limit should be set to `1000` sessions.
Here it's necessary to distinguish between the estimated `RPS` (requests per second) and `inflight`. In the first case, this is the total number of requests to {{ ydb-short-name }} completed within `1` second. For example, if `RPS`=`10000` and the average `latency` is `100`ms, it's sufficient to set the session limit to `1000`. This means that each session will perform an average of `10` consecutive requests for the estimated second.

Below are examples of the code for setting the session pool limit in different {{ ydb-short-name }} SDKs.

{% list tabs %}

- Go

  {% include [go.md](_includes/go.md) %}

- Java

  {% include [java.md](_includes/java.md) %}

{% endlist %}

