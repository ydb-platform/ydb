#include <library/cpp/resource/resource.h>

#include "ydb_resources.h"

namespace NYdb {

const char* YDB_AUTH_TICKET_HEADER = "x-ydb-auth-ticket";
const char* YDB_DATABASE_HEADER = "x-ydb-database";
const char* YDB_TRACE_ID_HEADER = "x-ydb-trace-id";
const char* OTEL_TRACE_HEADER = "traceparent"; // https://w3c.github.io/trace-context/#header-name
const char* YDB_SDK_BUILD_INFO_HEADER = "x-ydb-sdk-build-info";
const char* YDB_APPLICATION_NAME = "x-ydb-application-name";
const char* YDB_CLIENT_PID = "x-ydb-client-pid";
const char* YDB_REQUEST_TYPE_HEADER = "x-ydb-request-type";
const char* YDB_CONSUMED_UNITS_HEADER = "x-ydb-consumed-units";

// The x-ydb-server-hints header.
// This header can be sent in the trailing metadata with response.
// The only possible value is "session-close". In this case client should gracefully close the session, where the request was executed.
const char* YDB_SERVER_HINTS = "x-ydb-server-hints";

// The message that server can send in trailing metadata to client in order to gracefully shutdown the session.
const char* YDB_SESSION_CLOSE = "session-close";
// Feature compabilities.
// The client should send a feature capability-header in order to enable a feature on the server side.
// Send ("x-ydb-client-capabilities", "session-balancer") pair in metadata with a СreateSession request to enable server side session balancing feature.
const char* YDB_CLIENT_CAPABILITIES = "x-ydb-client-capabilities";
const char* YDB_CLIENT_CAPABILITY_SESSION_BALANCER = "session-balancer";


TString GetSdkSemver() {
    return NResource::Find("ydb_sdk_version.txt");
}

} // namespace NYdb
