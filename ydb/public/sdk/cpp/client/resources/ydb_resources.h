#pragma once

#include <util/generic/string.h>

namespace NYdb {

extern const char* YDB_AUTH_TICKET_HEADER;
extern const char* YDB_DATABASE_HEADER;
extern const char* YDB_TRACE_ID_HEADER;
extern const char* OTEL_TRACE_HEADER;
extern const char* YDB_SDK_BUILD_INFO_HEADER;
extern const char* YDB_REQUEST_TYPE_HEADER;
extern const char* YDB_CONSUMED_UNITS_HEADER;
extern const char* YDB_SERVER_HINTS;
extern const char* YDB_CLIENT_CAPABILITIES;
extern const char* YDB_SESSION_CLOSE;
extern const char* YDB_CLIENT_CAPABILITY_SESSION_BALANCER;
extern const char* YDB_APPLICATION_NAME;
extern const char* YDB_CLIENT_PID;

TString GetSdkSemver();

} // namespace NYdb
