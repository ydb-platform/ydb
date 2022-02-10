#pragma once

#include <util/generic/string.h>

namespace NYdb {

extern const char* YDB_AUTH_TICKET_HEADER; 
extern const char* YDB_DATABASE_HEADER; 
extern const char* YDB_TRACE_ID_HEADER; 
extern const char* YDB_SDK_BUILD_INFO_HEADER; 
extern const char* YDB_REQUEST_TYPE_HEADER;
extern const char* YDB_CONSUMED_UNITS_HEADER;
extern const char* YDB_SERVER_HINTS;
extern const char* YDB_CLIENT_CAPABILITIES;
extern const char* YDB_SESSION_CLOSE;

TString GetSdkSemver();

} // namespace NYdb
