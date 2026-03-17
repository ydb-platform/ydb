#pragma once
#include <CHDBPoco/Net/HTTPResponse.h>


namespace DB_CHDB
{

/// Converts Exception code to HTTP status code.
CHDBPoco::Net::HTTPResponse::HTTPStatus exceptionCodeToHTTPStatus(int exception_code);

}
