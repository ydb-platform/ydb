#pragma once
#include <DBPoco/Net/HTTPResponse.h>


namespace DB
{

/// Converts Exception code to HTTP status code.
DBPoco::Net::HTTPResponse::HTTPStatus exceptionCodeToHTTPStatus(int exception_code);

}
