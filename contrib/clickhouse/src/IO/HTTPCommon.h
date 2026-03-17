#pragma once

#include <memory>
#include <mutex>
#include <string_view>

#include <DBPoco/Net/HTTPClientSession.h>
#include <DBPoco/Net/HTTPRequest.h>
#include <DBPoco/Net/HTTPResponse.h>
#include <DBPoco/URI.h>
#include <DBPoco/URIStreamFactory.h>
#include <Common/HTTPConnectionPool.h>
#include <Common/ProxyConfiguration.h>

#include <IO/ConnectionTimeouts.h>


namespace DB
{

class HTTPServerResponse;

class HTTPException : public Exception
{
public:
    HTTPException(
        int code,
        const std::string & uri,
        DBPoco::Net::HTTPResponse::HTTPStatus http_status_,
        const std::string & reason,
        const std::string & body
    )
        : Exception(makeExceptionMessage(code, uri, http_status_, reason, body))
        , http_status(http_status_)
    {}

    HTTPException * clone() const override { return new HTTPException(*this); }
    void rethrow() const override { throw *this; } /// NOLINT(cert-err60-cpp)

    DBPoco::Net::HTTPResponse::HTTPStatus getHTTPStatus() const { return http_status; }

private:
    DBPoco::Net::HTTPResponse::HTTPStatus http_status{};

    static Exception makeExceptionMessage(
        int code,
        const std::string & uri,
        DBPoco::Net::HTTPResponse::HTTPStatus http_status,
        const std::string & reason,
        const std::string & body);

    const char * name() const noexcept override { return "DB::HTTPException"; }
    const char * className() const noexcept override { return "DB::HTTPException"; }
};

using HTTPSessionPtr = std::shared_ptr<DBPoco::Net::HTTPClientSession>;

void setResponseDefaultHeaders(HTTPServerResponse & response);

/// Create session object to perform requests and set required parameters.
HTTPSessionPtr makeHTTPSession(
    HTTPConnectionGroupType group,
    const DBPoco::URI & uri,
    const ConnectionTimeouts & timeouts,
    const ProxyConfiguration & proxy_config = {},
    UInt64 * connect_time = nullptr
);

bool isRedirect(DBPoco::Net::HTTPResponse::HTTPStatus status);

/** Used to receive response (response headers and possibly body)
  *  after sending data (request headers and possibly body).
  * Throws exception in case of non HTTP_OK (200) response code.
  * Returned istream lives in 'session' object.
  */
std::istream * receiveResponse(
    DBPoco::Net::HTTPClientSession & session, const DBPoco::Net::HTTPRequest & request, DBPoco::Net::HTTPResponse & response, bool allow_redirects);

void assertResponseIsOk(
    const String & uri, DBPoco::Net::HTTPResponse & response, std::istream & istr, bool allow_redirects = false);

}
