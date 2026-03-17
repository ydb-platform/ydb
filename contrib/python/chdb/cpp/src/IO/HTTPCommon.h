#pragma once

#include <memory>
#include <mutex>

#include <CHDBPoco/Net/HTTPClientSession.h>
#include <CHDBPoco/Net/HTTPRequest.h>
#include <CHDBPoco/Net/HTTPResponse.h>
#include <CHDBPoco/URI.h>
#include <CHDBPoco/URIStreamFactory.h>
#include <Common/HTTPConnectionPool.h>
#include <Common/ProxyConfiguration.h>

#include <IO/ConnectionTimeouts.h>


namespace DB_CHDB
{

class HTTPServerResponse;

class HTTPException : public Exception
{
public:
    HTTPException(
        int code,
        const std::string & uri,
        CHDBPoco::Net::HTTPResponse::HTTPStatus http_status_,
        const std::string & reason,
        const std::string & body
    )
        : Exception(makeExceptionMessage(code, uri, http_status_, reason, body))
        , http_status(http_status_)
    {}

    HTTPException * clone() const override { return new HTTPException(*this); }
    void rethrow() const override { throw *this; } /// NOLINT(cert-err60-cpp)

    CHDBPoco::Net::HTTPResponse::HTTPStatus getHTTPStatus() const { return http_status; }

private:
    CHDBPoco::Net::HTTPResponse::HTTPStatus http_status{};

    static Exception makeExceptionMessage(
        int code,
        const std::string & uri,
        CHDBPoco::Net::HTTPResponse::HTTPStatus http_status,
        const std::string & reason,
        const std::string & body);

    const char * name() const noexcept override { return "DB_CHDB::HTTPException"; }
    const char * className() const noexcept override { return "DB_CHDB::HTTPException"; }
};

using HTTPSessionPtr = std::shared_ptr<CHDBPoco::Net::HTTPClientSession>;

void setResponseDefaultHeaders(HTTPServerResponse & response);

/// Create session object to perform requests and set required parameters.
HTTPSessionPtr makeHTTPSession(
    HTTPConnectionGroupType group,
    const CHDBPoco::URI & uri,
    const ConnectionTimeouts & timeouts,
    const ProxyConfiguration & proxy_config = {}
);

bool isRedirect(CHDBPoco::Net::HTTPResponse::HTTPStatus status);

/** Used to receive response (response headers and possibly body)
  *  after sending data (request headers and possibly body).
  * Throws exception in case of non HTTP_OK (200) response code.
  * Returned istream lives in 'session' object.
  */
std::istream * receiveResponse(
    CHDBPoco::Net::HTTPClientSession & session, const CHDBPoco::Net::HTTPRequest & request, CHDBPoco::Net::HTTPResponse & response, bool allow_redirects);

void assertResponseIsOk(
    const String & uri, CHDBPoco::Net::HTTPResponse & response, std::istream & istr, bool allow_redirects = false);

}
