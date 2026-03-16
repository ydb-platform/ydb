#include <string_view>
#include <IO/HTTPCommon.h>

#include <Server/HTTP/HTTPServerResponse.h>
#include <DBPoco/Any.h>
#include <DBPoco/StreamCopier.h>
#include <Common/Exception.h>

#include "clickhouse_config.h"

#if USE_SSL
#    include <DBPoco/Net/AcceptCertificateHandler.h>
#    include <DBPoco/Net/Context.h>
#    include <DBPoco/Net/HTTPSClientSession.h>
#    include <DBPoco/Net/InvalidCertificateHandler.h>
#    include <DBPoco/Net/PrivateKeyPassphraseHandler.h>
#    include <DBPoco/Net/RejectCertificateHandler.h>
#    include <DBPoco/Net/SSLManager.h>
#    include <DBPoco/Net/SecureStreamSocket.h>
#endif

#include <DBPoco/Util/Application.h>

#include <istream>
#include <sstream>
#include <unordered_map>
#include <Common/ProxyConfiguration.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int RECEIVED_ERROR_FROM_REMOTE_IO_SERVER;
    extern const int RECEIVED_ERROR_TOO_MANY_REQUESTS;
}

void setResponseDefaultHeaders(HTTPServerResponse & response)
{
    if (!response.getKeepAlive())
        return;

    const size_t keep_alive_timeout = response.getSession().getKeepAliveTimeout();
    const size_t keep_alive_max_requests = response.getSession().getMaxKeepAliveRequests();
    if (keep_alive_timeout)
    {
        if (keep_alive_max_requests)
            response.set("Keep-Alive", fmt::format("timeout={}, max={}", keep_alive_timeout, keep_alive_max_requests));
        else
            response.set("Keep-Alive", fmt::format("timeout={}", keep_alive_timeout));
    }
}

HTTPSessionPtr makeHTTPSession(
    HTTPConnectionGroupType group,
    const DBPoco::URI & uri,
    const ConnectionTimeouts & timeouts,
    const ProxyConfiguration & proxy_configuration,
    UInt64 * connect_time)
{
    auto connection_pool = HTTPConnectionPools::instance().getPool(group, uri, proxy_configuration);
    return connection_pool->getConnection(timeouts, connect_time);
}

bool isRedirect(const DBPoco::Net::HTTPResponse::HTTPStatus status) { return status == DBPoco::Net::HTTPResponse::HTTP_MOVED_PERMANENTLY  || status == DBPoco::Net::HTTPResponse::HTTP_FOUND || status == DBPoco::Net::HTTPResponse::HTTP_SEE_OTHER  || status == DBPoco::Net::HTTPResponse::HTTP_TEMPORARY_REDIRECT; }

std::istream * receiveResponse(
    DBPoco::Net::HTTPClientSession & session, const DBPoco::Net::HTTPRequest & request, DBPoco::Net::HTTPResponse & response, const bool allow_redirects)
{
    auto & istr = session.receiveResponse(response);
    assertResponseIsOk(request.getURI(), response, istr, allow_redirects);
    return &istr;
}

void assertResponseIsOk(const String & uri, DBPoco::Net::HTTPResponse & response, std::istream & istr, const bool allow_redirects)
{
    auto status = response.getStatus();

    if (!(status == DBPoco::Net::HTTPResponse::HTTP_OK
        || status == DBPoco::Net::HTTPResponse::HTTP_CREATED
        || status == DBPoco::Net::HTTPResponse::HTTP_ACCEPTED
        || status == DBPoco::Net::HTTPResponse::HTTP_PARTIAL_CONTENT /// Reading with Range header was successful.
        || (isRedirect(status) && allow_redirects)))
    {
        int code = status == DBPoco::Net::HTTPResponse::HTTP_TOO_MANY_REQUESTS
            ? ErrorCodes::RECEIVED_ERROR_TOO_MANY_REQUESTS
            : ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER;

        std::string body;
        DBPoco::StreamCopier::copyToString(istr, body);

        throw HTTPException(code, uri, status, response.getReason(), body);
    }
}

Exception HTTPException::makeExceptionMessage(
    int code,
    const std::string & uri,
    DBPoco::Net::HTTPResponse::HTTPStatus http_status,
    const std::string & reason,
    const std::string & body)
{
    return Exception(code,
        "Received error from remote server {}. "
        "HTTP status code: {} '{}', "
        "body length: {} bytes, body: '{}'",
        uri, static_cast<int>(http_status), reason, body.length(), body);
}

}
