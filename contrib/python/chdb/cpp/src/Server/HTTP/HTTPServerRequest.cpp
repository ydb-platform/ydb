#include <Server/HTTP/HTTPServerRequest.h>

#include <IO/EmptyReadBuffer.h>
#include <IO/HTTPChunkedReadBuffer.h>
#include <IO/LimitReadBuffer.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/ReadHelpers.h>
#include <Server/HTTP/HTTPServerResponse.h>
#include <Server/HTTP/ReadHeaders.h>

#include <CHDBPoco/Net/HTTPHeaderStream.h>
#include <CHDBPoco/Net/HTTPStream.h>
#include <CHDBPoco/Net/NetException.h>

#include <Common/logger_useful.h>

#if USE_SSL
#include <CHDBPoco/Net/SecureStreamSocketImpl.h>
#include <CHDBPoco/Net/SSLException.h>
#include <CHDBPoco/Net/X509Certificate.h>
#endif

static constexpr UInt64 HTTP_MAX_CHUNK_SIZE = 100ULL << 30;

namespace DB_CHDB
{
HTTPServerRequest::HTTPServerRequest(HTTPContextPtr context, HTTPServerResponse & response, CHDBPoco::Net::HTTPServerSession & session, const ProfileEvents::Event & read_event)
    : max_uri_size(context->getMaxUriSize())
    , max_fields_number(context->getMaxFields())
    , max_field_name_size(context->getMaxFieldNameSize())
    , max_field_value_size(context->getMaxFieldValueSize())
{
    response.attachRequest(this);

    /// Now that we know socket is still connected, obtain addresses
    client_address = session.clientAddress();
    server_address = session.serverAddress();
    secure = session.socket().secure();

    auto receive_timeout = context->getReceiveTimeout();
    auto send_timeout = context->getSendTimeout();

    session.socket().setReceiveTimeout(receive_timeout);
    session.socket().setSendTimeout(send_timeout);

    auto in = std::make_unique<ReadBufferFromPocoSocket>(session.socket(), read_event);
    socket = session.socket().impl();

    readRequest(*in);  /// Try parse according to RFC7230

    /// If a client crashes, most systems will gracefully terminate the connection with FIN just like it's done on close().
    /// So we will get 0 from recv(...) and will not be able to understand that something went wrong (well, we probably
    /// will get RST later on attempt to write to the socket that closed on the other side, but it will happen when the query is finished).
    /// If we are extremely unlucky and data format is TSV, for example, then we may stop parsing exactly between rows
    /// and decide that it's EOF (but it is not). It may break deduplication, because clients cannot control it
    /// and retry with exactly the same (incomplete) set of rows.
    /// That's why we have to check body size if it's provided.
    if (getChunkedTransferEncoding())
        stream = std::make_unique<HTTPChunkedReadBuffer>(std::move(in), HTTP_MAX_CHUNK_SIZE);
    else if (hasContentLength())
    {
        size_t content_length = getContentLength();
        stream = std::make_unique<LimitReadBuffer>(std::move(in), content_length,
                                                   /* trow_exception */ true, /* exact_limit */ content_length);
    }
    else if (getMethod() != HTTPRequest::HTTP_GET && getMethod() != HTTPRequest::HTTP_HEAD && getMethod() != HTTPRequest::HTTP_DELETE)
    {
        stream = std::move(in);
        if (!startsWith(getContentType(), "multipart/form-data"))
            LOG_WARNING(LogFrequencyLimiter(getLogger("HTTPServerRequest"), 10), "Got an HTTP request with no content length "
                "and no chunked/multipart encoding, it may be impossible to distinguish graceful EOF from abnormal connection loss");
    }
    else
        /// We have to distinguish empty buffer and nullptr.
        stream = std::make_unique<EmptyReadBuffer>();
}

bool HTTPServerRequest::checkPeerConnected() const
{
    try
    {
        char b;
        if (!socket->receiveBytes(&b, 1, MSG_DONTWAIT | MSG_PEEK))
            return false;
    }
    catch (CHDBPoco::TimeoutException &) // NOLINT(bugprone-empty-catch)
    {
    }
    catch (...)
    {
        return false;
    }

    return true;
}

#if USE_SSL
bool HTTPServerRequest::havePeerCertificate() const
{
    if (!secure)
        return false;

    const CHDBPoco::Net::SecureStreamSocketImpl * secure_socket = dynamic_cast<const CHDBPoco::Net::SecureStreamSocketImpl *>(socket);
    if (!secure_socket)
        return false;

    return secure_socket->havePeerCertificate();
}

CHDBPoco::Net::X509Certificate HTTPServerRequest::peerCertificate() const
{
    if (secure)
    {
        const CHDBPoco::Net::SecureStreamSocketImpl * secure_socket = dynamic_cast<const CHDBPoco::Net::SecureStreamSocketImpl *>(socket);
        if (secure_socket)
            return secure_socket->peerCertificate();
    }
    throw CHDBPoco::Net::SSLException("No certificate available");
}
#endif

void HTTPServerRequest::readRequest(ReadBuffer & in)
{
    char ch;
    std::string method;
    std::string uri;
    std::string version;

    method.reserve(16);
    uri.reserve(64);
    version.reserve(16);

    if (in.eof())
        throw CHDBPoco::Net::NoMessageException();

    skipWhitespaceIfAny(in);

    if (in.eof())
        throw CHDBPoco::Net::MessageException("No HTTP request header");

    while (in.read(ch) && !CHDBPoco::Ascii::isSpace(ch) && method.size() <= MAX_METHOD_LENGTH)
        method += ch;

    if (method.size() > MAX_METHOD_LENGTH)
        throw CHDBPoco::Net::MessageException("HTTP request method invalid or too long");

    skipWhitespaceIfAny(in);

    while (in.read(ch) && !CHDBPoco::Ascii::isSpace(ch) && uri.size() <= max_uri_size)
        uri += ch;

    if (uri.size() > max_uri_size)
        throw CHDBPoco::Net::MessageException("HTTP request URI invalid or too long");

    skipWhitespaceIfAny(in);

    while (in.read(ch) && !CHDBPoco::Ascii::isSpace(ch) && version.size() <= MAX_VERSION_LENGTH)
        version += ch;

    if (version.size() > MAX_VERSION_LENGTH)
        throw CHDBPoco::Net::MessageException("Invalid HTTP version string");

    // since HTTP always use Windows-style EOL '\r\n' we always can safely skip to '\n'

    skipToNextLineOrEOF(in);

    readHeaders(*this, in, max_fields_number, max_field_name_size, max_field_value_size);

    skipToNextLineOrEOF(in);

    setMethod(method);
    setURI(uri);
    setVersion(version);
}

}
