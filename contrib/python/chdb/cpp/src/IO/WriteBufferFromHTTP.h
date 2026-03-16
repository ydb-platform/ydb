#pragma once

#include <IO/ConnectionTimeouts.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromOStream.h>
#include <IO/HTTPCommon.h>
#include <IO/HTTPHeaderEntries.h>
#include <CHDBPoco/Net/HTTPClientSession.h>
#include <CHDBPoco/Net/HTTPRequest.h>
#include <CHDBPoco/Net/HTTPResponse.h>
#include <CHDBPoco/URI.h>


namespace DB_CHDB
{

/* Perform HTTP POST/PUT request.
 */
class WriteBufferFromHTTP : public WriteBufferFromOStream
{
public:
    explicit WriteBufferFromHTTP(const HTTPConnectionGroupType & connection_group,
                                 const CHDBPoco::URI & uri,
                                 const std::string & method = CHDBPoco::Net::HTTPRequest::HTTP_POST, // POST or PUT only
                                 const std::string & content_type = "",
                                 const std::string & content_encoding = "",
                                 const HTTPHeaderEntries & additional_headers = {},
                                 const ConnectionTimeouts & timeouts = {},
                                 size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE,
                                 ProxyConfiguration proxy_configuration = {});

private:
    /// Receives response from the server after sending all data.
    void finalizeImpl() override;

    HTTPSessionPtr session;
    CHDBPoco::Net::HTTPRequest request;
    CHDBPoco::Net::HTTPResponse response;
};

}
