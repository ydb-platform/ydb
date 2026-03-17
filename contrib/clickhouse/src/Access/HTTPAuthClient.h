#pragma once
#include <IO/ConnectionTimeouts.h>
#include <IO/HTTPCommon.h>

#include <base/sleep.h>
#include <DBPoco/Net/HTTPBasicCredentials.h>


namespace DB
{

struct HTTPAuthClientParams
{
    DBPoco::URI uri;
    ConnectionTimeouts timeouts;
    size_t max_tries;
    size_t retry_initial_backoff_ms;
    size_t retry_max_backoff_ms;
    std::vector<String> forward_headers;
};

template <typename TResponseParser>
class HTTPAuthClient
{
public:
    using Result = TResponseParser::Result;

    explicit HTTPAuthClient(const HTTPAuthClientParams & params, const TResponseParser & parser_ = TResponseParser{})
        : timeouts{params.timeouts}
        , max_tries{params.max_tries}
        , retry_initial_backoff_ms{params.retry_initial_backoff_ms}
        , retry_max_backoff_ms{params.retry_max_backoff_ms}
        , forward_headers{params.forward_headers}
        , uri{params.uri}
        , parser{parser_}
    {
    }

    Result authenticateRequest(DBPoco::Net::HTTPRequest & request) const
    {
        auto session = makeHTTPSession(HTTPConnectionGroupType::HTTP, uri, timeouts);
        DBPoco::Net::HTTPResponse response;

        auto milliseconds_to_wait = retry_initial_backoff_ms;
        for (size_t attempt = 0; attempt < max_tries; ++attempt)
        {
            bool last_attempt = attempt + 1 >= max_tries;
            try
            {
                session->sendRequest(request);
                auto & body_stream = session->receiveResponse(response);
                return parser.parse(response, &body_stream);
            }
            catch (const DBPoco::Exception &) // TODO: make retries smarter
            {
                if (last_attempt)
                    throw;

                sleepForMilliseconds(milliseconds_to_wait);
                milliseconds_to_wait = std::min(milliseconds_to_wait * 2, retry_max_backoff_ms);
            }
        }
        UNREACHABLE();
    }

    const DBPoco::URI & getURI() const { return uri; }
    const std::vector<String> & getForwardHeaders() const { return forward_headers; }

private:
    const ConnectionTimeouts timeouts;
    const size_t max_tries;
    const size_t retry_initial_backoff_ms;
    const size_t retry_max_backoff_ms;
    const std::vector<String> forward_headers;
    const DBPoco::URI uri;
    TResponseParser parser;
};


template <typename TResponseParser>
class HTTPBasicAuthClient : private HTTPAuthClient<TResponseParser>
{
public:
    using HTTPAuthClient<TResponseParser>::HTTPAuthClient;
    using Result = HTTPAuthClient<TResponseParser>::Result;

    Result authenticate(const String & user_name, const String & password, const std::unordered_map<String, String> & headers) const
    {
        DBPoco::Net::HTTPRequest request{
            DBPoco::Net::HTTPRequest::HTTP_GET, this->getURI().getPathAndQuery(), DBPoco::Net::HTTPRequest::HTTP_1_1};

        for (const auto & k : this->getForwardHeaders())
        {
            auto it = headers.find(k);
            if (it != headers.end())
                request.add(k, it->second);
        }

        DBPoco::Net::HTTPBasicCredentials basic_credentials{user_name, password};
        basic_credentials.authenticate(request);

        return this->authenticateRequest(request);
    }
};

}
