/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/core/http/curl/CurlHttpClient.h>
#include <aws/core/http/HttpRequest.h>
#include <aws/core/http/standard/StandardHttpResponse.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/core/utils/logging/LogMacros.h>
#include <aws/core/utils/ratelimiter/RateLimiterInterface.h>
#include <aws/core/utils/DateTime.h>
#include <aws/core/utils/crypto/Hash.h>
#include <aws/core/utils/Outcome.h>
#include <aws/core/monitoring/HttpClientMetrics.h>
#include <cassert>
#include <algorithm>


using namespace Aws::Client;
using namespace Aws::Http;
using namespace Aws::Http::Standard;
using namespace Aws::Utils;
using namespace Aws::Utils::Logging;
using namespace Aws::Monitoring;

#ifdef AWS_CUSTOM_MEMORY_MANAGEMENT

static const char* MemTag = "libcurl";
static size_t offset = sizeof(size_t);

static void* malloc_callback(size_t size)
{
    char* newMem = reinterpret_cast<char*>(Aws::Malloc(MemTag, size + offset));
    std::size_t* pointerToSize = reinterpret_cast<std::size_t*>(newMem);
    *pointerToSize = size;
    return reinterpret_cast<void*>(newMem + offset);
}

static void free_callback(void* ptr)
{
    if(ptr)
    {
        char* shiftedMemory = reinterpret_cast<char*>(ptr);
        Aws::Free(shiftedMemory - offset);
    }
}

static void* realloc_callback(void* ptr, size_t size)
{
    if(!ptr)
    {
        return malloc_callback(size);
    }


    if(!size && ptr)
    {
        free_callback(ptr);
        return nullptr;
    }

    char* originalLenCharPtr = reinterpret_cast<char*>(ptr) - offset;
    size_t originalLen = *reinterpret_cast<size_t*>(originalLenCharPtr);

    char* rawMemory = reinterpret_cast<char*>(Aws::Malloc(MemTag, size + offset));
    if(rawMemory)
    {
        std::size_t* pointerToSize = reinterpret_cast<std::size_t*>(rawMemory);
        *pointerToSize = size;

        size_t copyLength = (std::min)(originalLen, size);
#ifdef _MSC_VER
        memcpy_s(rawMemory + offset, size, ptr, copyLength);
#else
        memcpy(rawMemory + offset, ptr, copyLength);
#endif
        free_callback(ptr);
        return reinterpret_cast<void*>(rawMemory + offset);
    }
    else
    {
        return ptr;
    }

}

static void* calloc_callback(size_t nmemb, size_t size)
{
    size_t dataSize = nmemb * size;
    char* newMem = reinterpret_cast<char*>(Aws::Malloc(MemTag, dataSize + offset));
    std::size_t* pointerToSize = reinterpret_cast<std::size_t*>(newMem);
    *pointerToSize = dataSize;
#ifdef _MSC_VER
    memset_s(newMem + offset, dataSize, 0, dataSize);
#else
    memset(newMem + offset, 0, dataSize);
#endif

    return reinterpret_cast<void*>(newMem + offset);
}

static char* strdup_callback(const char* str)
{
    size_t len = strlen(str) + 1;
    size_t newLen = len + offset;
    char* newMem = reinterpret_cast<char*>(Aws::Malloc(MemTag, newLen));

    if(newMem)
    {
        std::size_t* pointerToSize = reinterpret_cast<std::size_t*>(newMem);
        *pointerToSize = len;
#ifdef _MSC_VER
        memcpy_s(newMem + offset, len, str, len);
#else
        memcpy(newMem + offset, str, len);
#endif
        return newMem + offset;
    }
    return nullptr;
}

#endif

struct CurlWriteCallbackContext
{
    CurlWriteCallbackContext(const CurlHttpClient* client,
                             HttpRequest* request,
                             HttpResponse* response,
                             Aws::Utils::RateLimits::RateLimiterInterface* rateLimiter) :
        m_client(client),
        m_request(request),
        m_response(response),
        m_rateLimiter(rateLimiter),
        m_numBytesResponseReceived(0)
    {}

    const CurlHttpClient* m_client;
    HttpRequest* m_request;
    HttpResponse* m_response;
    Aws::Utils::RateLimits::RateLimiterInterface* m_rateLimiter;
    int64_t m_numBytesResponseReceived;
};

struct CurlReadCallbackContext
{
    CurlReadCallbackContext(const CurlHttpClient* client, CURL* curlHandle, HttpRequest* request, Aws::Utils::RateLimits::RateLimiterInterface* limiter) :
        m_client(client),
        m_curlHandle(curlHandle),
        m_rateLimiter(limiter),
        m_request(request),
        m_chunkEnd(false)
    {}

    const CurlHttpClient* m_client;
    CURL* m_curlHandle;
    Aws::Utils::RateLimits::RateLimiterInterface* m_rateLimiter;
    HttpRequest* m_request;
    bool m_chunkEnd;
};

static const char* CURL_HTTP_CLIENT_TAG = "CurlHttpClient";

static int64_t GetContentLengthFromHeader(CURL* connectionHandle,
                                          bool& hasContentLength) {
#if LIBCURL_VERSION_NUM >= 0x073700  // 7.55.0
  curl_off_t contentLength = {};
  CURLcode res = curl_easy_getinfo(
      connectionHandle, CURLINFO_CONTENT_LENGTH_DOWNLOAD_T, &contentLength);
#else
  double contentLength = {};
  CURLcode res = curl_easy_getinfo(
      connectionHandle, CURLINFO_CONTENT_LENGTH_DOWNLOAD, &contentLength);
#endif
  hasContentLength = (res == CURLE_OK) && (contentLength != -1);
  return hasContentLength ? static_cast<int64_t>(contentLength) : -1;
}

static size_t WriteData(char* ptr, size_t size, size_t nmemb, void* userdata)
{
    if (ptr)
    {
        CurlWriteCallbackContext* context = reinterpret_cast<CurlWriteCallbackContext*>(userdata);

        const CurlHttpClient* client = context->m_client;
        if(!client->ContinueRequest(*context->m_request) || !client->IsRequestProcessingEnabled())
        {
            return 0;
        }

        HttpResponse* response = context->m_response;
        size_t sizeToWrite = size * nmemb;
        if (context->m_rateLimiter)
        {
            context->m_rateLimiter->ApplyAndPayForCost(static_cast<int64_t>(sizeToWrite));
        }

        for (const auto& hashIterator : context->m_request->GetResponseValidationHashes())
        {
            hashIterator.second->Update(reinterpret_cast<unsigned char*>(ptr), sizeToWrite);
        }

        response->GetResponseBody().write(ptr, static_cast<std::streamsize>(sizeToWrite));
        if (context->m_request->IsEventStreamRequest() && !response->HasHeader(Aws::Http::X_AMZN_ERROR_TYPE))
        {
            response->GetResponseBody().flush();
        }
        auto& receivedHandler = context->m_request->GetDataReceivedEventHandler();
        if (receivedHandler)
        {
            receivedHandler(context->m_request, context->m_response, static_cast<long long>(sizeToWrite));
        }

        AWS_LOGSTREAM_TRACE(CURL_HTTP_CLIENT_TAG, sizeToWrite << " bytes written to response.");
        context->m_numBytesResponseReceived += sizeToWrite;
        return sizeToWrite;
    }
    return 0;
}

static size_t WriteHeader(char* ptr, size_t size, size_t nmemb, void* userdata)
{
    if (ptr)
    {
        CurlWriteCallbackContext* context = reinterpret_cast<CurlWriteCallbackContext*>(userdata);
        AWS_LOGSTREAM_TRACE(CURL_HTTP_CLIENT_TAG, ptr);
        HttpResponse* response = context->m_response;
        Aws::String headerLine(ptr);
        Aws::Vector<Aws::String> keyValuePair = StringUtils::Split(headerLine, ':', 2);

        if (keyValuePair.size() == 2)
        {
            response->AddHeader(StringUtils::Trim(keyValuePair[0].c_str()), StringUtils::Trim(keyValuePair[1].c_str()));
        }

        return size * nmemb;
    }
    return 0;
}

static size_t ReadBody(char* ptr, size_t size, size_t nmemb, void* userdata, bool isStreaming)
{
    CurlReadCallbackContext* context = reinterpret_cast<CurlReadCallbackContext*>(userdata);
    if(context == nullptr)
    {
        return 0;
    }

    const CurlHttpClient* client = context->m_client;
    if(!client->ContinueRequest(*context->m_request) || !client->IsRequestProcessingEnabled())
    {
        return CURL_READFUNC_ABORT;
    }

    HttpRequest* request = context->m_request;
    const std::shared_ptr<Aws::IOStream>& ioStream = request->GetContentBody();

    size_t amountToRead = size * nmemb;
    bool isAwsChunked = request->HasHeader(Aws::Http::CONTENT_ENCODING_HEADER) &&
        request->GetHeaderValue(Aws::Http::CONTENT_ENCODING_HEADER) == Aws::Http::AWS_CHUNKED_VALUE;
    // aws-chunk = hex(chunk-size) + CRLF + chunk-data + CRLF
    // Needs to reserve bytes of sizeof(hex(chunk-size)) + sizeof(CRLF) + sizeof(CRLF)
    if (isAwsChunked)
    {
        Aws::String amountToReadHexString = Aws::Utils::StringUtils::ToHexString(amountToRead);
        amountToRead -= (amountToReadHexString.size() + 4);
    }

    if (ioStream != nullptr && amountToRead > 0)
    {
        if (isStreaming)
        {
            if (ioStream->readsome(ptr, amountToRead) == 0 && !ioStream->eof())
            {
                return CURL_READFUNC_PAUSE;
            }
        }
        else
        {
            ioStream->read(ptr, amountToRead);
        }
        size_t amountRead = static_cast<size_t>(ioStream->gcount());

        if (isAwsChunked)
        {
            if (amountRead > 0)
            {
                if (request->GetRequestHash().second != nullptr)
                {
                    request->GetRequestHash().second->Update(reinterpret_cast<unsigned char*>(ptr), amountRead);
                }

                Aws::String hex = Aws::Utils::StringUtils::ToHexString(amountRead);
                memmove(ptr + hex.size() + 2, ptr, amountRead);
                memmove(ptr + hex.size() + 2 + amountRead, "\r\n", 2);
                memmove(ptr, hex.c_str(), hex.size());
                memmove(ptr + hex.size(), "\r\n", 2);
                amountRead += hex.size() + 4;
            }
            else if (!context->m_chunkEnd)
            {
                Aws::StringStream chunkedTrailer;
                chunkedTrailer << "0\r\n";
                if (request->GetRequestHash().second != nullptr)
                {
                    chunkedTrailer << "x-amz-checksum-" << request->GetRequestHash().first << ":"
                        << HashingUtils::Base64Encode(request->GetRequestHash().second->GetHash().GetResult()) << "\r\n";
                }
                chunkedTrailer << "\r\n";
                amountRead = chunkedTrailer.str().size();
                memcpy(ptr, chunkedTrailer.str().c_str(), amountRead);
                context->m_chunkEnd = true;
            }
        }

        auto& sentHandler = request->GetDataSentEventHandler();
        if (sentHandler)
        {
            sentHandler(request, static_cast<long long>(amountRead));
        }

        if (context->m_rateLimiter)
        {
            context->m_rateLimiter->ApplyAndPayForCost(static_cast<int64_t>(amountRead));
        }

        return amountRead;
    }

    return 0;
}

static size_t ReadBodyStreaming(char* ptr, size_t size, size_t nmemb, void* userdata) {
    return ReadBody(ptr, size, nmemb, userdata, true);
}

static size_t ReadBodyFunc(char* ptr, size_t size, size_t nmemb, void* userdata) {
    return ReadBody(ptr, size, nmemb, userdata, false);
}

static size_t SeekBody(void* userdata, curl_off_t offset, int origin)
{
    CurlReadCallbackContext* context = reinterpret_cast<CurlReadCallbackContext*>(userdata);
    if(context == nullptr)
    {
        return CURL_SEEKFUNC_FAIL;
    }

    const CurlHttpClient* client = context->m_client;
    if(!client->ContinueRequest(*context->m_request) || !client->IsRequestProcessingEnabled())
    {
        return CURL_SEEKFUNC_FAIL;
    }

    HttpRequest* request = context->m_request;
    const std::shared_ptr<Aws::IOStream>& ioStream = request->GetContentBody();

    std::ios_base::seekdir dir;
    switch(origin)
    {
        case SEEK_SET:
            dir = std::ios_base::beg;
            break;
        case SEEK_CUR:
            dir = std::ios_base::cur;
            break;
        case SEEK_END:
            dir = std::ios_base::end;
            break;
        default:
            return CURL_SEEKFUNC_FAIL;
    }

    ioStream->clear();
    ioStream->seekg(offset, dir);
    if (ioStream->fail()) {
        return CURL_SEEKFUNC_CANTSEEK;
    }

    return CURL_SEEKFUNC_OK;
}
#if LIBCURL_VERSION_NUM >= 0x072000 // 7.32.0
static int CurlProgressCallback(void *userdata, curl_off_t, curl_off_t, curl_off_t, curl_off_t)
#else
static int CurlProgressCallback(void *userdata, double, double, double, double)
#endif
{
    CurlReadCallbackContext* context = reinterpret_cast<CurlReadCallbackContext*>(userdata);

    const std::shared_ptr<Aws::IOStream>& ioStream = context->m_request->GetContentBody();
    if (ioStream->eof())
    {
        curl_easy_pause(context->m_curlHandle, CURLPAUSE_CONT);
        return 0;
    }
    char output[1];
    if (ioStream->readsome(output, 1) > 0)
    {
        ioStream->unget();
        if (!ioStream->good())
        {
            AWS_LOGSTREAM_WARN(CURL_HTTP_CLIENT_TAG, "Input stream failed to perform unget().");
        }
        curl_easy_pause(context->m_curlHandle, CURLPAUSE_CONT);
    }

    return 0;
}

void SetOptCodeForHttpMethod(CURL* requestHandle, const std::shared_ptr<HttpRequest>& request)
{
    switch (request->GetMethod())
    {
        case HttpMethod::HTTP_GET:
            curl_easy_setopt(requestHandle, CURLOPT_HTTPGET, 1L);
            break;
        case HttpMethod::HTTP_POST:
            if (request->HasHeader(Aws::Http::CONTENT_LENGTH_HEADER) && request->GetHeaderValue(Aws::Http::CONTENT_LENGTH_HEADER) == "0")
            {
                curl_easy_setopt(requestHandle, CURLOPT_CUSTOMREQUEST, "POST");
            }
            else
            {
                curl_easy_setopt(requestHandle, CURLOPT_POST, 1L);
            }
            break;
        case HttpMethod::HTTP_PUT:
            if ((!request->HasHeader(Aws::Http::CONTENT_LENGTH_HEADER) || request->GetHeaderValue(Aws::Http::CONTENT_LENGTH_HEADER) == "0") &&
                 !request->HasHeader(Aws::Http::TRANSFER_ENCODING_HEADER))
            {
                curl_easy_setopt(requestHandle, CURLOPT_CUSTOMREQUEST, "PUT");
            }
            else
            {
#if LIBCURL_VERSION_NUM >= 0x070c01 // 7.12.1
                curl_easy_setopt(requestHandle, CURLOPT_UPLOAD, 1L);
#else
                curl_easy_setopt(requestHandle, CURLOPT_PUT, 1L);
#endif
            }
            break;
        case HttpMethod::HTTP_HEAD:
            curl_easy_setopt(requestHandle, CURLOPT_HTTPGET, 1L);
            curl_easy_setopt(requestHandle, CURLOPT_NOBODY, 1L);
            break;
        case HttpMethod::HTTP_PATCH:
            if ((!request->HasHeader(Aws::Http::CONTENT_LENGTH_HEADER)|| request->GetHeaderValue(Aws::Http::CONTENT_LENGTH_HEADER) == "0") &&
                 !request->HasHeader(Aws::Http::TRANSFER_ENCODING_HEADER))
            {
                curl_easy_setopt(requestHandle, CURLOPT_CUSTOMREQUEST, "PATCH");
            }
            else
            {
                curl_easy_setopt(requestHandle, CURLOPT_POST, 1L);
                curl_easy_setopt(requestHandle, CURLOPT_CUSTOMREQUEST, "PATCH");
            }

            break;
        case HttpMethod::HTTP_DELETE:
            curl_easy_setopt(requestHandle, CURLOPT_CUSTOMREQUEST, "DELETE");
            break;
        default:
            assert(0);
            curl_easy_setopt(requestHandle, CURLOPT_CUSTOMREQUEST, "GET");
            break;
    }
}


std::atomic<bool> CurlHttpClient::isInit(false);

void CurlHttpClient::InitGlobalState()
{
    if (!isInit)
    {
        auto curlVersionData = curl_version_info(CURLVERSION_NOW);
        AWS_LOGSTREAM_INFO(CURL_HTTP_CLIENT_TAG, "Initializing Curl library with version: " << curlVersionData->version
            << ", ssl version: " << curlVersionData->ssl_version);
        isInit = true;
#ifdef AWS_CUSTOM_MEMORY_MANAGEMENT
        curl_global_init_mem(CURL_GLOBAL_ALL, &malloc_callback, &free_callback, &realloc_callback, &strdup_callback, &calloc_callback);
#else
        curl_global_init(CURL_GLOBAL_ALL);
#endif
    }
}


void CurlHttpClient::CleanupGlobalState()
{
    curl_global_cleanup();
}

Aws::String CurlInfoTypeToString(curl_infotype type)
{
    switch(type)
    {
        case CURLINFO_TEXT:
            return "Text";

        case CURLINFO_HEADER_IN:
            return "HeaderIn";

        case CURLINFO_HEADER_OUT:
            return "HeaderOut";

        case CURLINFO_DATA_IN:
            return "DataIn";

        case CURLINFO_DATA_OUT:
            return "DataOut";

        case CURLINFO_SSL_DATA_IN:
            return "SSLDataIn";

        case CURLINFO_SSL_DATA_OUT:
            return "SSLDataOut";

        default:
            return "Unknown";
    }
}

int CurlDebugCallback(CURL *handle, curl_infotype type, char *data, size_t size, void *userptr)
{
    AWS_UNREFERENCED_PARAM(handle);
    AWS_UNREFERENCED_PARAM(userptr);

    if(type == CURLINFO_SSL_DATA_IN || type == CURLINFO_SSL_DATA_OUT)
    {
        AWS_LOGSTREAM_DEBUG("CURL", "(" << CurlInfoTypeToString(type) << ") " << size << "bytes");
    }
    else
    {
        Aws::String debugString(data, size);
        AWS_LOGSTREAM_DEBUG("CURL", "(" << CurlInfoTypeToString(type) << ") " << debugString);
    }

    return 0;
}


CurlHttpClient::CurlHttpClient(const ClientConfiguration& clientConfig) :
    Base(),
    m_curlHandleContainer(clientConfig.maxConnections, clientConfig.httpRequestTimeoutMs, clientConfig.connectTimeoutMs, clientConfig.enableTcpKeepAlive,
                          clientConfig.tcpKeepAliveIntervalMs, clientConfig.requestTimeoutMs, clientConfig.lowSpeedLimit),
    m_isUsingProxy(!clientConfig.proxyHost.empty()), m_proxyUserName(clientConfig.proxyUserName),
    m_proxyPassword(clientConfig.proxyPassword), m_proxyScheme(SchemeMapper::ToString(clientConfig.proxyScheme)), m_proxyHost(clientConfig.proxyHost),
    m_proxySSLCertPath(clientConfig.proxySSLCertPath), m_proxySSLCertType(clientConfig.proxySSLCertType),
    m_proxySSLKeyPath(clientConfig.proxySSLKeyPath), m_proxySSLKeyType(clientConfig.proxySSLKeyType),
    m_proxyKeyPasswd(clientConfig.proxySSLKeyPassword),
    m_proxyPort(clientConfig.proxyPort), m_verifySSL(clientConfig.verifySSL), m_caPath(clientConfig.caPath),
    m_caFile(clientConfig.caFile), m_proxyCaPath(clientConfig.proxyCaPath), m_proxyCaFile(clientConfig.proxyCaFile),
    m_disableExpectHeader(clientConfig.disableExpectHeader)
{
    if (clientConfig.followRedirects == FollowRedirectsPolicy::NEVER ||
       (clientConfig.followRedirects == FollowRedirectsPolicy::DEFAULT && clientConfig.region == Aws::Region::AWS_GLOBAL))
    {
        m_allowRedirects = false;
    }
    else
    {
        m_allowRedirects = true;
    }
    if(clientConfig.nonProxyHosts.GetLength() > 0)
    {
        Aws::StringStream ss;
        ss << clientConfig.nonProxyHosts.GetItem(0);
        for (auto i=1u; i < clientConfig.nonProxyHosts.GetLength(); i++)
        {
            ss << "," << clientConfig.nonProxyHosts.GetItem(i);
        }
        m_nonProxyHosts = ss.str();
    }
}


std::shared_ptr<HttpResponse> CurlHttpClient::MakeRequest(const std::shared_ptr<HttpRequest>& request,
    Aws::Utils::RateLimits::RateLimiterInterface* readLimiter,
    Aws::Utils::RateLimits::RateLimiterInterface* writeLimiter) const
{
    URI uri = request->GetUri();
    Aws::String url = uri.GetURIString();
    std::shared_ptr<HttpResponse> response = Aws::MakeShared<StandardHttpResponse>(CURL_HTTP_CLIENT_TAG, request);

    AWS_LOGSTREAM_TRACE(CURL_HTTP_CLIENT_TAG, "Making request to " << url);
    struct curl_slist* headers = NULL;

    if (writeLimiter != nullptr)
    {
        writeLimiter->ApplyAndPayForCost(request->GetSize());
    }

    Aws::StringStream headerStream;
    HeaderValueCollection requestHeaders = request->GetHeaders();

    AWS_LOGSTREAM_TRACE(CURL_HTTP_CLIENT_TAG, "Including headers:");
    for (auto& requestHeader : requestHeaders)
    {
        headerStream.str("");
        headerStream << requestHeader.first << ": " << requestHeader.second;
        Aws::String headerString = headerStream.str();
        AWS_LOGSTREAM_TRACE(CURL_HTTP_CLIENT_TAG, headerString);
        headers = curl_slist_append(headers, headerString.c_str());
    }

    if (!request->HasHeader(Http::TRANSFER_ENCODING_HEADER))
    {
        headers = curl_slist_append(headers, "transfer-encoding:");
    }

    if (!request->HasHeader(Http::CONTENT_LENGTH_HEADER))
    {
        headers = curl_slist_append(headers, "content-length:");
    }

    if (!request->HasHeader(Http::CONTENT_TYPE_HEADER))
    {
        headers = curl_slist_append(headers, "content-type:");
    }

    // Discard Expect header so as to avoid using multiple payloads to send a http request (header + body)
    if (m_disableExpectHeader)
    {
        headers = curl_slist_append(headers, "Expect:");
    }

    CURL* connectionHandle = m_curlHandleContainer.AcquireCurlHandle();

    if (connectionHandle)
    {
        AWS_LOGSTREAM_DEBUG(CURL_HTTP_CLIENT_TAG, "Obtained connection handle " << connectionHandle);

        if (headers)
        {
            curl_easy_setopt(connectionHandle, CURLOPT_HTTPHEADER, headers);
        }

        CurlWriteCallbackContext writeContext(this, request.get(), response.get(), readLimiter);
        CurlReadCallbackContext readContext(this, connectionHandle, request.get(), writeLimiter);

        SetOptCodeForHttpMethod(connectionHandle, request);

        curl_easy_setopt(connectionHandle, CURLOPT_URL, url.c_str());
        curl_easy_setopt(connectionHandle, CURLOPT_WRITEFUNCTION, WriteData);
        curl_easy_setopt(connectionHandle, CURLOPT_WRITEDATA, &writeContext);
        curl_easy_setopt(connectionHandle, CURLOPT_HEADERFUNCTION, WriteHeader);
        curl_easy_setopt(connectionHandle, CURLOPT_HEADERDATA, &writeContext);

        //we only want to override the default path if someone has explicitly told us to.
        if(!m_caPath.empty())
        {
            curl_easy_setopt(connectionHandle, CURLOPT_CAPATH, m_caPath.c_str());
        }
        if(!m_caFile.empty())
        {
            curl_easy_setopt(connectionHandle, CURLOPT_CAINFO, m_caFile.c_str());
        }

        // enable the cookie engine without reading any initial cookies.
        curl_easy_setopt(connectionHandle, CURLOPT_COOKIEFILE, "");

	// only set by android test builds because the emulator is missing a cert needed for aws services
#ifdef TEST_CERT_PATH
	curl_easy_setopt(connectionHandle, CURLOPT_CAPATH, TEST_CERT_PATH);
#endif // TEST_CERT_PATH

        if (m_verifySSL)
        {
            curl_easy_setopt(connectionHandle, CURLOPT_SSL_VERIFYPEER, 1L);
            curl_easy_setopt(connectionHandle, CURLOPT_SSL_VERIFYHOST, 2L);

#if LIBCURL_VERSION_MAJOR >= 7
#if LIBCURL_VERSION_MINOR >= 34
            curl_easy_setopt(connectionHandle, CURLOPT_SSLVERSION, CURL_SSLVERSION_TLSv1);
#endif //LIBCURL_VERSION_MINOR
#endif //LIBCURL_VERSION_MAJOR
        }
        else
        {
            curl_easy_setopt(connectionHandle, CURLOPT_SSL_VERIFYPEER, 0L);
            curl_easy_setopt(connectionHandle, CURLOPT_SSL_VERIFYHOST, 0L);
        }

        if (m_allowRedirects)
        {
            curl_easy_setopt(connectionHandle, CURLOPT_FOLLOWLOCATION, 1L);
        }
        else
        {
            curl_easy_setopt(connectionHandle, CURLOPT_FOLLOWLOCATION, 0L);
        }

#ifdef ENABLE_CURL_LOGGING
        curl_easy_setopt(connectionHandle, CURLOPT_VERBOSE, 1);
        curl_easy_setopt(connectionHandle, CURLOPT_DEBUGFUNCTION, CurlDebugCallback);
#endif
        if (m_isUsingProxy)
        {
            Aws::StringStream ss;
            ss << m_proxyScheme << "://" << m_proxyHost;
            curl_easy_setopt(connectionHandle, CURLOPT_PROXY, ss.str().c_str());
            curl_easy_setopt(connectionHandle, CURLOPT_PROXYPORT, (long) m_proxyPort);
            if(!m_proxyCaPath.empty())
            {
                curl_easy_setopt(connectionHandle, CURLOPT_PROXY_CAPATH, m_proxyCaPath.c_str());
            }
            if(!m_proxyCaFile.empty())
            {
                curl_easy_setopt(connectionHandle, CURLOPT_PROXY_CAINFO, m_proxyCaFile.c_str());
            }
            if (!m_proxyUserName.empty() || !m_proxyPassword.empty())
            {
                curl_easy_setopt(connectionHandle, CURLOPT_PROXYUSERNAME, m_proxyUserName.c_str());
                curl_easy_setopt(connectionHandle, CURLOPT_PROXYPASSWORD, m_proxyPassword.c_str());
            }
            curl_easy_setopt(connectionHandle, CURLOPT_NOPROXY, m_nonProxyHosts.c_str());
#ifdef CURL_HAS_TLS_PROXY
            if (!m_proxySSLCertPath.empty())
            {
                curl_easy_setopt(connectionHandle, CURLOPT_PROXY_SSLCERT, m_proxySSLCertPath.c_str());
                if (!m_proxySSLCertType.empty())
                {
                    curl_easy_setopt(connectionHandle, CURLOPT_PROXY_SSLCERTTYPE, m_proxySSLCertType.c_str());
                }
            }
            if (!m_proxySSLKeyPath.empty())
            {
                curl_easy_setopt(connectionHandle, CURLOPT_PROXY_SSLKEY, m_proxySSLKeyPath.c_str());
                if (!m_proxySSLKeyType.empty())
                {
                    curl_easy_setopt(connectionHandle, CURLOPT_PROXY_SSLKEYTYPE, m_proxySSLKeyType.c_str());
                }
                if (!m_proxyKeyPasswd.empty())
                {
                    curl_easy_setopt(connectionHandle, CURLOPT_PROXY_KEYPASSWD, m_proxyKeyPasswd.c_str());
                }
            }
#endif //CURL_HAS_TLS_PROXY
        }
        else
        {
            curl_easy_setopt(connectionHandle, CURLOPT_PROXY, "");
        }

        if (request->GetContentBody())
        {
            curl_easy_setopt(connectionHandle, CURLOPT_READFUNCTION, ReadBodyFunc);
            curl_easy_setopt(connectionHandle, CURLOPT_READDATA, &readContext);
            curl_easy_setopt(connectionHandle, CURLOPT_SEEKFUNCTION, SeekBody);
            curl_easy_setopt(connectionHandle, CURLOPT_SEEKDATA, &readContext);
            if (request->IsEventStreamRequest() && !response->HasHeader(Aws::Http::X_AMZN_ERROR_TYPE))
            {
                curl_easy_setopt(connectionHandle, CURLOPT_READFUNCTION, ReadBodyStreaming);
                curl_easy_setopt(connectionHandle, CURLOPT_NOPROGRESS, 0L);
#if LIBCURL_VERSION_NUM >= 0x072000 // 7.32.0
                curl_easy_setopt(connectionHandle, CURLOPT_XFERINFOFUNCTION, CurlProgressCallback);
                curl_easy_setopt(connectionHandle, CURLOPT_XFERINFODATA, &readContext);
#else
                curl_easy_setopt(connectionHandle, CURLOPT_PROGRESSFUNCTION, CurlProgressCallback);
                curl_easy_setopt(connectionHandle, CURLOPT_PROGRESSDATA, &readContext);
#endif
            }
        }

        OverrideOptionsOnConnectionHandle(connectionHandle);
        Aws::Utils::DateTime startTransmissionTime = Aws::Utils::DateTime::Now();
        CURLcode curlResponseCode = curl_easy_perform(connectionHandle);
        bool shouldContinueRequest = ContinueRequest(*request);
        if (curlResponseCode != CURLE_OK && shouldContinueRequest)
        {
            response->SetClientErrorType(CoreErrors::NETWORK_CONNECTION);
            Aws::StringStream ss;
            ss << "curlCode: " << curlResponseCode << ", " << curl_easy_strerror(curlResponseCode);
            response->SetClientErrorMessage(ss.str());
            AWS_LOGSTREAM_ERROR(CURL_HTTP_CLIENT_TAG, "Curl returned error code " << curlResponseCode
                    << " - " << curl_easy_strerror(curlResponseCode));
        }
        else if(!shouldContinueRequest)
        {
            response->SetClientErrorType(CoreErrors::USER_CANCELLED);
            response->SetClientErrorMessage("Request cancelled by user's continuation handler");
        }
        else
        {
            long responseCode;
            curl_easy_getinfo(connectionHandle, CURLINFO_RESPONSE_CODE, &responseCode);
            response->SetResponseCode(static_cast<HttpResponseCode>(responseCode));
            AWS_LOGSTREAM_DEBUG(CURL_HTTP_CLIENT_TAG, "Returned http response code " << responseCode);

            char* contentType = nullptr;
            curl_easy_getinfo(connectionHandle, CURLINFO_CONTENT_TYPE, &contentType);
            if (contentType)
            {
                response->SetContentType(contentType);
                AWS_LOGSTREAM_DEBUG(CURL_HTTP_CLIENT_TAG, "Returned content type " << contentType);
            }

            bool hasContentLength = false;
            int64_t contentLength =
                GetContentLengthFromHeader(connectionHandle, hasContentLength);

            if (request->GetMethod() != HttpMethod::HTTP_HEAD &&
                writeContext.m_client->IsRequestProcessingEnabled() &&
                hasContentLength)
            {
                int64_t numBytesResponseReceived = writeContext.m_numBytesResponseReceived;
                AWS_LOGSTREAM_TRACE(CURL_HTTP_CLIENT_TAG, "Response content-length header: " << contentLength);
                AWS_LOGSTREAM_TRACE(CURL_HTTP_CLIENT_TAG, "Response body length: " << numBytesResponseReceived);
                if (contentLength != numBytesResponseReceived)
                {
                    response->SetClientErrorType(CoreErrors::NETWORK_CONNECTION);
                    response->SetClientErrorMessage("Response body length doesn't match the content-length header.");
                    AWS_LOGSTREAM_ERROR(CURL_HTTP_CLIENT_TAG, "Response body length doesn't match the content-length header.");
                }
            }

            AWS_LOGSTREAM_DEBUG(CURL_HTTP_CLIENT_TAG, "Releasing curl handle " << connectionHandle);
        }

        double timep;
        CURLcode ret = curl_easy_getinfo(connectionHandle, CURLINFO_NAMELOOKUP_TIME, &timep); // DNS Resolve Latency, seconds.
        if (ret == CURLE_OK)
        {
            request->AddRequestMetric(GetHttpClientMetricNameByType(HttpClientMetricsType::DnsLatency), static_cast<int64_t>(timep * 1000));// to milliseconds
        }

        ret = curl_easy_getinfo(connectionHandle, CURLINFO_STARTTRANSFER_TIME, &timep); // Connect Latency
        if (ret == CURLE_OK)
        {
            request->AddRequestMetric(GetHttpClientMetricNameByType(HttpClientMetricsType::ConnectLatency), static_cast<int64_t>(timep * 1000));
        }

        ret = curl_easy_getinfo(connectionHandle, CURLINFO_APPCONNECT_TIME, &timep); // Ssl Latency
        if (ret == CURLE_OK)
        {
            request->AddRequestMetric(GetHttpClientMetricNameByType(HttpClientMetricsType::SslLatency), static_cast<int64_t>(timep * 1000));
        }

        const char* ip = nullptr;
        auto curlGetInfoResult = curl_easy_getinfo(connectionHandle, CURLINFO_PRIMARY_IP, &ip); // Get the IP address of the remote endpoint
        if (curlGetInfoResult == CURLE_OK && ip)
        {
            request->SetResolvedRemoteHost(ip);
        }
        if (curlResponseCode != CURLE_OK)
        {
            m_curlHandleContainer.DestroyCurlHandle(connectionHandle);
        }
        else
        {
            m_curlHandleContainer.ReleaseCurlHandle(connectionHandle);
        }
        //go ahead and flush the response body stream
        response->GetResponseBody().flush();
        request->AddRequestMetric(GetHttpClientMetricNameByType(HttpClientMetricsType::RequestLatency), (DateTime::Now() - startTransmissionTime).count());
    }

    if (headers)
    {
        curl_slist_free_all(headers);
    }

    return response;
}
