#pragma once

#include "fwd.h"

#include <yt/cpp/mapreduce/interface/common.h>
#include <yt/cpp/mapreduce/interface/errors.h>
#include <yt/cpp/mapreduce/interface/format.h>
#include <yt/cpp/mapreduce/interface/io.h>
#include <yt/cpp/mapreduce/interface/node.h>

#include <library/cpp/deprecated/atomic/atomic.h>
#include <library/cpp/http/io/stream.h>

#include <util/generic/hash.h>
#include <util/generic/hash_multi_map.h>
#include <util/generic/strbuf.h>
#include <util/generic/guid.h>
#include <util/network/socket.h>
#include <util/stream/input.h>
#include <util/system/mutex.h>
#include <util/system/rwlock.h>
#include <util/generic/ptr.h>

namespace NYT {

class TNode;

namespace NHttp {

struct THeadersPtrWrapper;

} // NHttp

////////////////////////////////////////////////////////////////////////////////

enum class EFrameType
{
    Data = 0x01,
    KeepAlive = 0x02,
};

struct TRequestContext
{
    TString RequestId;
    TString HostName;
    TString Method;
};

class THttpHeader
{
public:
    THttpHeader(const TString& method, const TString& command, bool isApi = true);

    void AddParameter(const TString& key, TNode value, bool overwrite = false);
    void RemoveParameter(const TString& key);
    void MergeParameters(const TNode& parameters, bool overwrite = false);
    TNode GetParameters() const;

    void AddTransactionId(const TTransactionId& transactionId, bool overwrite = false);
    void AddPath(const TString& path, bool overwrite = false);
    void AddOperationId(const TOperationId& operationId, bool overwrite = false);
    TMutationId AddMutationId();
    bool HasMutationId() const;
    void SetMutationId(TMutationId mutationId);

    void SetToken(const TString& token);
    void SetProxyAddress(const TString& proxyAddress);
    void SetHostPort(const TString& hostPort);
    void SetImpersonationUser(const TString& impersonationUser);

    void SetServiceTicket(const TString& ticket);

    void SetInputFormat(const TMaybe<TFormat>& format);

    void SetOutputFormat(const TMaybe<TFormat>& format);
    TMaybe<TFormat> GetOutputFormat() const;

    void SetRequestCompression(const TString& compression);
    void SetResponseCompression(const TString& compression);

    TString GetCommand() const;
    TString GetUrl(bool needProxy = false) const;
    TString GetHeaderAsString(const TString& hostName, const TString& requestId, bool includeParameters = true) const;
    NHttp::THeadersPtrWrapper GetHeader(const TString& hostName, const TString& requestId, bool includeParameters) const;

    const TString& GetMethod() const;

private:
    bool ShouldAcceptFraming() const;

private:
    const TString Method_;
    const TString Command_;
    const bool IsApi_;

    TNode::TMapType Parameters_;
    TString ImpersonationUser_;
    TString Token_;
    TString ServiceTicket_;
    TNode Attributes_;
    TString ProxyAddress_;
    TString HostPort_;

    TMaybe<TFormat> InputFormat_ = TFormat::YsonText();
    TMaybe<TFormat> OutputFormat_ = TFormat::YsonText();

    TString RequestCompression_ = "identity";
    TString ResponseCompression_ = "identity";
};

////////////////////////////////////////////////////////////////////////////////

class TAddressCache
{
public:
    using TAddressPtr = TAtomicSharedPtr<TNetworkAddress>;

    static TAddressCache* Get();

    TAddressPtr Resolve(const TString& hostName);

private:
    struct TCacheEntry {
        TAddressPtr Address;
        TInstant ExpirationTime;
    };

private:
    TAddressPtr FindAddress(const TString& hostName) const;
    void AddAddress(TString hostName, TAddressPtr address);

private:
    TRWMutex Lock_;
    THashMap<TString, TCacheEntry> Cache_;
};

////////////////////////////////////////////////////////////////////////////////

struct TConnection
{
    std::unique_ptr<TSocket> Socket;
    TAtomic Busy = 1;
    TInstant DeadLine;
    ui32 Id;
};

using TConnectionPtr = TAtomicSharedPtr<TConnection>;

class TConnectionPool
{
public:
    using TConnectionMap = THashMultiMap<TString, TConnectionPtr>;

    static TConnectionPool* Get();

    TConnectionPtr Connect(const TString& hostName, TDuration socketTimeout);
    void Release(TConnectionPtr connection);
    void Invalidate(const TString& hostName, TConnectionPtr connection);

private:
    void Refresh();
    static SOCKET DoConnect(TAddressCache::TAddressPtr address);

private:
    TMutex Lock_;
    TConnectionMap Connections_;
};

////////////////////////////////////////////////////////////////////////////////

//
// Input stream that handles YT-specific header/trailer errors
// and throws TErrorResponse if it finds any.
class THttpResponse
    : public IInputStream
{
public:
    // 'requestId' and 'hostName' are provided for debug reasons
    // (they will appear in some error messages).
    THttpResponse(
        TRequestContext context,
        IInputStream* socketStream);

    ~THttpResponse();

    const THttpHeaders& Headers() const;

    void CheckErrorResponse() const;
    bool IsExhausted() const;
    int GetHttpCode() const;
    const TString& GetHostName() const;
    bool IsKeepAlive() const;

protected:
    size_t DoRead(void* buf, size_t len) override;
    size_t DoSkip(size_t len) override;

private:
    void CheckTrailers(const THttpHeaders& trailers);
    TMaybe<TErrorResponse> ParseError(const THttpHeaders& headers);
    size_t UnframeRead(void* buf, size_t len);
    size_t UnframeSkip(size_t len);
    bool RefreshFrameIfNecessary();

private:
    class THttpInputWrapped;

private:
    std::unique_ptr<THttpInputWrapped> HttpInput_;

    const bool Unframe_;

    TRequestContext Context_;
    int HttpCode_ = 0;
    TMaybe<TErrorResponse> ErrorResponse_;
    bool IsExhausted_ = false;
    size_t RemainingFrameSize_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class THttpRequest
{
public:
    THttpRequest(TString requestId, TString hostName, THttpHeader header, TDuration socketTimeout);
    ~THttpRequest();

    TString GetRequestId() const;

    IOutputStream* StartRequest();
    void FinishRequest();

    void SmallRequest(TMaybe<TStringBuf> body);

    THttpResponse* GetResponseStream();

    TString GetResponse();

    void InvalidateConnection();

    int GetHttpCode();

private:
    IOutputStream* StartRequestImpl(bool includeParameters);

private:
    class TRequestStream;

private:
    const TRequestContext Context_;
    const THttpHeader Header_;
    const TString Url_;
    const TDuration SocketTimeout_;

    TInstant StartTime_;
    TString LoggedAttributes_;

    TConnectionPtr Connection_;

    std::unique_ptr<TRequestStream> RequestStream_;

    std::unique_ptr<TSocketInput> SocketInput_;
    std::unique_ptr<THttpResponse> Input_;

    bool LogResponse_ = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
