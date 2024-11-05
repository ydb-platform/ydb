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
    void AddMutationId();
    bool HasMutationId() const;

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
    const TString Method;
    const TString Command;
    const bool IsApi;

    TNode::TMapType Parameters;
    TString ImpersonationUser;
    TString Token;
    TString ServiceTicket;
    TNode Attributes;
    TString ProxyAddress;
    TString HostPort;

private:
    TMaybe<TFormat> InputFormat = TFormat::YsonText();
    TMaybe<TFormat> OutputFormat = TFormat::YsonText();

    TString RequestCompression = "identity";
    TString ResponseCompression = "identity";
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
    THolder<TSocket> Socket;
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
        IInputStream* socketStream,
        const TString& requestId,
        const TString& hostName);

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
    THttpInput HttpInput_;
    const TString RequestId_;
    const TString HostName_;
    int HttpCode_ = 0;
    TMaybe<TErrorResponse> ErrorResponse_;
    bool IsExhausted_ = false;
    const bool Unframe_;
    size_t RemainingFrameSize_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class THttpRequest
{
public:
    THttpRequest();
    THttpRequest(const TString& requestId);
    ~THttpRequest();

    TString GetRequestId() const;

    void Connect(TString hostName, TDuration socketTimeout = TDuration::Zero());

    IOutputStream* StartRequest(const THttpHeader& header);
    void FinishRequest();

    void SmallRequest(const THttpHeader& header, TMaybe<TStringBuf> body);

    THttpResponse* GetResponseStream();

    TString GetResponse();

    void InvalidateConnection();

    int GetHttpCode();

private:
    IOutputStream* StartRequestImpl(const THttpHeader& header, bool includeParameters);

private:
    class TRequestStream;

private:
    TString HostName;
    TString RequestId;
    TString Url_;
    TInstant StartTime_;
    TString LoggedAttributes_;

    TConnectionPtr Connection;

    THolder<TRequestStream> RequestStream_;

    THolder<TSocketInput> SocketInput;
    THolder<THttpResponse> Input;

    bool LogResponse = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
