#pragma once

#include <yt/yt/core/http/http.h>
#include <yt/yt/core/net/address.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT::NHttp {

////////////////////////////////////////////////////////////////////////////////

class TRequestMock
    : public IRequest
{
public:
    TRequestMock();

    MOCK_METHOD(TFuture<TSharedRef>, Read, (), (override));
    MOCK_METHOD((std::pair<int, int>), GetVersion, (), (override));
    MOCK_METHOD(EMethod, GetMethod, (), (override));
    MOCK_METHOD(const TUrlRef&, GetUrl, (), (override));
    MOCK_METHOD(const THeadersPtr&, GetHeaders, (), (override));
    MOCK_METHOD(const NNet::TNetworkAddress&, GetRemoteAddress, (), (const, override));
    MOCK_METHOD(TGuid, GetConnectionId, (), (const, override));
    MOCK_METHOD(TGuid, GetRequestId, (), (const, override));
    MOCK_METHOD(i64, GetReadByteCount, (), (const, override));
    MOCK_METHOD(TInstant, GetStartTime, (), (const, override));
    MOCK_METHOD(int, GetPort, (), (const, override));

    TUrlRef Url;
    THeadersPtr Headers;
    NNet::TNetworkAddress Address;
};

////////////////////////////////////////////////////////////////////////////////

class TResponseWriterMock
    : public IResponseWriter
{
public:
    TResponseWriterMock();

    MOCK_METHOD(TFuture<void>, Write, (const TSharedRef&), (override));
    MOCK_METHOD(TFuture<void>, Flush, (), (override));
    MOCK_METHOD(TFuture<void>, Close, (), (override));

    MOCK_METHOD(const THeadersPtr&, GetHeaders, (), (override));
    MOCK_METHOD(const THeadersPtr&, GetTrailers, (), (override));
    MOCK_METHOD(bool, AreHeadersFlushed, (), (const, override));
    MOCK_METHOD(std::optional<EStatusCode>, GetStatus, (), (const, override));
    MOCK_METHOD(void, SetStatus, (EStatusCode), (override));
    MOCK_METHOD(void, AddConnectionCloseHeader, (), (override));
    MOCK_METHOD(i64, GetWriteByteCount, (), (const, override));
    MOCK_METHOD(TFuture<void>, WriteBody, (const TSharedRef&), (override));

    THeadersPtr Headers;
    THeadersPtr Trailers;
};

////////////////////////////////////////////////////////////////////////////////

class TResponseMock
    : public IResponse
{
    TResponseMock();

    MOCK_METHOD(EStatusCode, GetStatusCode, (), (override));
    MOCK_METHOD(THeadersPtr&, GetHeaders, (), (override));
    MOCK_METHOD(THeadersPtr&, GetTrailers, (), (override));
    MOCK_METHOD(TFuture<TSharedRef>, Read, (), (override));

    THeadersPtr Headers;
    THeadersPtr Trailers;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttp
