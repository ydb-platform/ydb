#pragma once

#include <yt/yt/core/http/client.h>
#include <yt/yt/core/http/http.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT::NHttp {

////////////////////////////////////////////////////////////////////////////////

using TMockHeaders = std::vector<std::pair<TString, TString>>;

struct TMockResponse
{
    EStatusCode StatusCode;
    TString Body;
    TMockHeaders Headers;
};

class TMockClient
    : public IClient
{
public:
    TFuture<IResponsePtr> Get(const TString& url, const THeadersPtr& headers) override;
    TFuture<IResponsePtr> Post(const TString& url, const TSharedRef& body, const THeadersPtr& headers) override;
    TFuture<IResponsePtr> Patch(const TString& url, const TSharedRef& body, const THeadersPtr& headers) override;
    TFuture<IResponsePtr> Put(const TString& url, const TSharedRef& body, const THeadersPtr& headers) override;
    TFuture<IResponsePtr> Delete(const TString& url, const THeadersPtr& headers) override;

    TFuture<IActiveRequestPtr> StartPost(const TString& url, const THeadersPtr& headers) override;
    TFuture<IActiveRequestPtr> StartPatch(const TString& url, const THeadersPtr& headers) override;
    TFuture<IActiveRequestPtr> StartPut(const TString& url, const THeadersPtr& headers) override;

    TFuture<IResponsePtr> Request(EMethod method, const TString& url, const std::optional<TSharedRef>& body, const THeadersPtr& headers) override;

    MOCK_METHOD(TMockResponse, Get, (const TString& url, const TMockHeaders& headers));
    MOCK_METHOD(TMockResponse, Post, (const TString& url, const TString& body, const TMockHeaders& headers));
    MOCK_METHOD(TMockResponse, Patch, (const TString& url, const TString& body, const TMockHeaders& headers));
    MOCK_METHOD(TMockResponse, Put, (const TString& url, const TString& body, const TMockHeaders& headers));
    MOCK_METHOD(TMockResponse, Delete, (const TString& url, const TMockHeaders& headers));
    MOCK_METHOD(TMockResponse, Request, (EMethod method, const TString& url, const std::optional<TString>& body, const TMockHeaders& headers));
};

DECLARE_REFCOUNTED_CLASS(TMockClient)
DEFINE_REFCOUNTED_TYPE(TMockClient)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttp
