#pragma once

#include <yt/yt/core/http/client.h>
#include <yt/yt/core/http/http.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT::NHttp {

////////////////////////////////////////////////////////////////////////////////

using TMockHeaders = std::vector<std::pair<std::string, std::string>>;

struct TMockResponse
{
    EStatusCode StatusCode;
    std::string Body;
    TMockHeaders Headers;
};

class TMockClient
    : public IClient
{
public:
    TFuture<IResponsePtr> Get(const std::string& url, const THeadersPtr& headers) override;
    TFuture<IResponsePtr> Post(const std::string& url, const TSharedRef& body, const THeadersPtr& headers) override;
    TFuture<IResponsePtr> Patch(const std::string& url, const TSharedRef& body, const THeadersPtr& headers) override;
    TFuture<IResponsePtr> Put(const std::string& url, const TSharedRef& body, const THeadersPtr& headers) override;
    TFuture<IResponsePtr> Delete(const std::string& url, const THeadersPtr& headers) override;

    TFuture<IActiveRequestPtr> StartPost(const std::string& url, const THeadersPtr& headers) override;
    TFuture<IActiveRequestPtr> StartPatch(const std::string& url, const THeadersPtr& headers) override;
    TFuture<IActiveRequestPtr> StartPut(const std::string& url, const THeadersPtr& headers) override;

    TFuture<IResponsePtr> Request(EMethod method, const std::string& url, const std::optional<TSharedRef>& body, const THeadersPtr& headers) override;

    MOCK_METHOD(TMockResponse, Get, (const std::string& url, const TMockHeaders& headers));
    MOCK_METHOD(TMockResponse, Post, (const std::string& url, const std::string& body, const TMockHeaders& headers));
    MOCK_METHOD(TMockResponse, Patch, (const std::string& url, const std::string& body, const TMockHeaders& headers));
    MOCK_METHOD(TMockResponse, Put, (const std::string& url, const std::string& body, const TMockHeaders& headers));
    MOCK_METHOD(TMockResponse, Delete, (const std::string& url, const TMockHeaders& headers));
    MOCK_METHOD(TMockResponse, Request, (EMethod method, const std::string& url, const std::optional<std::string>& body, const TMockHeaders& headers));
};

DECLARE_REFCOUNTED_CLASS(TMockClient)
DEFINE_REFCOUNTED_TYPE(TMockClient)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttp
