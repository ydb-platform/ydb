#pragma once

#include <yt/yt/core/http/client.h>
#include <yt/yt/core/http/http.h>
#include <yt/yt/core/http/public.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT::NHttp {

using TMockHeaders = std::vector<std::pair<TString, TString>>;

struct TMockResponse
{
    EStatusCode StatusCode;
    TString Body;
    TMockHeaders Headers;
}; // class TMockResponse

class TMockServer
{
public:
    TMockServer(TServerConfigPtr config = nullptr);

    ~TMockServer();

    ui16 GetPort();

    // Path includes the ?query, if any.
    MOCK_METHOD(TMockResponse, Delete, (
        const TString& path, const TMockHeaders& headers));
    MOCK_METHOD(TMockResponse, Get, (
        const TString& path, const TMockHeaders& headers));
    MOCK_METHOD(TMockResponse, Head, (
        const TString& path, const TMockHeaders& headers));
    MOCK_METHOD(TMockResponse, Post, (
        const TString& path, const TString& body, const TMockHeaders& headers));
    MOCK_METHOD(TMockResponse, Put, (
        const TString& path, const TString& body, const TMockHeaders& headers));

private:
    const IServerPtr Server_;
    const IHttpHandlerPtr Handler_;
}; // class TMockHttpServer

class TMockClient
    : public IClient
{
public:
    TFuture<IResponsePtr> Get(
        const TString& url,
        const THeadersPtr& headers) override;
    TFuture<IResponsePtr> Post(
        const TString& url,
        const TSharedRef& body,
        const THeadersPtr& headers) override;
    TFuture<IResponsePtr> Patch(
        const TString& url,
        const TSharedRef& body,
        const THeadersPtr& headers) override;
    TFuture<IResponsePtr> Put(
        const TString& url,
        const TSharedRef& body,
        const THeadersPtr& headers) override;
    TFuture<IResponsePtr> Delete(
        const TString& url,
        const THeadersPtr& headers) override;

    TFuture<IActiveRequestPtr> StartPost(
        const TString& url,
        const THeadersPtr& headers) override;

    TFuture<IActiveRequestPtr> StartPatch(
        const TString& url,
        const THeadersPtr& headers) override;

    TFuture<IActiveRequestPtr> StartPut(
        const TString& url,
        const THeadersPtr& headers) override;

    MOCK_METHOD(TMockResponse, Get, (
        const TString& url, const TMockHeaders& headers));
    MOCK_METHOD(TMockResponse, Post, (
        const TString& url, const TString& body, const TMockHeaders& headers));
    MOCK_METHOD(TMockResponse, Patch, (
        const TString& url, const TString& body, const TMockHeaders& headers));
    MOCK_METHOD(TMockResponse, Put, (
        const TString& url, const TString& body, const TMockHeaders& headers));
    MOCK_METHOD(TMockResponse, Delete, (
        const TString& url, const TMockHeaders& headers));

}; // class TMockClient

DECLARE_REFCOUNTED_CLASS(TMockClient)
DEFINE_REFCOUNTED_TYPE(TMockClient)

} // namespace NYT::NHttp
