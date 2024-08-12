#pragma once

#include "public.h"
#include "http.h"

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/actions/future.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NHttp {

////////////////////////////////////////////////////////////////////////////////

class TCallbackHandler
    : public IHttpHandler
{
public:
    explicit TCallbackHandler(TCallback<void(const IRequestPtr&, const IResponseWriterPtr&)> handler);

    void HandleRequest(const IRequestPtr& req, const IResponseWriterPtr& rsp) override;

private:
    const TCallback<void(const IRequestPtr&, const IResponseWriterPtr&)> Handler_;
};

////////////////////////////////////////////////////////////////////////////////

/*!
 *  Thread affinity: single-threaded
 */
struct IServer
    : public virtual TRefCounted
{
    //! Attaches a new handler.
    /*!
     *  Path matching semantic is copied from go standard library.
     *  See https://golang.org/pkg/net/http/#ServeMux
     */
    virtual void AddHandler(
        const TString& pattern,
        const IHttpHandlerPtr& handler) = 0;

    //! Returns the address this server listens at.
    virtual const NNet::TNetworkAddress& GetAddress() const = 0;

    //! Starts the server.
    /*!
     *  Must be called at most once.
     *  All #AddHandler calls must happen prior to start.
     */
    virtual void Start() = 0;

    //! Stops the server.
    /*!
     *  Can be called multiple times (and even if not started).
     */
    virtual void Stop() = 0;

    //! Sets path matcher.
    /*!
     *  Must be called before adding callbacks.
     *  @see IRequestPathMatcher
     */
    virtual void SetPathMatcher(const IRequestPathMatcherPtr& matcher) = 0;
    virtual IRequestPathMatcherPtr GetPathMatcher() = 0;


    // Extension methods
    void AddHandler(
        const TString& pattern,
        TCallback<void(const IRequestPtr& req, const IResponseWriterPtr& rsp)> handler);
};

DEFINE_REFCOUNTED_TYPE(IServer)

////////////////////////////////////////////////////////////////////////////////

IServerPtr CreateServer(
    const TServerConfigPtr& config,
    const NNet::IListenerPtr& listener,
    const NConcurrency::IPollerPtr& poller);
IServerPtr CreateServer(
    const TServerConfigPtr& config,
    const NNet::IListenerPtr& listener,
    const NConcurrency::IPollerPtr& poller,
    const NConcurrency::IPollerPtr& acceptor);
IServerPtr CreateServer(
    const TServerConfigPtr& config,
    const NConcurrency::IPollerPtr& poller);
IServerPtr CreateServer(
    const TServerConfigPtr& config,
    const NConcurrency::IPollerPtr& poller,
    const NConcurrency::IPollerPtr& acceptor);
IServerPtr CreateServer(
    int port,
    const NConcurrency::IPollerPtr& poller);
IServerPtr CreateServer(
    const TServerConfigPtr& config,
    int pollerThreadCount = 1);
IServerPtr CreateServer(
    const TServerConfigPtr& config,
    const NConcurrency::IPollerPtr& poller,
    const IInvokerPtr& invoker);

////////////////////////////////////////////////////////////////////////////////

//! IRequestPathMatcher is responsible for storing handlers and giving them back by path
struct IRequestPathMatcher
    : public virtual TRefCounted
{
    virtual void Add(const TString& pattern, const IHttpHandlerPtr& handler) = 0;
    virtual void Add(const TString& pattern, TCallback<void(const IRequestPtr&, const IResponseWriterPtr&)> handler) = 0;
    virtual IHttpHandlerPtr Match(TStringBuf path) = 0;
    virtual bool IsEmpty() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IRequestPathMatcher)

////////////////////////////////////////////////////////////////////////////////

class TRequestPathMatcher
    : public IRequestPathMatcher
{
public:
    void Add(const TString& pattern, const IHttpHandlerPtr& handler) override;
    void Add(const TString& pattern, TCallback<void(const IRequestPtr&, const IResponseWriterPtr&)> handler) override;
    IHttpHandlerPtr Match(TStringBuf path) override;
    bool IsEmpty() const override;

private:
    THashMap<TString, IHttpHandlerPtr> Exact_;
    THashMap<TString, IHttpHandlerPtr> Subtrees_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttp
