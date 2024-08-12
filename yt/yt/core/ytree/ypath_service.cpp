#include "ypath_service.h"
#include "convert.h"
#include "ephemeral_node_factory.h"
#include "tree_builder.h"
#include "ypath_client.h"
#include "ypath_detail.h"
#include "ypath_proxy.h"
#include "fluent.h"

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/rpc/dispatcher.h>

#include <yt/yt/core/yson/async_consumer.h>
#include <yt/yt/core/yson/attribute_consumer.h>
#include <yt/yt/core/yson/list_verb_lazy_yson_consumer.h>
#include <yt/yt/core/yson/null_consumer.h>
#include <yt/yt/core/yson/ypath_designated_consumer.h>
#include <yt/yt/core/yson/writer.h>

#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/checksum.h>
#include <yt/yt/core/misc/atomic_object.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

struct TCacheKey
{
    TYPath Path;
    TProtobufString Method;
    TSharedRef RequestBody;
    TChecksum RequestBodyHash;

    TCacheKey(
        const TYPath& path,
        const TProtobufString& method,
        const TSharedRef& requestBody)
        : Path(path)
        , Method(method)
        , RequestBody(requestBody)
        , RequestBodyHash(GetChecksum(RequestBody))
    { }

    bool operator == (const TCacheKey& other) const
    {
        return
            Path == other.Path &&
            Method == other.Method &&
            RequestBodyHash == other.RequestBodyHash &&
            TRef::AreBitwiseEqual(RequestBody, other.RequestBody);
    }

    friend void FormatValue(TStringBuilderBase* builder, const TCacheKey& key, TStringBuf /*spec*/)
    {
        Format(
            builder,
            "{%v %v %x}",
            key.Method,
            key.Path,
            key.RequestBodyHash);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree

template <>
struct THash<NYT::NYTree::TCacheKey>
{
    size_t operator()(const NYT::NYTree::TCacheKey& key) const
    {
        size_t result = 0;
        NYT::HashCombine(result, key.Path);
        NYT::HashCombine(result, key.Method);
        NYT::HashCombine(result, key.RequestBodyHash);
        return result;
    }
};

namespace NYT::NYTree {

using namespace NYson;
using namespace NRpc;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

void CheckProducedNonEmptyData(const TString& data)
{
    if (data.empty()) {
        THROW_ERROR_EXCEPTION(
            NRpc::EErrorCode::Unavailable,
            "Producer returned an empty result; please contact developers for further assistance");
    }
}

////////////////////////////////////////////////////////////////////////////////

class TFromProducerYPathService
    : public TYPathServiceBase
    , public TSupportsGet
    , public ICachedYPathService
{
public:
    TFromProducerYPathService(TYsonProducer producer, TDuration cachePeriod)
        : Producer_(std::move(producer))
        , CachePeriod_(cachePeriod)
    { }

    TResolveResult Resolve(
        const TYPath& path,
        const IYPathServiceContextPtr& context) override
    {
        // Try to handle root get requests without constructing ephemeral YTree.
        if (path.empty() && context->GetMethod() == "Get") {
            return TResolveResultHere{path};
        } else {
            return TResolveResultThere{BuildNodeFromProducer(), path};
        }
    }

    void SetCachePeriod(TDuration period) override
    {
        CachePeriod_ = period;
    }

private:
    const TYsonProducer Producer_;

    TYsonString CachedString_;
    INodePtr CachedNode_;
    TDuration CachePeriod_;
    TInstant LastStringUpdateTime_;
    TInstant LastNodeUpdateTime_;

    bool DoInvoke(const IYPathServiceContextPtr& context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(Get);
        return TYPathServiceBase::DoInvoke(context);
    }

    void GetSelf(TReqGet* request, TRspGet* response, const TCtxGetPtr& context) override
    {
        if (request->has_attributes())  {
            // Execute fallback.
            auto node = BuildNodeFromProducer();
            ExecuteVerb(node, context->GetUnderlyingContext());
            return;
        }

        context->SetRequestInfo();
        auto yson = BuildStringFromProducer();
        response->set_value(yson.ToString());
        context->Reply();
    }

    void GetRecursive(const TYPath& /*path*/, TReqGet* /*request*/, TRspGet* /*response*/, const TCtxGetPtr& /*context*/) override
    {
        YT_ABORT();
    }

    void GetAttribute(const TYPath& /*path*/, TReqGet* /*request*/, TRspGet* /*response*/, const TCtxGetPtr& /*context*/) override
    {
        YT_ABORT();
    }

    TYsonString BuildStringFromProducer()
    {
        if (CachePeriod_ != TDuration()) {
            auto now = NProfiling::GetInstant();
            if (LastStringUpdateTime_ + CachePeriod_ > now) {
                return CachedString_;
            }
        }

        TStringStream stream;
        TBufferedBinaryYsonWriter writer(&stream);
        Producer_.Run(&writer);
        writer.Flush();

        const auto& str = stream.Str();
        CheckProducedNonEmptyData(str);

        auto result = TYsonString(str);

        if (CachePeriod_ != TDuration()) {
            CachedString_ = result;
            LastStringUpdateTime_ = NProfiling::GetInstant();
        }

        return result;
    }

    INodePtr BuildNodeFromProducer()
    {
        if (CachePeriod_ != TDuration()) {
            auto now = NProfiling::GetInstant();
            if (LastNodeUpdateTime_ + CachePeriod_ > now) {
                return CachedNode_;
            }
        }

        auto result = ConvertTo<INodePtr>(BuildStringFromProducer());

        if (CachePeriod_ != TDuration()) {
            CachedNode_ = result;
            LastNodeUpdateTime_ = NProfiling::GetInstant();
        }

        return result;
    }
};

IYPathServicePtr IYPathService::FromProducer(TYsonProducer producer, TDuration cachePeriod)
{
    return New<TFromProducerYPathService>(std::move(producer), cachePeriod);
}

////////////////////////////////////////////////////////////////////////////////

class TFromExtendedProducerYPathService
    : public TYPathServiceBase
    , public TSupportsGet
{
    using TUnderlyingProducer = TExtendedYsonProducer<const IAttributeDictionaryPtr&>;
public:
    explicit TFromExtendedProducerYPathService(TUnderlyingProducer producer)
        : Producer_(std::move(producer))
    { }

    TResolveResult Resolve(
        const TYPath& path,
        const IYPathServiceContextPtr& context) override
    {
        // Try to handle root get requests without constructing ephemeral YTree.
        if (path.empty() && context->GetMethod() == "Get") {
            return TResolveResultHere{path};
        } else if (context->GetMethod() == "Get") {
            auto typedContext = New<TCtxGet>(context, NRpc::THandlerInvocationOptions{});
            if (!typedContext->DeserializeRequest()) {
                THROW_ERROR_EXCEPTION("Error deserializing request");
            }

            const auto& request = typedContext->Request();
            IAttributeDictionaryPtr options;
            if (request.has_options()) {
                options = NYTree::FromProto(request.options());
            }

            return TResolveResultThere{BuildNodeFromProducer(options), path};
        } else {
            return TResolveResultThere{BuildNodeFromProducer(/*options*/ CreateEphemeralAttributes()), path};
        }
    }

private:
    TUnderlyingProducer Producer_;

    bool DoInvoke(const IYPathServiceContextPtr& context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(Get);
        return TYPathServiceBase::DoInvoke(context);
    }

    void GetSelf(TReqGet* request, TRspGet* response, const TCtxGetPtr& context) override
    {
        IAttributeDictionaryPtr options;
        if (request->has_options()) {
            options = NYTree::FromProto(request->options());
        }

        if (request->has_attributes())  {
            // Execute fallback.
            auto node = BuildNodeFromProducer(options);
            ExecuteVerb(node, context->GetUnderlyingContext());
            return;
        }

        context->SetRequestInfo();
        auto yson = BuildStringFromProducer(options);
        response->set_value(yson.ToString());
        context->Reply();
    }

    void GetRecursive(const TYPath& /*path*/, TReqGet* /*request*/, TRspGet* /*response*/, const TCtxGetPtr& /*context*/) override
    {
        YT_ABORT();
    }

    void GetAttribute(const TYPath& /*path*/, TReqGet* /*request*/, TRspGet* /*response*/, const TCtxGetPtr& /*context*/) override
    {
        YT_ABORT();
    }


    TYsonString BuildStringFromProducer(const IAttributeDictionaryPtr& options)
    {
        TStringStream stream;
        {
            TBufferedBinaryYsonWriter writer(&stream);
            Producer_.Run(&writer, options);
            writer.Flush();
        }

        auto str = stream.Str();
        CheckProducedNonEmptyData(str);

        return TYsonString(std::move(str));
    }

    INodePtr BuildNodeFromProducer(const IAttributeDictionaryPtr& options)
    {
        return ConvertTo<INodePtr>(BuildStringFromProducer(options));
    }
};

IYPathServicePtr IYPathService::FromProducer(
    NYson::TExtendedYsonProducer<const IAttributeDictionaryPtr&> producer)
{
    return New<TFromExtendedProducerYPathService>(std::move(producer));
}

////////////////////////////////////////////////////////////////////////////////

class TLazyYPathServiceFromProducer
    : public TYPathServiceBase
    , public TSupportsGet
    , public TSupportsExists
    , public TSupportsList
{
public:
    explicit TLazyYPathServiceFromProducer(TYsonProducer producer)
        : Producer_(std::move(producer))
    { }

    TResolveResult Resolve(
        const TYPath& path,
        const IYPathServiceContextPtr& /*context*/) override
    {
        return TResolveResultHere{path};
    }

private:
    const TYsonProducer Producer_;

    bool DoInvoke(const IYPathServiceContextPtr& context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(Get);
        DISPATCH_YPATH_SERVICE_METHOD(Exists);
        DISPATCH_YPATH_SERVICE_METHOD(List);

        return TYPathServiceBase::DoInvoke(context);
    }

    void GetSelf(TReqGet* request, TRspGet* response, const TCtxGetPtr& context) override
    {
        if (request->has_attributes())  {
            // Execute fallback.
            auto node = BuildNodeFromProducer();
            ExecuteVerb(node, context->GetUnderlyingContext());
            return;
        }

        context->SetRequestInfo();
        auto yson = BuildStringFromProducer();
        response->set_value(yson.ToString());
        context->Reply();
    }

    void GetRecursive(const TYPath& path, TReqGet* request, TRspGet* response, const TCtxGetPtr& context) override
    {
        if (request->has_attributes())  {
            // Execute fallback.
            auto node = BuildNodeFromProducer();
            ExecuteVerb(node, context->GetUnderlyingContext());
            return;
        }

        context->SetRequestInfo();

        TStringStream stream;
        {
            TBufferedBinaryYsonWriter writer(&stream);
            auto consumer = CreateYPathDesignatedConsumer(path, EMissingPathMode::ThrowError, &writer);
            Producer_.Run(consumer.get());
            writer.Flush();
        }

        auto str = stream.Str();
        CheckProducedNonEmptyData(str);

        response->set_value(std::move(str));
        context->Reply();
    }

    void GetAttribute(const TYPath& /*path*/, TReqGet* /*request*/, TRspGet* /*response*/, const TCtxGetPtr& context) override
    {
        // Execute fallback.
        auto node = BuildNodeFromProducer();
        ExecuteVerb(node, context->GetUnderlyingContext());
    }

    void ListSelf(TReqList* request, TRspList* response, const TCtxListPtr& context) override
    {
        ListRecursive("", request, response, context);
    }

    void ListRecursive(const TYPath& path, TReqList* request, TRspList* response, const TCtxListPtr& context) override
    {
        if (request->has_attributes())  {
            // Execute fallback.
            auto node = BuildNodeFromProducer();
            ExecuteVerb(node, context->GetUnderlyingContext());
            return;
        }

        context->SetRequestInfo();

        auto limit = request->has_limit()
            ? std::optional(request->limit())
            : std::nullopt;

        TStringStream stream;
        TBufferedBinaryYsonWriter writer(&stream);
        TListVerbLazyYsonConsumer lazyConsumer(&writer, limit);
        if (path.empty()) {
            Producer_.Run(&lazyConsumer);
        } else {
            auto consumer = CreateYPathDesignatedConsumer(path, EMissingPathMode::ThrowError, &lazyConsumer);
            Producer_.Run(consumer.get());
        }
        writer.Flush();

        auto str = stream.Str();
        CheckProducedNonEmptyData(str);

        response->set_value(std::move(str));

        context->Reply();
    }

    void ListAttribute(const TYPath& /*path*/, TReqList* /*request*/, TRspList* /*response*/, const TCtxListPtr& context) override
    {
        // Execute fallback.
        auto node = BuildNodeFromProducer();
        ExecuteVerb(node, context->GetUnderlyingContext());
    }

    void ExistsRecursive(const TYPath& path, TReqExists* /*request*/, TRspExists* /*response*/, const TCtxExistsPtr& context) override
    {
        context->SetRequestInfo();

        auto consumer = CreateYPathDesignatedConsumer(path, EMissingPathMode::ThrowError, GetNullYsonConsumer());
        try {
            Producer_.Run(consumer.get());
        } catch (const TErrorException& ex) {
            if (ex.Error().FindMatching(EErrorCode::ResolveError)) {
                Reply(context, /*exists*/ false);
                return;
            }
            throw;
        }

        Reply(context, /*exists*/ true);
    }

    TYsonString BuildStringFromProducer()
    {
        TStringStream stream;
        TBufferedBinaryYsonWriter writer(&stream);
        Producer_.Run(&writer);
        writer.Flush();

        auto str = stream.Str();
        CheckProducedNonEmptyData(str);

        return TYsonString(std::move(str));
    }

    INodePtr BuildNodeFromProducer()
    {
        auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
        Producer_.Run(builder.get());
        return builder->EndTree();
    }
};

IYPathServicePtr IYPathService::FromProducerLazy(TYsonProducer producer)
{
    return New<TLazyYPathServiceFromProducer>(std::move(producer));
}

////////////////////////////////////////////////////////////////////////////////

class TYPathServiceToProducerHandler
    : public TRefCounted
{
public:
    TYPathServiceToProducerHandler(
        IYPathServicePtr underlyingService,
        IInvokerPtr workerInvoker,
        TDuration updatePeriod)
        : UnderlyingService_(std::move(underlyingService))
        , WorkerInvoker_(std::move(workerInvoker))
        , UpdatePeriod_(updatePeriod)
    { }

    TYsonProducer Run()
    {
        ScheduleUpdate(true);
        return TYsonProducer(BIND(&TYPathServiceToProducerHandler::Produce, MakeStrong(this)));
    }

private:
    const IYPathServicePtr UnderlyingService_;
    const IInvokerPtr WorkerInvoker_;
    const TDuration UpdatePeriod_;

    TAtomicObject<TYsonString> CachedString_ = {BuildYsonStringFluently().Entity()};

    void Produce(IYsonConsumer* consumer)
    {
        consumer->OnRaw(CachedString_.Load());
    }

    void ScheduleUpdate(bool immediately)
    {
        TDelayedExecutor::Submit(
            BIND(&TYPathServiceToProducerHandler::OnUpdate, MakeWeak(this)),
            immediately ? TDuration::Zero() : UpdatePeriod_,
            WorkerInvoker_);
    }

    void OnUpdate()
    {
        AsyncYPathGet(UnderlyingService_, TYPath())
            .Subscribe(BIND(&TYPathServiceToProducerHandler::OnUpdateResult, MakeWeak(this)));
    }

    void OnUpdateResult(const TErrorOr<TYsonString>& errorOrString)
    {
        if (errorOrString.IsOK()) {
            CachedString_.Store(errorOrString.Value());
        } else {
            CachedString_.Store(BuildYsonStringFluently()
                .BeginAttributes()
                    .Item("error").Value(TError(errorOrString))
                .EndAttributes()
                .Entity());
        }
        ScheduleUpdate(false);
    }
};

TYsonProducer IYPathService::ToProducer(
    IInvokerPtr workerInvoker,
    TDuration updatePeriod)
{
    return New<TYPathServiceToProducerHandler>(
        this,
        std::move(workerInvoker),
        updatePeriod)
        ->Run();
}

////////////////////////////////////////////////////////////////////////////////

class TViaYPathService
    : public TYPathServiceBase
{
public:
    TViaYPathService(
        IYPathServicePtr underlyingService,
        IInvokerPtr invoker)
        : UnderlyingService_(underlyingService)
        , Invoker_(invoker)
    { }

    TResolveResult Resolve(
        const TYPath& path,
        const IYPathServiceContextPtr& /*context*/) override
    {
        return TResolveResultHere{path};
    }

    bool ShouldHideAttributes() override
    {
        return UnderlyingService_->ShouldHideAttributes();
    }

private:
    const IYPathServicePtr UnderlyingService_;
    const IInvokerPtr Invoker_;


    bool DoInvoke(const IYPathServiceContextPtr& context) override
    {
        Invoker_->Invoke(BIND([=, this, this_ = MakeStrong(this)] {
            ExecuteVerb(UnderlyingService_, context);
        }));
        return true;
    }
};

IYPathServicePtr IYPathService::Via(IInvokerPtr invoker)
{
    return New<TViaYPathService>(this, invoker);
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TCacheProfilingCounters)

struct TCacheProfilingCounters
    : public TRefCounted
{
    explicit TCacheProfilingCounters(const NProfiling::TProfiler& profiler)
        : CacheHitCounter(profiler.Counter("/cache_hit"))
        , CacheMissCounter(profiler.Counter("/cache_miss"))
        , RedundantCacheMissCounter(profiler.Counter("/redundant_cache_miss"))
        , InvalidCacheHitCounter(profiler.Counter("/invalid_cache_hit"))
        , ByteSize(profiler.Gauge("/byte_size"))
    { }

    NProfiling::TCounter CacheHitCounter;
    NProfiling::TCounter CacheMissCounter;
    NProfiling::TCounter RedundantCacheMissCounter;
    NProfiling::TCounter InvalidCacheHitCounter;
    NProfiling::TGauge ByteSize;
};

DEFINE_REFCOUNTED_TYPE(TCacheProfilingCounters)

DECLARE_REFCOUNTED_CLASS(TCacheSnapshot)

class TCacheSnapshot
    : public TRefCounted
{
public:
    TCacheSnapshot(TErrorOr<INodePtr> treeOrError, TCacheProfilingCountersPtr profilingCounters)
        : TreeOrError_(std::move(treeOrError))
        , ProfilingCounters_(std::move(profilingCounters))
    { }

    const TErrorOr<INodePtr>& GetTreeOrError() const
    {
        return TreeOrError_;
    }

    void AddResponse(const TCacheKey& key, const TSharedRefArray& responseMessage)
    {
        auto guard = WriterGuard(Lock_);

        decltype(KeyToResponseMessage_)::insert_ctx ctx;
        auto it = KeyToResponseMessage_.find(key, ctx);

        if (it == KeyToResponseMessage_.end()) {
            KeyToResponseMessage_.emplace_direct(ctx, key, responseMessage);
        } else {
            ProfilingCounters_->RedundantCacheMissCounter.Increment();
        }
    }

    std::optional<TErrorOr<TSharedRefArray>> LookupResponse(const TCacheKey& key) const
    {
        auto guard = ReaderGuard(Lock_);

        auto it = KeyToResponseMessage_.find(key);
        if (it == KeyToResponseMessage_.end()) {
            return std::nullopt;
        } else {
            return it->second;
        }
    }

private:
    const TErrorOr<INodePtr> TreeOrError_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, Lock_);
    THashMap<TCacheKey, TSharedRefArray> KeyToResponseMessage_;
    TCacheProfilingCountersPtr ProfilingCounters_;
};

DEFINE_REFCOUNTED_TYPE(TCacheSnapshot)

DECLARE_REFCOUNTED_CLASS(TCachedYPathServiceContext)

class TCachedYPathServiceContext
    : public TYPathServiceContextWrapper
{
public:
    TCachedYPathServiceContext(
        const IYPathServiceContextPtr& underlyingContext,
        TWeakPtr<TCacheSnapshot> cacheSnapshot,
        TCacheKey cacheKey)
        : TYPathServiceContextWrapper(underlyingContext)
        , CacheSnapshot_(std::move(cacheSnapshot))
        , CacheKey_(std::move(cacheKey))
    {
        underlyingContext->GetAsyncResponseMessage()
            .Subscribe(BIND([this, weakThis = MakeWeak(this)] (const TErrorOr<TSharedRefArray>& responseMessageOrError) {
                if (auto this_ = weakThis.Lock()) {
                    if (responseMessageOrError.IsOK()) {
                        TryAddResponseToCache(responseMessageOrError.Value());
                    }
                }
            }));
    }

private:
    const TWeakPtr<TCacheSnapshot> CacheSnapshot_;
    const TCacheKey CacheKey_;

    void TryAddResponseToCache(const TSharedRefArray& responseMessage)
    {
        if (auto cacheSnapshot = CacheSnapshot_.Lock()) {
            cacheSnapshot->AddResponse(CacheKey_, responseMessage);
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TCachedYPathServiceContext)

DECLARE_REFCOUNTED_CLASS(TCachedYPathService)

class TCachedYPathService
    : public TYPathServiceBase
    , public ICachedYPathService
{
public:
    TCachedYPathService(
        IYPathServicePtr underlyingService,
        TDuration updatePeriod,
        IInvokerPtr workerInvoker,
        const NProfiling::TProfiler& profiler);

    TResolveResult Resolve(const TYPath& path, const IYPathServiceContextPtr& /*context*/) override;

    void SetCachePeriod(TDuration period) override;

private:
    const IYPathServicePtr UnderlyingService_;
    const IInvokerPtr WorkerInvoker_;

    const TPeriodicExecutorPtr PeriodicExecutor_;

    std::atomic<bool> IsCacheEnabled_ = false;
    std::atomic<bool> IsCacheValid_ = false;

    TCacheProfilingCountersPtr ProfilingCounters_;

    TAtomicIntrusivePtr<TCacheSnapshot> CurrentCacheSnapshot_;

    bool DoInvoke(const IYPathServiceContextPtr& context) override;

    void RebuildCache();

    void UpdateCachedTree(const TErrorOr<INodePtr>& treeOrError);
};

DEFINE_REFCOUNTED_TYPE(TCachedYPathService)

TCachedYPathService::TCachedYPathService(
    IYPathServicePtr underlyingService,
    TDuration updatePeriod,
    IInvokerPtr workerInvoker,
    const NProfiling::TProfiler& profiler)
    : UnderlyingService_(std::move(underlyingService))
    , WorkerInvoker_(workerInvoker
        ? workerInvoker
        : NRpc::TDispatcher::Get()->GetHeavyInvoker())
    , PeriodicExecutor_(New<TPeriodicExecutor>(
        WorkerInvoker_,
        BIND(&TCachedYPathService::RebuildCache, MakeWeak(this)),
        updatePeriod))
    , ProfilingCounters_(New<TCacheProfilingCounters>(profiler))
{
    YT_VERIFY(UnderlyingService_);
    SetCachePeriod(updatePeriod);
}

IYPathService::TResolveResult TCachedYPathService::Resolve(
    const TYPath& path,
    const IYPathServiceContextPtr& /*context*/)
{
    return TResolveResultHere{path};
}

void TCachedYPathService::SetCachePeriod(TDuration period)
{
    if (period == TDuration::Zero()) {
        if (IsCacheEnabled_) {
            IsCacheEnabled_.store(false);
            YT_UNUSED_FUTURE(PeriodicExecutor_->Stop());
        }
    } else {
        PeriodicExecutor_->SetPeriod(period);
        if (!IsCacheEnabled_) {
            IsCacheEnabled_.store(true);
            IsCacheValid_.store(false);
            PeriodicExecutor_->Start();
        }
    }
}

void ReplyErrorOrValue(const IYPathServiceContextPtr& context, const TErrorOr<TSharedRefArray>& response)
{
    if (response.IsOK()) {
        context->Reply(response.Value());
    } else {
        context->Reply(static_cast<TError>(response));
    }
}

bool TCachedYPathService::DoInvoke(const IYPathServiceContextPtr& context)
{
    if (IsCacheEnabled_ && IsCacheValid_) {
        WorkerInvoker_->Invoke(BIND([this, context, this_ = MakeStrong(this)] {
            try {
                auto cacheSnapshot = CurrentCacheSnapshot_.Acquire();
                YT_VERIFY(cacheSnapshot);

                if (context->GetRequestMessage().Size() < 2) {
                    context->Reply(TError("Invalid request"));
                    return;
                }

                TCacheKey key(
                    GetRequestTargetYPath(context->GetRequestHeader()),
                    context->GetRequestHeader().method(),
                    context->GetRequestMessage()[1]);

                if (auto cachedResponse = cacheSnapshot->LookupResponse(key)) {
                    ReplyErrorOrValue(context, *cachedResponse);
                    ProfilingCounters_->CacheHitCounter.Increment();
                    return;
                }

                auto treeOrError = cacheSnapshot->GetTreeOrError();
                if (!treeOrError.IsOK()) {
                    context->Reply(static_cast<TError>(treeOrError));
                    return;
                }
                const auto& tree = treeOrError.Value();

                auto contextWrapper = New<TCachedYPathServiceContext>(context, MakeWeak(cacheSnapshot), std::move(key));
                ExecuteVerb(tree, contextWrapper);
                ProfilingCounters_->CacheMissCounter.Increment();
            } catch (const std::exception& ex) {
                context->Reply(ex);
            }
        }));
    } else {
        UnderlyingService_->Invoke(context);
        ProfilingCounters_->InvalidCacheHitCounter.Increment();
    }

    return true;
}

void TCachedYPathService::RebuildCache()
{
    try {
        auto asyncYson = AsyncYPathGet(UnderlyingService_, /*path*/ TYPath(), TAttributeFilter());

        auto yson = WaitFor(asyncYson)
            .ValueOrThrow();

        ProfilingCounters_->ByteSize.Update(yson.AsStringBuf().Size());

        UpdateCachedTree(ConvertToNode(yson));
    } catch (const std::exception& ex) {
        UpdateCachedTree(TError(ex));
    }
}

void TCachedYPathService::UpdateCachedTree(const TErrorOr<INodePtr>& treeOrError)
{
    auto newCachedTree = New<TCacheSnapshot>(treeOrError, ProfilingCounters_);
    CurrentCacheSnapshot_.Store(newCachedTree);
    IsCacheValid_ = true;
}

IYPathServicePtr IYPathService::Cached(
    TDuration updatePeriod,
    IInvokerPtr workerInvoker,
    const NProfiling::TProfiler& profiler)
{
    return New<TCachedYPathService>(this, updatePeriod, workerInvoker, profiler);
}

////////////////////////////////////////////////////////////////////////////////

class TPermissionValidatingYPathService
    : public TYPathServiceBase
    , public TSupportsPermissions
{
public:
    TPermissionValidatingYPathService(
        IYPathServicePtr underlyingService,
        TCallback<void(const TString&, EPermission)> validationCallback)
        : UnderlyingService_(std::move(underlyingService))
        , ValidationCallback_(std::move(validationCallback))
        , PermissionValidator_(this, EPermissionCheckScope::This)
    { }

    TResolveResult Resolve(
        const TYPath& path,
        const IYPathServiceContextPtr& /*context*/) override
    {
        return TResolveResultHere{path};
    }

    bool ShouldHideAttributes() override
    {
        return UnderlyingService_->ShouldHideAttributes();
    }

private:
    const IYPathServicePtr UnderlyingService_;
    const TCallback<void(const TString&, EPermission)> ValidationCallback_;

    TCachingPermissionValidator PermissionValidator_;

    void ValidatePermission(
        EPermissionCheckScope /*scope*/,
        EPermission permission,
        const TString& user) override
    {
        ValidationCallback_.Run(user, permission);
    }

    bool DoInvoke(const IYPathServiceContextPtr& context) override
    {
        // TODO(max42): choose permission depending on method.
        PermissionValidator_.Validate(EPermission::Read, context->GetAuthenticationIdentity().User);
        ExecuteVerb(UnderlyingService_, context);
        return true;
    }
};

IYPathServicePtr IYPathService::WithPermissionValidator(TCallback<void(const TString&, EPermission)> validationCallback)
{
    return New<TPermissionValidatingYPathService>(this, std::move(validationCallback));
}

////////////////////////////////////////////////////////////////////////////////

void IYPathService::WriteAttributesFragment(
    IAsyncYsonConsumer* consumer,
    const TAttributeFilter& attributeFilter,
    bool stable)
{
    if (!attributeFilter && ShouldHideAttributes()) {
        return;
    }
    DoWriteAttributesFragment(consumer, attributeFilter, stable);
}

void IYPathService::WriteAttributes(
    IAsyncYsonConsumer* consumer,
    const TAttributeFilter& attributeFilter,
    bool stable)
{
    TAttributeFragmentConsumer attributesConsumer(consumer);
    WriteAttributesFragment(&attributesConsumer, attributeFilter, stable);
    attributesConsumer.Finish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
