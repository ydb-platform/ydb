#include "service_combiner.h"
#include "ypath_client.h"
#include "ypath_detail.h"
#include "ypath_proxy.h"

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/rpc/dispatcher.h>

#include <yt/yt/core/yson/async_writer.h>
#include <yt/yt/core/yson/tokenizer.h>

#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NYTree {

using namespace NRpc;
using namespace NConcurrency;
using namespace NYPath;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

constexpr auto UpdateKeysFailedBackoff = TDuration::Seconds(1);

////////////////////////////////////////////////////////////////////////////////

class TServiceCombinerImpl
    : public TSupportsAttributes
{
public:
    TServiceCombinerImpl(
        std::vector<IYPathServicePtr> services,
        std::optional<TDuration> keysUpdatePeriod,
        bool updateKeysOnMissingKey)
        : Services_(std::move(services))
        , KeysUpdatePeriod_(keysUpdatePeriod)
        , UpdateKeysOnMissingKey_(updateKeysOnMissingKey)
    { }

    void Initialize()
    {
        auto workerInvoker = TDispatcher::Get()->GetHeavyInvoker();
        auto keysUpdateCallback = BIND(&TServiceCombinerImpl::UpdateKeys, MakeWeak(this));
        if (KeysUpdatePeriod_) {
            UpdateKeysExecutor_ = New<TPeriodicExecutor>(workerInvoker, keysUpdateCallback, *KeysUpdatePeriod_);
            UpdateKeysExecutor_->Start();
        } else {
            workerInvoker->Invoke(keysUpdateCallback);
        }
    }

    TFuture<void> GetInitializedFuture() const
    {
        return InitializedPromise_.ToFuture();
    }

    void SetUpdatePeriod(std::optional<TDuration> period)
    {
        if (period) {
            if (KeysUpdatePeriod_) {
                UpdateKeysExecutor_->SetPeriod(period);
            } else {
                auto workerInvoker = TDispatcher::Get()->GetHeavyInvoker();
                UpdateKeysExecutor_ = New<TPeriodicExecutor>(
                    workerInvoker,
                    BIND(&TServiceCombinerImpl::UpdateKeys, MakeWeak(this)),
                    *period);
                UpdateKeysExecutor_->Start();
            }
        } else {
            if (KeysUpdatePeriod_) {
                YT_UNUSED_FUTURE(UpdateKeysExecutor_->Stop());
            }
        }
        KeysUpdatePeriod_ = period;
    }

private:
    const std::vector<IYPathServicePtr> Services_;

    std::optional<TDuration> KeysUpdatePeriod_;
    const bool UpdateKeysOnMissingKey_;

    NConcurrency::TPeriodicExecutorPtr UpdateKeysExecutor_;

    const TPromise<void> InitializedPromise_ = NewPromise<void>();

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, KeyMappingSpinLock_);
    using TKeyMappingOrError = TErrorOr<THashMap<std::string, IYPathServicePtr>>;
    TKeyMappingOrError KeyMapping_;

    bool DoInvoke(const IYPathServiceContextPtr& context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(Get);
        DISPATCH_YPATH_SERVICE_METHOD(List);
        DISPATCH_YPATH_SERVICE_METHOD(Exists);
        return TSupportsAttributes::DoInvoke(context);
    }

    TResolveResult ResolveRecursive(const TYPath& path, const IYPathServiceContextPtr& context) override
    {
        NYPath::TTokenizer tokenizer(path);
        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::Literal);
        const auto& key = tokenizer.GetLiteralValue();

        IYPathServicePtr foundService;
        {
            auto guard = Guard(KeyMappingSpinLock_);
            const auto& keyMapping = KeyMapping_.ValueOrThrow();
            auto it = keyMapping.find(key);
            if (it == keyMapping.end()) {
                if (UpdateKeysOnMissingKey_) {
                    guard.Release();
                    UpdateKeys();
                }
            } else {
                foundService = it->second;
            }
        }

        if (!foundService && UpdateKeysOnMissingKey_) {
            auto guard = Guard(KeyMappingSpinLock_);
            const auto& keyMapping = KeyMapping_.ValueOrThrow();
            auto it = keyMapping.find(key);
            if (it != keyMapping.end()) {
                foundService = it->second;
            }
        }

        if (!foundService) {
            if (context->GetMethod() == "Exists") {
                return TResolveResultHere{path};
            }
            THROW_ERROR_EXCEPTION("Node has no child with key %Qv", key);
        }

        return TResolveResultThere{foundService, "/" + path};
    }


    void GetSelf(TReqGet* request, TRspGet* response, const TCtxGetPtr& context) override
    {
        ValidateKeyMapping();

        i64 limit = request->has_limit()
            ? request->limit()
            : DefaultVirtualChildLimit;

        context->SetRequestInfo("Limit: %v", limit);

        std::vector<TFuture<TYPathProxy::TRspGetPtr>> innerFutures;
        for (const auto& service : Services_) {
            auto innerRequest = TYPathProxy::Get(TYPath());
            innerRequest->set_limit(limit);
            if (request->has_attributes()) {
                innerRequest->mutable_attributes()->CopyFrom(request->attributes());
            }
            if (request->has_options()) {
                innerRequest->mutable_options()->CopyFrom(request->options());
            }
            innerFutures.push_back(ExecuteVerb(service, innerRequest));
        }

        auto innerResponses = WaitFor(AllSucceeded(std::move(innerFutures)))
            .ValueOrThrow();

        bool incomplete = false;
        THashMap<std::string, INodePtr> keyToChildNode;
        for (const auto& innerResponse : innerResponses) {
            auto innerNode = ConvertToNode(TYsonString(innerResponse->value()));
            if (innerNode->GetType() != ENodeType::Map) {
                THROW_ERROR_EXCEPTION("Inner service did not return a map");
            }

            if (innerNode->Attributes().Get("incomplete", false)) {
                incomplete = true;
            }

            for (const auto& [key, child] : innerNode->AsMap()->GetChildren()) {
                if (std::ssize(keyToChildNode) >= limit && !keyToChildNode.contains(key)) {
                    incomplete = true;
                    break;
                }

                keyToChildNode[key] = child;
            }
        }

        TStringStream stream;
        TYsonWriter writer(&stream);
        if (incomplete) {
            writer.OnBeginAttributes();
            writer.OnKeyedItem("incomplete");
            writer.OnBooleanScalar(true);
            writer.OnEndAttributes();
        }
        Serialize(keyToChildNode, &writer);
        writer.Flush();
        response->set_value(stream.Str());
        context->Reply();
    }

    void ListSelf(TReqList* request, TRspList* response, const TCtxListPtr& context) override
    {
        ValidateKeyMapping();

        i64 limit = request->has_limit()
            ? request->limit()
            : DefaultVirtualChildLimit;

        context->SetRequestInfo("Limit: %v", limit);

        std::vector<TFuture<TYPathProxy::TRspListPtr>> innerFutures;
        for (const auto& service : Services_) {
            auto innerRequest = TYPathProxy::List(TYPath());
            innerRequest->set_limit(limit);
            if (request->has_attributes()) {
                innerRequest->mutable_attributes()->CopyFrom(request->attributes());
            }
            innerFutures.push_back(ExecuteVerb(service, innerRequest));
        }

        auto innerResponses = WaitFor(AllSucceeded(std::move(innerFutures)))
            .ValueOrThrow();

        bool incomplete = false;
        THashSet<std::string> keys;
        for (const auto& innerResponse : innerResponses) {
            auto innerNode = ConvertToNode(TYsonString(innerResponse->value()));
            if (innerNode->GetType() != ENodeType::List) {
                THROW_ERROR_EXCEPTION("Inner service did not return a list");
            }

            if (innerNode->Attributes().Get("incomplete", false)) {
                incomplete = true;
            }

            for (const auto& key : ConvertTo<std::vector<std::string>>(innerNode)) {
                if (std::ssize(keys) >= limit && !keys.contains(key)) {
                    incomplete = true;
                    break;
                }

                keys.insert(key);
            }
        }

        TStringStream stream;
        TYsonWriter writer(&stream);
        if (incomplete) {
            writer.OnBeginAttributes();
            writer.OnKeyedItem("incomplete");
            writer.OnBooleanScalar(true);
            writer.OnEndAttributes();
        }
        Serialize(keys, &writer);
        writer.Flush();
        response->set_value(stream.Str());
        context->Reply();
    }


    void UpdateKeys()
    {
        std::vector<TFuture<std::vector<TString>>> serviceListFutures;
        for (const auto& service : Services_) {
            serviceListFutures.push_back(AsyncYPathList(service, TYPath()));
        }

        auto serviceListsOrError = WaitFor(AllSucceeded(std::move(serviceListFutures)));
        if (!serviceListsOrError.IsOK()) {
            SetKeyMapping(TError(serviceListsOrError));
            if (!KeysUpdatePeriod_) {
                auto workerInvoker = TDispatcher::Get()->GetHeavyInvoker();
                TDelayedExecutor::Submit(
                    BIND(&TServiceCombinerImpl::UpdateKeys, MakeWeak(this)),
                    UpdateKeysFailedBackoff,
                    std::move(workerInvoker));
            }
            return;
        }
        const auto& serviceLists = serviceListsOrError.Value();

        TKeyMappingOrError newKeyMappingOrError;
        auto& newKeyMapping = newKeyMappingOrError.Value();

        for (int index = 0; index < std::ssize(Services_); ++index) {
            for (const auto& key : serviceLists[index]) {
                auto [_, inserted] =  newKeyMapping.emplace(key, Services_[index]);
                if (!inserted) {
                    SetKeyMapping(TError("Key %Qv is operated by more than one of inner services",
                        key));
                    return;
                }
            }
        }

        SetKeyMapping(std::move(newKeyMappingOrError));
    }

    void ValidateKeyMapping()
    {
        auto guard = Guard(KeyMappingSpinLock_);
        // If several services already share the same key, we'd better throw an error and do nothing.
        KeyMapping_.ThrowOnError();
    }

    void SetKeyMapping(TKeyMappingOrError keyMapping)
    {
        {
            auto guard = Guard(KeyMappingSpinLock_);
            KeyMapping_ = std::move(keyMapping);
        }
        InitializedPromise_.TrySet();
    }
};

class TServiceCombiner
    : public TYPathServiceBase
    , public IServiceCombiner
{
public:
    TServiceCombiner(
        std::vector<IYPathServicePtr> services,
        std::optional<TDuration> keysUpdatePeriod,
        bool updateKeysOnMissingKey)
        : Impl_(New<TServiceCombinerImpl>(
            std::move(services),
            keysUpdatePeriod,
            updateKeysOnMissingKey))
    {
        Impl_->Initialize();
    }

    void SetUpdatePeriod(std::optional<TDuration> period) override
    {
        Impl_->SetUpdatePeriod(period);
    }

    TResolveResult Resolve(const TYPath& path, const IYPathServiceContextPtr& /*context*/) override
    {
        return TResolveResultHere{path};
    }

    void Invoke(const IYPathServiceContextPtr& context) override
    {
        Impl_->GetInitializedFuture().Subscribe(
            BIND([impl = Impl_, context = context] (const TError& error) {
                if (error.IsOK()) {
                    ExecuteVerb(impl, context);
                } else {
                    context->Reply(error);
                }
            }).Via(TDispatcher::Get()->GetHeavyInvoker()));
    }

private:
    const TIntrusivePtr<TServiceCombinerImpl> Impl_;
};

IServiceCombinerPtr CreateServiceCombiner(
    std::vector<IYPathServicePtr> services,
    std::optional<TDuration> keysUpdatePeriod,
    bool updateKeysOnMissingKey)
{
    return New<TServiceCombiner>(
        std::move(services),
        keysUpdatePeriod,
        updateKeysOnMissingKey);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree

