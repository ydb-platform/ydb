#include "service_combiner.h"
#include "ypath_client.h"
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

const TDuration UpdateKeysFailedBackoff = TDuration::Seconds(1);

////////////////////////////////////////////////////////////////////////////////

class TServiceCombiner::TImpl
    : public TSupportsAttributes
{
public:
    TImpl(
        std::vector<IYPathServicePtr> services,
        std::optional<TDuration> keysUpdatePeriod,
        bool updateKeysOnMissingKey)
        : Services_(std::move(services))
        , KeysUpdatePeriod_(keysUpdatePeriod)
        , UpdateKeysOnMissingKey_(updateKeysOnMissingKey)
    {
        auto workerInvoker = TDispatcher::Get()->GetHeavyInvoker();
        auto keysUpdateCallback = BIND(&TImpl::UpdateKeys, MakeWeak(this));
        if (keysUpdatePeriod) {
            UpdateKeysExecutor_ = New<TPeriodicExecutor>(workerInvoker, keysUpdateCallback, *keysUpdatePeriod);
            UpdateKeysExecutor_->Start();
        } else {
            workerInvoker->Invoke(keysUpdateCallback);
        }
    }

    TFuture<void> GetInitialized()
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
                UpdateKeysExecutor_ = New<TPeriodicExecutor>(workerInvoker, BIND(&TImpl::UpdateKeys, MakeWeak(this)), *period);
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
    bool UpdateKeysOnMissingKey_;

    NConcurrency::TPeriodicExecutorPtr UpdateKeysExecutor_;

    TPromise<void> InitializedPromise_ = NewPromise<void>();

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, KeyMappingSpinLock_);
    using TKeyMappingOrError = TErrorOr<THashMap<TString, IYPathServicePtr>>;
    TKeyMappingOrError KeyMapping_;

private:
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

        IYPathServicePtr foundService = nullptr;
        THashMap<TString, IYPathServicePtr>::const_iterator keyIterator;
        {
            auto guard = Guard(KeyMappingSpinLock_);
            const auto& keyMapping = KeyMapping_.ValueOrThrow();

            keyIterator = keyMapping.find(key);
            if (keyIterator == keyMapping.end()) {
                if (UpdateKeysOnMissingKey_) {
                    guard.Release();
                    UpdateKeys();
                }
            } else {
                foundService = keyIterator->second;
            }
        }

        if (!foundService && UpdateKeysOnMissingKey_) {
            auto guard = Guard(KeyMappingSpinLock_);
            const auto& keyMapping = KeyMapping_.ValueOrThrow();

            keyIterator = keyMapping.find(key);
            if (keyIterator != keyMapping.end()) {
                foundService = keyIterator->second;
            }
        }

        if (!foundService) {
            if (context->GetMethod() == "Exists") {
                return TResolveResultHere{path};
            }
            THROW_ERROR_EXCEPTION("Node has no child with key %Qv", ToYPathLiteral(key));
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

        std::atomic<bool> incomplete = {false};

        // TODO(max42): Make it more efficient :(
        std::vector<TFuture<THashMap<TString, INodePtr>>> serviceResultFutures;
        for (const auto& service : Services_) {
            auto innerRequest = TYPathProxy::Get(TYPath());
            innerRequest->set_limit(limit);
            if (request->has_attributes()) {
                innerRequest->mutable_attributes()->CopyFrom(request->attributes());
            }
            if (request->has_options()) {
                innerRequest->mutable_options()->CopyFrom(request->options());
            }
            auto asyncInnerResult = ExecuteVerb(service, innerRequest)
                .Apply(BIND([&] (TYPathProxy::TRspGetPtr response) {
                    auto node = ConvertToNode(TYsonString(response->value()));
                    if (node->Attributes().Get("incomplete", false)) {
                        incomplete = true;
                    }
                    return ConvertTo<THashMap<TString, INodePtr>>(node);
                }));
            serviceResultFutures.push_back(asyncInnerResult);
        }
        auto asyncResult = AllSucceeded(serviceResultFutures);
        auto serviceResults = WaitFor(asyncResult)
            .ValueOrThrow();

        THashMap<TString, INodePtr> combinedServiceResults;
        for (const auto& serviceResult : serviceResults) {
            if (static_cast<i64>(serviceResult.size() + combinedServiceResults.size()) > limit) {
                combinedServiceResults.insert(
                    serviceResult.begin(),
                    std::next(serviceResult.begin(), limit - static_cast<i64>(serviceResult.size())));
                incomplete = true;
                break;
            } else {
                combinedServiceResults.insert(serviceResult.begin(), serviceResult.end());
            }
        }

        TStringStream stream;
        TYsonWriter writer(&stream);

        if (incomplete) {
            BuildYsonFluently(&writer)
                .BeginAttributes()
                    .Item("incomplete").Value(true)
                .EndAttributes();
        }
        BuildYsonFluently(&writer)
            .DoMapFor(combinedServiceResults, [] (TFluentMap fluent, const decltype(combinedServiceResults)::value_type& item) {
                fluent
                    .Item(item.first).Value(item.second);
            });

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

        std::atomic<bool> incomplete = {false};

        std::vector<TFuture<std::vector<IStringNodePtr>>> serviceResultFutures;
        for (const auto& service : Services_) {
            auto innerRequest = TYPathProxy::List(TYPath());
            innerRequest->set_limit(limit);
            if (request->has_attributes()) {
                innerRequest->mutable_attributes()->CopyFrom(request->attributes());
            }
            auto asyncInnerResult = ExecuteVerb(service, innerRequest)
                .Apply(BIND([&] (TYPathProxy::TRspListPtr response) {
                    auto node = ConvertToNode(TYsonString(response->value()));
                    if (node->Attributes().Get("incomplete", false)) {
                        incomplete = true;
                    }
                    return ConvertTo<std::vector<IStringNodePtr>>(node);
                }));
            serviceResultFutures.push_back(asyncInnerResult);
        }
        auto asyncResult = AllSucceeded(serviceResultFutures);
        auto serviceResults = WaitFor(asyncResult)
            .ValueOrThrow();

        std::vector<IStringNodePtr> combinedServiceResults;
        for (const auto& serviceResult : serviceResults) {
            if (static_cast<i64>(combinedServiceResults.size() + serviceResult.size()) > limit) {
                combinedServiceResults.insert(
                    combinedServiceResults.end(),
                    serviceResult.begin(),
                    std::next(serviceResult.begin(), limit - static_cast<i64>(combinedServiceResults.size())));
                incomplete = true;
                break;
            } else {
                combinedServiceResults.insert(combinedServiceResults.end(), serviceResult.begin(), serviceResult.end());
            }
        }

        TStringStream stream;
        TYsonWriter writer(&stream);

        if (incomplete) {
            BuildYsonFluently(&writer)
                .BeginAttributes()
                    .Item("incomplete").Value(true)
                .EndAttributes();
        }

        // There is a small chance that while we waited for all services to respond, they moved into an inconsistent
        // state and provided us with non-disjoint lists. In this case we force the list to contain only unique keys.
        THashSet<TString> keys;

        BuildYsonFluently(&writer)
            .DoListFor(combinedServiceResults, [&] (TFluentList fluent, const IStringNodePtr& item) {
                if (!keys.contains(item->GetValue())) {
                    fluent
                        .Item().Value(item);
                    keys.insert(item->GetValue());
                }
            });

        writer.Flush();
        response->set_value(stream.Str());
        context->Reply();
    }


    void UpdateKeys()
    {
        std::vector<TFuture<std::vector<TString>>> serviceListFutures;
        for (const auto& service : Services_) {
            auto asyncList = AsyncYPathList(service, TYPath() /*path*/, std::numeric_limits<i64>::max() /*limit*/);
            serviceListFutures.push_back(asyncList);
        }
        auto asyncResult = AllSucceeded(serviceListFutures);
        auto serviceListsOrError = WaitFor(asyncResult);

        if (!serviceListsOrError.IsOK()) {
            SetKeyMapping(TError(serviceListsOrError));
            if (!KeysUpdatePeriod_) {
                auto workerInvoker = TDispatcher::Get()->GetHeavyInvoker();
                TDelayedExecutor::Submit(
                    BIND(&TImpl::UpdateKeys, MakeWeak(this)),
                    UpdateKeysFailedBackoff,
                    std::move(workerInvoker));
            }
            return;
        }
        const auto& serviceLists = serviceListsOrError.Value();

        TKeyMappingOrError newKeyMappingOrError;
        auto& newKeyMapping = newKeyMappingOrError.Value();

        for (int index = 0; index < static_cast<int>(Services_.size()); ++index) {
            for (const auto& key : serviceLists[index]) {
                auto pair = newKeyMapping.emplace(key, Services_[index]);
                if (!pair.second) {
                    SetKeyMapping(TError("Key %Qv is operated by more than one YPathService",
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

////////////////////////////////////////////////////////////////////////////////

TServiceCombiner::TServiceCombiner(
    std::vector<IYPathServicePtr> services,
    std::optional<TDuration> keysUpdatePeriod,
    bool updateKeysOnMissingKey)
    : Impl_(New<TImpl>(std::move(services), keysUpdatePeriod, updateKeysOnMissingKey))
{ }

TServiceCombiner::~TServiceCombiner()
{ }

void TServiceCombiner::SetUpdatePeriod(TDuration period)
{
    Impl_->SetUpdatePeriod(period);
}

IYPathService::TResolveResult TServiceCombiner::Resolve(
    const TYPath& path,
    const IYPathServiceContextPtr& /*context*/)
{
    return TResolveResultHere{path};
}

void TServiceCombiner::Invoke(const IYPathServiceContextPtr& context)
{
    Impl_->GetInitialized().Subscribe(BIND([impl = Impl_, context = context] (const TError& error) {
        if (error.IsOK()) {
            ExecuteVerb(impl, context);
        } else {
            context->Reply(error);
        }
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree

