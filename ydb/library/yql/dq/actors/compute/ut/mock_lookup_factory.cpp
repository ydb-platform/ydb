#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/yql/dq/actors/compute/dq_async_compute_actor.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/dq/actors/compute/ut/proto/mock.pb.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include "mock_lookup_factory.h"

using namespace NActors;

namespace NYql::NDq {

namespace {

class TMockLookupActor
    : public NYql::NDq::IDqAsyncLookupSource,
      public NActors::TActorBootstrapped<TMockLookupActor> {
    using TBase = NActors::TActorBootstrapped<TMockLookupActor>;

public:
    TMockLookupActor(
        NActors::TActorId&& parentId,
        ::NMonitoring::TDynamicCounterPtr /*taskCounters*/,
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc,
        std::shared_ptr<IDqAsyncLookupSource::TKeyTypeHelper> keyTypeHelper,
        Mock::TLookupSource&& lookupSource,
        const NKikimr::NMiniKQL::TStructType* keyType,
        const NKikimr::NMiniKQL::TStructType* payloadType,
        const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv,
        const NKikimr::NMiniKQL::THolderFactory& holderFactory,
        const size_t maxKeysInRequest)
        : ParentId(std::move(parentId))
        , Alloc(alloc)
        , KeyTypeHelper(keyTypeHelper)
        , LookupSource(std::move(lookupSource))
        , KeyType(keyType)
        , PayloadType(payloadType)
        , HolderFactory(holderFactory)
        , MaxKeysInRequest(std::min(maxKeysInRequest, size_t{100}))
    {
        Y_ENSURE(KeyType->GetMembersCount() == 1);
        auto key1Type = KeyType->GetMemberType(0);
        if (key1Type->IsOptional()) {
            key1Type = static_cast<NKikimr::NMiniKQL::TOptionalType*>(key1Type)->GetItemType();
        }
        Y_ENSURE(key1Type->IsSameType(*NKikimr::NMiniKQL::TDataType::Create(NUdf::TDataType<i32>::Id, typeEnv)));
        Y_ENSURE(PayloadType->GetMembersCount() == 1);
        auto payload1Type = PayloadType->GetMemberType(0);
        if (payload1Type->IsOptional()) {
            payload1Type = static_cast<NKikimr::NMiniKQL::TOptionalType*>(payload1Type)->GetItemType();
        }
        Y_ENSURE(payload1Type->IsSameType(*NKikimr::NMiniKQL::TDataType::Create(NUdf::TDataType<char*>::Id, typeEnv)));
    }

    ~TMockLookupActor() {
        Free();
    }

private:
    void Free() {
        auto guard = Guard(*Alloc);
        Request.reset();
        KeyTypeHelper.reset();
    }
public:

    void Bootstrap() {
        Become(&TMockLookupActor::StateFunc);
    }

    static constexpr char ActorName[] = "MOCK_LOOKUP_ACTOR";

private: // IDqAsyncLookupSource
    size_t GetMaxSupportedKeysInRequest() const override {
        return MaxKeysInRequest;
    }
    void AsyncLookup(std::weak_ptr<IDqAsyncLookupSource::TUnboxedValueMap> request) override {
        auto guard = Guard(*Alloc);
        CreateRequest(request.lock());
    }
    void PassAway() override {
        Free();
        TBase::PassAway();
    }

private: // events
    STRICT_STFUNC(StateFunc,
                  hFunc(TEvLookupRequest, Handle);
                  hFunc(NActors::TEvents::TEvPoison, Handle);)

    void Handle(NActors::TEvents::TEvPoison::TPtr) {
        PassAway();
    }

    void Handle(TEvLookupRequest::TPtr ev) {
        auto guard = Guard(*Alloc);
        CreateRequest(ev->Get()->Request.lock());
    }

private:
    void CreateRequest(std::shared_ptr<IDqAsyncLookupSource::TUnboxedValueMap> request) {
        if (!request) {
            return;
        }
        Y_ABORT_IF(request->size() == 0 || request->size() > MaxKeysInRequest);
        Request = std::move(request);
        SendRequest();
    }

    void SendRequest() {
        FinalizeRequest();
    }

    void FinalizeRequest() {
        auto guard = Guard(*Alloc);
        i32 minValue = LookupSource.GetMinValue();
        i32 maxValue = LookupSource.GetMaxValue();
        auto key1Type = KeyType->GetMemberType(0);
        for (auto& [key, value] : *Request) {
            Y_ENSURE(key);
            auto key1 = key.GetElement(0);
            Y_ENSURE(key1);
            if (key1Type->IsOptional()) {
                key1 = key1.GetOptionalValue();
                Y_ENSURE(key1);
            }
            Y_ENSURE(key1.IsEmbedded());
            const auto val = key1.Get<i32>();
            if (val >= minValue && val <= maxValue) {
                NUdf::TUnboxedValue* valueItems;
                value = HolderFactory.CreateDirectArrayHolder(PayloadType->GetMembersCount(), valueItems);

                auto str = ToString(val);
                valueItems[0] = NKikimr::NMiniKQL::MakeString(NUdf::TStringRef(str.data(), str.size()));
                if (PayloadType->GetMemberType(0)->IsOptional()) {
                    valueItems[0] = valueItems[0].MakeOptional();
                }
            } else if (val == 999) { // simulate error
                SendError();
                return;
            }
        }
        auto ev = new IDqAsyncLookupSource::TEvLookupResult(Request);
        Request.reset();
        TActivationContext::ActorSystem()->Send(new NActors::IEventHandle(ParentId, SelfId(), ev));
    }

    void SendError() {
        auto actorSystem = TActivationContext::ActorSystem();
        TIssues issues;
        issues.AddIssue(TString("User touched mousetrap"));
        auto errEv = std::make_unique<IDqComputeActorAsyncInput::TEvAsyncInputError>(
                -1,
                issues,
                NYql::NDqProto::StatusIds::GENERIC_ERROR);
        actorSystem->Send(new NActors::IEventHandle(ParentId, SelfId(), errEv.release()));
    }

private:
    const NActors::TActorId ParentId;
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
    std::shared_ptr<TKeyTypeHelper> KeyTypeHelper;
    Mock::TLookupSource LookupSource;
    const NKikimr::NMiniKQL::TStructType* const KeyType;
    const NKikimr::NMiniKQL::TStructType* const PayloadType;
    const NKikimr::NMiniKQL::THolderFactory& HolderFactory;
    const size_t MaxKeysInRequest;
    std::shared_ptr<IDqAsyncLookupSource::TUnboxedValueMap> Request;
};

std::pair<NYql::NDq::IDqAsyncLookupSource*, NActors::IActor*> CreateMockLookupActor(
    NActors::TActorId parentId,
    ::NMonitoring::TDynamicCounterPtr taskCounters,
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc,
    std::shared_ptr<IDqAsyncLookupSource::TKeyTypeHelper> keyTypeHelper,
    Mock::TLookupSource&& lookupSource,
    const NKikimr::NMiniKQL::TStructType* keyType,
    const NKikimr::NMiniKQL::TStructType* payloadType,
    const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv,
    const NKikimr::NMiniKQL::THolderFactory& holderFactory,
    const size_t maxKeysInRequest)
{
    auto guard = Guard(*alloc);
    const auto actor = new TMockLookupActor(
        std::move(parentId),
        taskCounters,
        alloc,
        keyTypeHelper,
        std::move(lookupSource),
        keyType,
        payloadType,
        typeEnv,
        holderFactory,
        maxKeysInRequest);
    return {actor, actor};
}

} // anonymous namespace

void RegisterMockProviderFactories(TDqAsyncIoFactory& factory) {
    auto lookupActorFactory = [](Mock::TLookupSource&& lookupSource, IDqAsyncIoFactory::TLookupSourceArguments&& args) {
        return CreateMockLookupActor(
                std::move(args.ParentId),
                std::move(args.TaskCounters),
                std::move(args.Alloc),
                std::move(args.KeyTypeHelper),
                std::move(lookupSource),
                args.KeyType,
                args.PayloadType,
                args.TypeEnv,
                args.HolderFactory,
                args.MaxKeysInRequest);
    };

    factory.RegisterLookupSource<Mock::TLookupSource>("MockLookup", lookupActorFactory);
}

}
