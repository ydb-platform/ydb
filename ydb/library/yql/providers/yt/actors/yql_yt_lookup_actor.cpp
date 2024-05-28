#include "yql_yt_lookup_actor.h"

#include <ydb/library/yql/providers/yt/gateway/file/yql_yt_file_text_yson.h>
#include <ydb/library/yql/providers/yt/codec/yt_codec.h>
#include <ydb/library/yql/providers/yt/comp_nodes/yql_mkql_file_input_state.h>
#include <ydb/library/yql/providers/yt/comp_nodes/yql_mkql_file_list.h>
#include <ydb/library/yql/providers/yt/common/yql_names.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/schema/parser/yql_type_parser.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/dq/runtime/dq_arrow_helpers.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/mkql_proto/mkql_proto.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/yql/utils/log/log.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <library/cpp/yson/node/node_io.h>

namespace NYql::NDq {

using namespace NActors;

namespace {


enum class EColumnType {
    None,
    Key,
    Payload
};

using TIndexedColumns = std::vector<std::pair<EColumnType, size_t>>;


//Note: TStringBuf is used, that refers to arg's memory. Be careful with object's lifetimes
THashMap<TStringBuf, size_t> MemberToIndex(const NKikimr::NMiniKQL::TStructType* s) {
    THashMap<TStringBuf, size_t> result;
    for (ui32 i = 0; i != s->GetMembersCount(); ++i) {
        result[s->GetMemberName(i)] = i;
    }
    return result;
}

} // namespace

class TYtLookupActor
    : public NYql::NDq::IDqAsyncLookupSource,
        public NActors::TActorBootstrapped<TYtLookupActor> 
{
    using TBase = NActors::TActorBootstrapped<TYtLookupActor>;
public:
    TYtLookupActor(
        NFile::TYtFileServices::TPtr ytServices,
        NActors::TActorId parentId,
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc,
        const NKikimr::NMiniKQL::IFunctionRegistry& functionRegistry,
        const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv,
        NYql::NYt::NSource::TLookupSource&& lookupSource,
        const NKikimr::NMiniKQL::TStructType* keyType,
        const NKikimr::NMiniKQL::TStructType* payloadType,
        const NKikimr::NMiniKQL::THolderFactory& holderFactory,
        const size_t maxKeysInRequest)
        : YtServices(ytServices)
        , ParentId(std::move(parentId))
        , Alloc(alloc)
        , FunctionRegistry(functionRegistry)
        , LookupSource(std::move(lookupSource))
        , KeyType(keyType)
        , PayloadType(payloadType)
        , HolderFactory(holderFactory)
        , TypeEnv(typeEnv)
        , MaxKeysInRequest(maxKeysInRequest)
        , KeyTypeHelper(keyType)
        , Data(10,
            KeyTypeHelper.GetValueHash(),
            KeyTypeHelper.GetValueEqual()
        )
    {
    }
    ~TYtLookupActor() {
        auto guard = Guard(*Alloc);
        KeyTypeHelper = TKeyTypeHelper{};
        Data = TTableData(0, KeyTypeHelper.GetValueHash(), KeyTypeHelper.GetValueEqual());
    }


    void Bootstrap() {
        YQL_CLOG(INFO, ProviderYt) << "New Yt proivider lookup source actor(ActorId=" << SelfId() << ") for"
                                        << " cluster=" << LookupSource.GetCluster()
                                        << ", table=" << LookupSource.GetTable();
        auto path = YtServices->GetTablePath(LookupSource.cluster(),  LookupSource.table(), false);

        auto guard = Guard(*Alloc);
        NCommon::TCodecContext codecCtx(TypeEnv, FunctionRegistry, &HolderFactory);
        const auto tableCodecSpec = LookupSource.GetRowSpec();

        const auto meta = TString("{\"") + YqlIOSpecTables + "\" = [" + tableCodecSpec + "]}";

        TMkqlIOSpecs specs;
        specs.Init(codecCtx, meta, {path}, TMaybe<TVector<TString>>{});
        TVector<std::pair<TString, NYql::NFile::TColumnsInfo>> files{{path, NYql::NFile::TColumnsInfo{}}};
        THolder<IInputState> input = MakeHolder<TFileInputState>(specs, HolderFactory, NYql::NFile::MakeTextYsonInputs(files), 0u, 1_MB);

        auto keyColumns = MemberToIndex(KeyType);
        auto payloadColumns = MemberToIndex(PayloadType);

        std::vector<std::pair<EColumnType, size_t>> columnDestinations(specs.Inputs[0]->Fields.size());
        for (const auto& [k, f]: specs.Inputs[0]->Fields) {
            if (const auto p = keyColumns.FindPtr(k)) {
                columnDestinations[f.StructIndex] = {EColumnType::Key, *p};
            } else if (const auto p = payloadColumns.FindPtr(k)) {
                columnDestinations[f.StructIndex] = {EColumnType::Payload, *p};
            } else {
                columnDestinations[f.StructIndex] = {EColumnType::None, -1};
            }
        }
        NUdf::TUnboxedValue v;
        //read all table data
        for(;input->IsValid(); input->Next()) {
            NUdf::TUnboxedValue inputValue = input->GetCurrent();
            NUdf::TUnboxedValue* keyItems;
            NUdf::TUnboxedValue key = HolderFactory.CreateDirectArrayHolder(KeyType->GetMembersCount(), keyItems);
            NUdf::TUnboxedValue* payloadItems;
            NUdf::TUnboxedValue payload = HolderFactory.CreateDirectArrayHolder(PayloadType->GetMembersCount(), payloadItems);
            for (size_t i = 0; i != columnDestinations.size(); ++i) {
                switch(columnDestinations[i].first) {
                    case EColumnType::Key:
                        keyItems[columnDestinations[i].second] = inputValue.GetElement(i);
                        break;
                    case EColumnType::Payload:
                        payloadItems[columnDestinations[i].second] = inputValue.GetElement(i);
                        break;
                    case EColumnType::None:
                        break;
                }
            }
            Data.emplace(std::move(key), std::move(payload));

        }
        Become(&TYtLookupActor::StateFunc);
    }

    static constexpr char ActorName[] = "YT_PROVIDER_LOOKUP_ACTOR";

private: //IDqAsyncLookupSource
    size_t GetMaxSupportedKeysInRequest() const override {
        return MaxKeysInRequest;
    }
    void AsyncLookup(const NKikimr::NMiniKQL::TUnboxedValueVector& keys) override {
        YQL_CLOG(DEBUG, ProviderYt) << "ActorId=" << SelfId() << " Got LookupRequest for " << keys.size() << " keys";
        Y_ABORT_IF(InProgress);
        Y_ABORT_IF(keys.size() > MaxKeysInRequest);
        InProgress = true;
        auto guard = Guard(*Alloc);
        NKikimr::NMiniKQL::TKeyPayloadPairVector lookupResult;
        lookupResult.reserve(keys.size());
        for (const  auto& k: keys) {
            const auto it = Data.find(k);
            lookupResult.emplace_back(k, it != Data.end() ? it->second : NUdf::TUnboxedValue{});
        }
        auto ev = new IDqAsyncLookupSource::TEvLookupResult(Alloc, std::move(lookupResult));
        TActivationContext::ActorSystem()->Send(new NActors::IEventHandle(ParentId, SelfId(), ev));
        InProgress = false;
    }

private: //events
    STRICT_STFUNC(StateFunc,
        hFunc(NActors::TEvents::TEvPoison, Handle);
    )

    void Handle(NActors::TEvents::TEvPoison::TPtr) {
        PassAway();
    }

private:
    enum class EColumnDestination {
        Key,
        Payload
    };

private:
    NFile::TYtFileServices::TPtr YtServices;
    const NActors::TActorId ParentId;
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
    const NKikimr::NMiniKQL::IFunctionRegistry& FunctionRegistry;
    NYql::NYt::NSource::TLookupSource LookupSource;
    const NKikimr::NMiniKQL::TStructType* const KeyType;
    const NKikimr::NMiniKQL::TStructType* const PayloadType;
    const NKikimr::NMiniKQL::THolderFactory& HolderFactory;
    const NKikimr::NMiniKQL::TTypeEnvironment& TypeEnv;
    const size_t MaxKeysInRequest;
    std::atomic_bool InProgress;
    using TKeyTypeHelper = NKikimr::NMiniKQL::TKeyTypeContanerHelper<true, true, false>;
    TKeyTypeHelper KeyTypeHelper;
    using TTableData = std::unordered_map<
        NUdf::TUnboxedValue,
        NUdf::TUnboxedValue,
        NKikimr::NMiniKQL::TValueHasher,
        NKikimr::NMiniKQL::TValueEqual,
        NKikimr::NMiniKQL::TMKQLAllocator<std::pair<const NUdf::TUnboxedValue, NUdf::TUnboxedValue>>
    >;
    TTableData Data;
};

std::pair<NYql::NDq::IDqAsyncLookupSource*, NActors::IActor*> CreateYtLookupActor(
    NFile::TYtFileServices::TPtr ytServices,
    NActors::TActorId parentId,
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc,
    const NKikimr::NMiniKQL::IFunctionRegistry& functionRegistry,
    NYql::NYt::NSource::TLookupSource&& lookupSource,
    const NKikimr::NMiniKQL::TStructType* keyType,
    const NKikimr::NMiniKQL::TStructType* payloadType,
    const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv,
    const NKikimr::NMiniKQL::THolderFactory& holderFactory,
    const size_t maxKeysInRequest)
{
    const auto actor = new TYtLookupActor(
        ytServices,
        parentId,
        alloc,
        functionRegistry,
        typeEnv,
        std::move(lookupSource),
        keyType,
        payloadType,
        holderFactory,
        maxKeysInRequest);
    return {actor, actor};
}

} // namespace NYql::NDq
