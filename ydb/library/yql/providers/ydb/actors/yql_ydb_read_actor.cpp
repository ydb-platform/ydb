#include "yql_ydb_read_actor.h"

#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/yql/providers/ydb/proto/range.pb.h>

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/hfunc.h>

#include <ydb/public/lib/experimental/ydb_clickhouse_internal.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <util/generic/size_literals.h>

#include <queue>

namespace NYql::NDq {

using namespace NActors;

namespace {

struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(TEvents::ES_PRIVATE),

        EvScanResult = EvBegin,
        EvRetryTime,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

    // Events
    struct TEvScanResult : public TEventLocal<TEvScanResult, EvScanResult>, public ::NYdb::NClickhouseInternal::TScanResult {
        TEvScanResult(::NYdb::NClickhouseInternal::TScanResult&& result): TScanResult(std::move(result)) {}
    };

    struct TEvRetryTime : public TEventLocal<TEvRetryTime, EvRetryTime> {};
};

bool IsRetriable(::NYdb::EStatus status) {
    switch (status) {
    case ::NYdb::EStatus::BAD_REQUEST:
    case ::NYdb::EStatus::SCHEME_ERROR:
    case ::NYdb::EStatus::UNAUTHORIZED:
    case ::NYdb::EStatus::NOT_FOUND:
        return false;
    default:
        return true;
    }
}

bool RangeFinished(const TString& lastReadKey, const TString& endKey, const TVector<NKikimr::NScheme::TTypeInfo>& keyColumnTypes) {
    if (lastReadKey.empty())
        return true;

    if (endKey.empty())
        return false;

    const NKikimr::TSerializedCellVec last(lastReadKey), end(endKey);
    return NKikimr::CompareTypedCellVectors(last.GetCells().data(), end.GetCells().data(), keyColumnTypes.data(), last.GetCells().size(), end.GetCells().size()) >= 0;
}

} // namespace

class TYdbReadActor : public TActorBootstrapped<TYdbReadActor>, public IDqComputeActorAsyncInput {
public:
    TYdbReadActor(
        ui64 inputIndex,
        TCollectStatsLevel statsLevel,
        const TString& database,
        const TString& endpoint,
        std::shared_ptr<::NYdb::ICredentialsProviderFactory> credentialsProviderFactory,
        bool secure,
        const TString& path,
        ::NYdb::TDriver driver,
        const NActors::TActorId& computeActorId,
        const TVector<TString>& columns, const TVector<NKikimr::NScheme::TTypeInfo>& keyColumnTypes,
        ui64 maxRowsInRequest, ui64 maxBytesInRequest, const TString& keyFrom, const TString& keyTo
    )
        : InputIndex(inputIndex)
        , ComputeActorId(computeActorId)
        , ActorSystem(TActivationContext::ActorSystem())
        , Path(path), Columns(columns), KeyColumnTypes(keyColumnTypes)
        , MaxRows(maxRowsInRequest), MaxBytes(maxBytesInRequest)
        , EndKey(keyTo)
        , Connection(driver, ::NYdb::TCommonClientSettings().Database(database).DiscoveryEndpoint(endpoint).CredentialsProviderFactory(credentialsProviderFactory).DiscoveryMode(::NYdb::EDiscoveryMode::Async).SslCredentials(::NYdb::TSslCredentials(secure)))
        , LastReadKey(keyFrom.empty() ? NKikimr::TSerializedCellVec::Serialize(TVector<NKikimr::TCell>(KeyColumnTypes.size())) : keyFrom)
        , LastReadKeyInclusive(false)
        , Retried(0U)
        , WakeUpTime(TMonotonic::Now())
        , RequestSent(false)
        , RequestsDone(!EndKey.empty() && RangeFinished(LastReadKey, EndKey, KeyColumnTypes))
        , MemoryUsed(0U)
    {
        IngressStats.Level = statsLevel;
    }

    void Bootstrap() {
        Become(&TYdbReadActor::StateFunc);
        SendRequest();
    }

    static constexpr char ActorName[] = "YQL_YDB_READ_ACTOR";

private:
    void SaveState(const NDqProto::TCheckpoint&, TSourceState&) final {}
    void LoadState(const TSourceState&) final {}
    void CommitState(const NDqProto::TCheckpoint&) final {}
    
    ui64 GetInputIndex() const final {
        return InputIndex;
    }

    const TDqAsyncStats& GetIngressStats() const final {
        return IngressStats;
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvScanResult, Handle);
        hFunc(TEvPrivate::TEvRetryTime, Handle);
    )

    void Handle(TEvPrivate::TEvRetryTime::TPtr&) {
        SendRequest();
    }

    // IActor & IDqComputeActorAsyncInput
    void PassAway() override { // Is called from Compute Actor
        RequestsDone = true;
        TActorBootstrapped<TYdbReadActor>::PassAway();
    }

    i64 GetAsyncInputData(NKikimr::NMiniKQL::TUnboxedValueBatch& buffer, TMaybe<TInstant>&, bool& finished, i64 freeSpace) final {
        i64 total = 0LL;
        if (!Blocks.empty()) {
            do {
                const auto size = Blocks.front().size();
                buffer.emplace_back(NKikimr::NMiniKQL::MakeString(Blocks.front()));
                Blocks.pop();
                total += size;
                freeSpace -= size;
                MemoryUsed -= size;
            } while (!Blocks.empty() && freeSpace > 0LL);
        }

        if (RequestsDone && Blocks.empty()) {
            finished = true;
        }

        SendRequest();
        return total;
    }

    void SendRequest() {
        if (!RequestsDone && !RequestSent && MemoryUsed < MaxQueueVolume && WakeUpTime <= TMonotonic::Now()) {
            RequestSent = true;
            Connection.Scan(Path, Columns, MaxRows, MaxBytes, LastReadKey, !LastReadKeyInclusive, Settings).Subscribe(std::bind(&TYdbReadActor::OnRespond, ActorSystem, SelfId(), std::placeholders::_1));
        }
    }

    static void OnRespond(TActorSystem* ass, const TActorId& selfId, const ::NYdb::NClickhouseInternal::TAsyncScanResult& result) {
        ass->Send(new IEventHandle(selfId, TActorId(), new TEvPrivate::TEvScanResult(const_cast<::NYdb::NClickhouseInternal::TAsyncScanResult&>(result).ExtractValueSync())));
    }

    void Handle(TEvPrivate::TEvScanResult::TPtr& result) {
        if (const auto& res = *result->Get(); res.GetStatus() == ::NYdb::EStatus::SUCCESS)
            ProcessResult(res);
        else
            ProcessError(res);
    }

    void ProcessResult(const ::NYdb::NClickhouseInternal::TScanResult& res) {
        RequestSent = false;
        Retried = 0U;

        const auto bc = res.GetBuffersCount();
        const bool notify = Blocks.empty();
        for (size_t i = 0U; i < bc; ++i) {
            if (auto block = res.GetBuffer(i); !block.empty()) {
                MemoryUsed += block.size();
                Blocks.emplace(std::move(block));
            }
        }

        std::tie(LastReadKey, LastReadKeyInclusive) = res.GetLastKey();
        RequestsDone = res.IsEos() || RangeFinished(LastReadKey, EndKey, KeyColumnTypes);
        SendRequest();
        if (notify)
            Send(ComputeActorId, new TEvNewAsyncInputDataArrived(InputIndex));
    }

    void ProcessError(const ::NYdb::NClickhouseInternal::TScanResult& res) {
        RequestSent = false;
        if (!IsRetriable(res.GetStatus()) || Retried > MaxRetries) {
            RequestsDone = true;
            while(!Blocks.empty())
                Blocks.pop();
            Send(ComputeActorId, new TEvAsyncInputError(InputIndex, res.GetIssues(), NYql::NDqProto::StatusIds::EXTERNAL_ERROR));
        } else {
            WakeUpTime = TMonotonic::Now() + Min(TDuration::Seconds(3), TDuration::MilliSeconds(0x30U * (1U << ++Retried)));
            ActorSystem->Schedule(WakeUpTime, new IEventHandle(SelfId(), TActorId(), new TEvPrivate::TEvRetryTime));
        }
    }

    static constexpr auto MaxRetries = 0x10U;
    static constexpr auto MaxQueueVolume = 4_MB;

    const ui64 InputIndex;
    TDqAsyncStats IngressStats;
    const NActors::TActorId ComputeActorId;

    TActorSystem* const ActorSystem;

    const TString Path;
    const TVector<TString> Columns;
    const TVector<NKikimr::NScheme::TTypeInfo> KeyColumnTypes;
    const ui64 MaxRows;
    const ui64 MaxBytes;
    const TString EndKey;
    const ::NYdb::NClickhouseInternal::TScanSettings Settings;

    ::NYdb::NClickhouseInternal::TScanClient Connection;

    TString LastReadKey;
    bool LastReadKeyInclusive;
    size_t Retried;

    TMonotonic WakeUpTime;
    bool RequestSent;
    bool RequestsDone;
    size_t MemoryUsed;

    std::queue<TString> Blocks;
};

std::pair<NYql::NDq::IDqComputeActorAsyncInput*, IActor*> CreateYdbReadActor(
    NYql::NYdb::TSource&& params,
    ui64 inputIndex,
    TCollectStatsLevel statsLevel,
    const THashMap<TString, TString>& secureParams,
    const THashMap<TString, TString>& taskParams,
    const NActors::TActorId& computeActorId,
    ::NYdb::TDriver driver,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory)
{
    TString keyFrom, keyTo;
    if (const auto taskParamsIt = taskParams.find("ydb"); taskParamsIt != taskParams.cend()) {
        NYql::NYdb::TKeyRange range;
        TStringInput input(taskParamsIt->second);
        range.Load(&input);
        keyFrom = range.Getfrom_key();
        keyTo = range.Getto_key();
    }

    auto token = secureParams.Value(params.GetToken(), TString{});
    auto credentialsProviderFactory = CreateCredentialsProviderFactoryForStructuredToken(credentialsFactory, token, params.GetAddBearerToToken());
    TVector<TString> columns;
    columns.reserve(params.GetColumns().size());
    for (auto i = 0; i < params.GetColumns().size(); ++i)
        columns.emplace_back(params.GetColumns().Get(i));

    TVector<NKikimr::NScheme::TTypeInfo> keyColumnTypes;
    keyColumnTypes.reserve(params.GetKeyColumnTypes().size());
    for (auto i = 0; i < params.GetKeyColumnTypes().size(); ++i)
        // TODO support pg types
        keyColumnTypes.emplace_back(NKikimr::NScheme::TTypeInfo(params.GetKeyColumnTypes().Get(i), nullptr));

    ui64 maxRowsInRequest = 0ULL;
    ui64 maxBytesInRequest = 0ULL;
    const auto actor = new TYdbReadActor(inputIndex, statsLevel, params.GetDatabase(), params.GetEndpoint(), credentialsProviderFactory, params.GetSecure(), params.GetTable(), std::move(driver), computeActorId, columns, keyColumnTypes, maxRowsInRequest, maxBytesInRequest, keyFrom, keyTo);
    return {actor, actor};
}

} // namespace NYql::NDq
