#include "yql_kik_scan.h"

#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/library/yql/providers/common/structured_token/yql_token_builder.h>

#include <ydb/public/lib/experimental/ydb_clickhouse_internal.h>

#include <ydb/core/scheme/scheme_tablecell.h>

#include <ydb/library/actors/core/actor.h>

#include <queue>
#include <mutex>

namespace NYql::NDqs {
using namespace NKikimr::NMiniKQL;

namespace {

bool IsRetriable(NYdb::EStatus status) {
    switch (status) {
    case NYdb::EStatus::BAD_REQUEST:
    case NYdb::EStatus::SCHEME_ERROR:
    case NYdb::EStatus::UNAUTHORIZED:
    case NYdb::EStatus::NOT_FOUND:
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

template<bool Async>
class TKikScan : public TMutableComputationNode<TKikScan<Async>> {
using TBaseComputation = TMutableComputationNode<TKikScan<Async>>;
    class TStream: public TComputationValue<TStream> {
    public:
        TStream(TMemoryUsageInfo* memInfo, NYdb::NClickhouseInternal::TScanIterator iterator)
            : TComputationValue<TStream>(memInfo), Iterator_(std::move(iterator))
        {}
    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) final try {
            if (const auto blocks = Iterator_.GetBlocks(); !blocks.empty()) {
                result = MakeString(blocks);
                return NUdf::EFetchStatus::Ok;
            }

            return NUdf::EFetchStatus::Finish;
        } catch (const std::exception& ex) {
            UdfTerminate(ex.what());
        }

        NYdb::NClickhouseInternal::TScanIterator Iterator_;
    };

    class TAsyncStream: public TComputationValue<TAsyncStream> {
        class TAsyncState : public std::enable_shared_from_this<TAsyncState> {
        public:
            using TPtr = std::shared_ptr<TAsyncState>;
            using TWeakPtr = std::weak_ptr<TAsyncState>;

            static TPtr Make(const NYdb::TDriver& driver, const TString& database, const TString& endpoint, const TString& token, const TString& path, bool secure, const TVector<TString>& columns, const TVector<NKikimr::NScheme::TTypeInfo>& keyColumnTypes,
                ui64 maxRowsInRequest, ui64 maxBytesInRequest, const TString& keyFrom, const TString& keyTo, const NYdb::NClickhouseInternal::TScanSettings& settings) {
                const auto ptr = std::make_shared<TAsyncState>(driver, database, endpoint, token, path, secure, columns, keyColumnTypes, maxRowsInRequest, maxBytesInRequest, keyFrom, keyTo, settings);
                ptr->SendRequest();
                return ptr;
            }

            NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) {
                const std::unique_lock lock(Sync);
                if (!Blocks.empty()) {
                    const auto& block = Blocks.front();
                    MemoryUsed -= block.size();
                    result = MakeString(block);
                    Blocks.pop();
                    SendRequest();
                    return NUdf::EFetchStatus::Ok;
                }

                if (RequestsDone)
                    return NUdf::EFetchStatus::Finish;

                if (Issues)
                    UdfTerminate(Issues.ToString().c_str());

                SendRequest();
                return NUdf::EFetchStatus::Yield;
            }

            TAsyncState(const NYdb::TDriver& driver, const TString& database, const TString& endpoint, const TString& token, const TString& path, bool secure, const TVector<TString>& columns, const TVector<NKikimr::NScheme::TTypeInfo>& keyColumnTypes,
                ui64 maxRowsInRequest, ui64 maxBytesInRequest, const TString& keyFrom, const TString& keyTo, const NYdb::NClickhouseInternal::TScanSettings& settings)
                : ActorSystem(NActors::TActivationContext::ActorSystem())
                , CurrentActorId(NActors::TActivationContext::AsActorContext().SelfID)
                , Path(path)
                , Columns(columns)
                , KeyColumnTypes(keyColumnTypes)
                , MaxRows(maxRowsInRequest)
                , MaxBytes(maxBytesInRequest)
                , EndKey(keyTo)
                , Settings(settings)
                , Connection(driver, NYdb::TCommonClientSettings().Database(database).DiscoveryEndpoint(endpoint).AuthToken(token).DiscoveryMode(NYdb::EDiscoveryMode::Async).SslCredentials(NYdb::TSslCredentials(secure)))
                , LastReadKey(keyFrom.empty() ? NKikimr::TSerializedCellVec::Serialize(TVector<NKikimr::TCell>(KeyColumnTypes.size())) : keyFrom)
                , LastReadKeyInclusive(false)
                , Retried(0U)
                , WakeUpTime(NActors::TMonotonic::Now())
                , RequestSent(false)
                , RequestsDone(!EndKey.empty() && RangeFinished(LastReadKey, EndKey, KeyColumnTypes))
                , MemoryUsed(0U)
            {}
        private:
            void SendRequest() {
                if (!RequestsDone && !RequestSent && MemoryUsed < MaxQueueVolume && WakeUpTime <= NActors::TMonotonic::Now()) {
                    RequestSent = true;
                    Connection.Scan(Path, Columns, MaxRows, MaxBytes, LastReadKey, !LastReadKeyInclusive, Settings).Subscribe(std::bind(&TAsyncState::OnRespondWrap, this->weak_from_this(), std::placeholders::_1));
                }
            }

            static void OnRespondWrap(const TWeakPtr& weak, const NYdb::NClickhouseInternal::TAsyncScanResult& result) {
                if (const auto& self = weak.lock())
                    self->OnRespond(result);
            }

            void OnRespond(const NYdb::NClickhouseInternal::TAsyncScanResult& result) {
                if (const auto& res = result.GetValueSync(); res.GetStatus() == NYdb::EStatus::SUCCESS) {
                    ProcessResult(res);
                    ActorSystem->Send(new NActors::IEventHandle(CurrentActorId, NActors::TActorId(), new NDq::TEvDqCompute::TEvResumeExecution()));
                } else if (!IsRetriable(res.GetStatus()) || Retried > MaxRetries) {
                    ProcessError(res);
                    ActorSystem->Send(new NActors::IEventHandle(CurrentActorId, NActors::TActorId(), new NDq::TEvDqCompute::TEvResumeExecution()));
                } else {
                    ActorSystem->Schedule(ProcessRetry(), new NActors::IEventHandle(CurrentActorId, NActors::TActorId(), new NDq::TEvDqCompute::TEvResumeExecution()));
                }
            }

            void ProcessResult(const NYdb::NClickhouseInternal::TScanResult& res) {
                const std::unique_lock lock(Sync);
                RequestSent = false;

                Retried = 0U;

                const auto bc = res.GetBuffersCount();
                for (size_t i = 0U; i < bc; ++i) {
                    if (auto block = res.GetBuffer(i); !block.empty()) {
                        MemoryUsed += block.size();
                        Blocks.emplace(std::move(block));
                    }
                }

                std::tie(LastReadKey, LastReadKeyInclusive) = res.GetLastKey();
                RequestsDone = res.IsEos() || RangeFinished(LastReadKey, EndKey, KeyColumnTypes);
                SendRequest();
            }

            void ProcessError(const NYdb::NClickhouseInternal::TScanResult& res) {
                const std::unique_lock lock(Sync);
                RequestSent = false;
                Issues = res.GetIssues();
                while (!Blocks.empty())
                    Blocks.pop();
            }

            NActors::TMonotonic ProcessRetry() {
                const std::unique_lock lock(Sync);
                RequestSent = false;
                return WakeUpTime = NActors::TMonotonic::Now() + Min(TDuration::Seconds(3), TDuration::MilliSeconds(0x30U * (1U << ++Retried)));
            }

            static constexpr auto MaxRetries = 0x10U;
            static constexpr auto MaxQueueVolume = 4_MB;

            NActors::TActorSystem* const ActorSystem;
            NActors::TActorId const CurrentActorId;

            const TString Path;
            const TVector<TString> Columns;
            const TVector<NKikimr::NScheme::TTypeInfo> KeyColumnTypes;
            const ui64 MaxRows;
            const ui64 MaxBytes;
            const TString EndKey;
            const NYdb::NClickhouseInternal::TScanSettings Settings;

            NYdb::NClickhouseInternal::TScanClient Connection;

            TString LastReadKey;
            bool LastReadKeyInclusive;
            size_t Retried;

            NActors::TMonotonic WakeUpTime;
            bool RequestSent;
            bool RequestsDone;
            size_t MemoryUsed;

            std::mutex Sync;
            std::queue<TString> Blocks;
            TIssues Issues;
        };
    public:
        TAsyncStream(TMemoryUsageInfo* memInfo, const NYdb::TDriver& driver, const TString& database, const TString& endpoint, const TString& token, const TString& path, bool secure, const TVector<TString>& columns, const TVector<NKikimr::NScheme::TTypeInfo>& keyColumnTypes,
            ui64 maxRowsInRequest, ui64 maxBytesInRequest, const TString& keyFrom, const TString& keyTo, const NYdb::NClickhouseInternal::TScanSettings& settings)
            : TComputationValue<TAsyncStream>(memInfo), State(TAsyncState::Make(driver, database, endpoint, token, path, secure, columns, keyColumnTypes, maxRowsInRequest, maxBytesInRequest, keyFrom, keyTo, settings))
        {}
    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) final {
            return State->Fetch(result);
        }

        const typename TAsyncState::TPtr State;
    };
public:
    TKikScan(
        TComputationMutables& mutables,
        const NYdb::TDriver driver,
        const std::string_view& table,
        const std::string_view& database,
        const std::string_view& endpoint,
        const std::string_view& token,
        const std::string_view& snapshot,
        bool secure,
        TVector<TString>&& columns,
        TVector<NKikimr::NScheme::TTypeInfo>&& keyColumnTypes,
        const std::string_view& keyFrom,
        const std::string_view& keyTo,
        IComputationNode* rows
    ) : TBaseComputation(mutables)
        , Driver(driver)
        , Table(table), Database(database)
        , Endpoint(endpoint)
        , Token(token)
        , Snapshot(snapshot)
        , Secure(secure)
        , Columns(std::move(columns))
        , KeyColumnTypes(std::move(keyColumnTypes))
        , KeyFrom(keyFrom)
        , KeyTo(keyTo)
        , Rows(rows)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto token = Token;

        if (NUdf::TStringRef tokenRef(token); ctx.Builder->GetSecureParam(tokenRef, tokenRef)) {
            token = tokenRef;
        } else {
            UdfTerminate((TString("Unable to get token ") += Token).c_str());
        }

        auto parser = CreateStructuredTokenParser(token);
        if (!parser.HasIAMToken()) {
            UdfTerminate("Structured token does not contan YDB token");
        }

        token = parser.GetIAMToken();

        const auto settings = NYdb::NClickhouseInternal::TScanSettings().SnapshotId(Snapshot);
        const auto rows = Rows->GetValue(ctx).GetOrDefault<ui64>(0ULL);

        if constexpr (Async) {
            return ctx.HolderFactory.Create<TAsyncStream>(Driver, Database, Endpoint, token, Table, Secure, Columns, KeyColumnTypes, rows, 0ULL, KeyFrom, KeyTo, settings);
        } else {
            return ctx.HolderFactory.Create<TStream>(NYdb::NClickhouseInternal::TScanIterator(Driver, Database, Endpoint, token, Secure, Table, Columns, KeyColumnTypes, rows, 0ULL, KeyFrom, KeyTo, settings));
        }
    }
private:
    void RegisterDependencies() const final {
        this->DependsOn(Rows);
    }

    const NYdb::TDriver Driver;

    const TString Table;
    const TString Database;
    const TString Endpoint;
    const TString Token;
    const TString Snapshot;
    const bool Secure;
    const TVector<TString> Columns;
    const TVector<NKikimr::NScheme::TTypeInfo> KeyColumnTypes;
    const TString KeyFrom;
    const TString KeyTo;
    IComputationNode* const Rows;
};

}

template<bool Async>
IComputationNode* WrapKikScan(TCallable& callable, const TComputationNodeFactoryContext& ctx, const NYdb::TDriver& driver) {
    MKQL_ENSURE(callable.GetInputsCount() >= 10 && callable.GetInputsCount() <= 11, "Expected 10 or 11 arguments.");
    const auto table = AS_VALUE(TDataLiteral, callable.GetInput(0))->AsValue().AsStringRef();
    const auto database = AS_VALUE(TDataLiteral, callable.GetInput(1))->AsValue().AsStringRef();
    const auto endpoint = AS_VALUE(TDataLiteral, callable.GetInput(2))->AsValue().AsStringRef();
    const auto token = AS_VALUE(TDataLiteral, callable.GetInput(3))->AsValue().AsStringRef();
    const auto snapshot = AS_VALUE(TDataLiteral, callable.GetInput(4))->AsValue().AsStringRef();

    const auto columnsNode = AS_VALUE(TTupleLiteral, callable.GetInput(5));
    TVector<TString> columns;
    columns.reserve(columnsNode->GetValuesCount());
    for (ui32 i = 0; i < columnsNode->GetValuesCount(); ++i) {
        columns.emplace_back(AS_VALUE(TDataLiteral, columnsNode->GetValue(i))->AsValue().AsStringRef());
    }

    const auto keysNode = AS_VALUE(TTupleLiteral, callable.GetInput(6));
    TVector<NKikimr::NScheme::TTypeInfo> keyColumnTypes;
    keyColumnTypes.reserve(keysNode->GetValuesCount());
    for (ui32 i = 0; i < keysNode->GetValuesCount(); ++i) {
        keyColumnTypes.emplace_back(AS_VALUE(TDataLiteral, keysNode->GetValue(i))->AsValue().Get<ui16>());
    }

    const auto keyFrom = AS_VALUE(TDataLiteral, callable.GetInput(7))->AsValue().AsStringRef();
    const auto keyTo = AS_VALUE(TDataLiteral, callable.GetInput(8))->AsValue().AsStringRef();
    const auto rows = LocateNode(ctx.NodeLocator, callable, 9);
    const bool secure = callable.GetInputsCount() >= 11 && AS_VALUE(TDataLiteral, callable.GetInput(10))->AsValue().Get<bool>();
    return new TKikScan<Async>(ctx.Mutables, driver, table, database, endpoint, token, snapshot, secure, std::move(columns), std::move(keyColumnTypes), keyFrom, keyTo, rows);
}

template IComputationNode* WrapKikScan<true>(TCallable& callable, const TComputationNodeFactoryContext& ctx, const NYdb::TDriver& driver);
template IComputationNode* WrapKikScan<false>(TCallable& callable, const TComputationNodeFactoryContext& ctx, const NYdb::TDriver& driver);

} // NYql
