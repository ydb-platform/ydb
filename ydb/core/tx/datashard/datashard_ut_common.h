#pragma once

#include "datashard.h"
#include "datashard_impl.h"

#include <ydb/core/engine/mkql_engine_flat.h>
#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>
#include <ydb/core/protos/ydb_result_set_old.pb.h>
#include <ydb/core/testlib/minikql_compile.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <library/cpp/testing/unittest/registar.h>


namespace NKikimr {

class TBalanceCoverageBuilder;
class TEngineHolder;
class TFakeMiniKQLProxy;
class TFakeProxyTx;

constexpr ui64 FAKE_SCHEMESHARD_TABLET_ID = 4200;
constexpr ui64 FAKE_TX_ALLOCATOR_TABLET_ID = 4201;

///
class TTester : public TNonCopyable {
public:
    friend class TEngineHolder;
    friend class TFakeMiniKQLProxy;
    friend class TFakeProxyTx;
    friend class TFakeScanTx;

    using TKeyResolver = std::function<void(TKeyDesc&)>;

    enum ESchema {
        ESchema_KV,
        ESchema_DoubleKV,
        ESchema_MultiShardKV,
        ESchema_SpecialKV,
        ESchema_DoubleKVExternal,
    };

    ///
    struct TOptions {
        static constexpr const ui64 DEFAULT_FIRST_STEP = 1000000;

        ui64 FirstStep;
        ui32 NumActiveTx;
        bool SoftUpdates;
        bool DelayReadSet;
        bool DelayData;
        bool RebootOnDelay;
        std::optional<bool> Mvcc;
        ui64 ExecutorCacheSize;

        TOptions(ui64 firstStep = 0)
            : FirstStep(firstStep ? firstStep : DEFAULT_FIRST_STEP)
            , NumActiveTx(0)
            , SoftUpdates(false)
            , DelayReadSet(false)
            , DelayData(false)
            , RebootOnDelay(false)
            , ExecutorCacheSize(Max<ui64>())
        {}

        void EnableOutOfOrder(ui32 num = 8) { NumActiveTx = num; }
        void EnableSoftUpdates() { SoftUpdates = true; }
        void EnableMvcc(std::optional<bool> enabled = {true}) { Mvcc = enabled; }

        TString PartConfig() const {
            TString pipelineConfig = Sprintf(R"(PipelineConfig {
                    NumActiveTx: %u
                    EnableOutOfOrder: %u
                    EnableSoftUpdates: %u
                })", NumActiveTx, (NumActiveTx != 0), SoftUpdates);

            TString cacheSize;
            if (ExecutorCacheSize != Max<ui64>())
                cacheSize = Sprintf("ExecutorCacheSize: %lu", ExecutorCacheSize);

            return Sprintf(R"(PartitionConfig {
                %s
                %s
            })", cacheSize.data(), pipelineConfig.data());
        }
    };

    struct TActiveZone {
        TTester& Tester;
        TActiveZone(TTester& t): Tester(t) { if (Tester.ActiveZone) *Tester.ActiveZone = true; }
        ~TActiveZone() { if (Tester.ActiveZone) *Tester.ActiveZone = false; }
    };

    TTestBasicRuntime Runtime;

    TTester(ESchema schema, const TOptions& opts = TOptions());
    TTester(ESchema schema, const TString& dispatchName, std::function<void (TTestActorRuntime&)> setup,
            bool& activeZone, const TOptions& opts = TOptions());

    static void Setup(TTestActorRuntime& runtime, const TOptions& opts = TOptions());

private:
    ESchema Schema;
    TActorId Sender;
    ui64 LastTxId;
    ui64 LastStep;
    TMockDbSchemeResolver DbSchemeResolver;
    TString DispatchName = "NONE";
    bool AllowIncompleteResult = false;
    bool* ActiveZone = nullptr;
    TDuration Timeout = TDuration::Minutes(10);

    TKeyResolver GetKeyResolver() const;
    void CreateSchema(ESchema schema, const TOptions& opts);
    void CreateDataShard(TFakeMiniKQLProxy& proxy, ui64 tabletId, const TString& schemeText, bool withRegister = false);
    void RegisterTableInResolver(const TString& schemeText);

    static void EmptyShardKeyResolver(TKeyDesc& key);
    static void SingleShardKeyResolver(TKeyDesc& key); // uses TTestTxConfig::TxTablet0
    static void ThreeShardPointKeyResolver(TKeyDesc& key); // uses TTestTxConfig::TxTablet0,1,2
};

///
struct TExpectedReadSet {
    struct TWaitFor {
        ui64 Shard;
        ui64 TxId;
    };

    ui64 SrcTablet;
    ui64 DstTablet;
    ui64 TxId;
    TWaitFor Freedom;

    TExpectedReadSet(ui64 txId, TWaitFor freedom)
        : SrcTablet(TTestTxConfig::TxTablet1)
        , DstTablet(TTestTxConfig::TxTablet0)
        , TxId(txId)
        , Freedom(freedom)
    {}
};

///
class TEngineHolder {
public:
    TEngineHolder()
        : Alloc(__LOCATION__)
        , Env(Alloc)
    {
        Alloc.Release();
    }

    virtual ~TEngineHolder() {
        Engine.Reset();
        Alloc.Acquire();
    }

    NMiniKQL::TRuntimeNode ProgramText2Bin(TTester& tester, const TString& programText);

protected:
    NMiniKQL::TScopedAlloc Alloc;
    NMiniKQL::TTypeEnvironment Env;
    THolder<NMiniKQL::IEngineFlat> Engine;
};

///
class TFakeProxyTx : public TEngineHolder {
public:
    using IEngineFlat = NMiniKQL::IEngineFlat;
    using TPtr = std::shared_ptr<TFakeProxyTx>;

    TVector<ui64> Shards;
    TMap<ui64, TVector<NKikimrTxDataShard::TError>> Errors;
    ui64 MinStep = 0;
    ui64 MaxStep = Max<ui64>();

    TFakeProxyTx(ui64 txId, const TString& txBody, ui32 flags = NDataShard::TTxFlags::Default)
        : TxId_(txId)
        , TxKind_(NKikimrTxDataShard::TX_KIND_DATA)
        , TxBody_(txBody)
        , TxFlags_(flags)
        , ShardsCount_(0)
    {}

    /// @return shards count
    ui32 SetProgram(TTester& tester, const TString& programText);
    virtual ui32 SetProgram(TTester& tester);
    /// @return shardId
    virtual ui32 GetShardProgram(ui32 idx, TString& outTxBody);
    void AddProposeShardResult(ui32 shardId, const TEvDataShard::TEvProposeTransactionResult * event);
    virtual void AddPlanStepShardResult(ui32 shardId, const TEvDataShard::TEvProposeTransactionResult * event, bool complete);

    virtual IEngineFlat::EStatus GetStatus(bool atPropose);
    virtual NKikimrMiniKQL::TResult GetResult() const;

    ui64 TxId() const { return TxId_; }
    ui32 TxFlags() const { return TxFlags_; }
    TString TxBody() const { return TxBody_; }
    NKikimrTxDataShard::ETransactionKind TxKind() { return TxKind_; }
    bool IsDataTx() const { return TxKind_ == NKikimrTxDataShard::TX_KIND_DATA; }
    bool IsReadTable() const { return TxKind_ == NKikimrTxDataShard::TX_KIND_SCAN; }
    bool HasErrors() const { return !Errors.empty(); }
    bool Immediate() const { return IsDataTx() && (ShardsCount_ < 2) && !(TxFlags_ & NDataShard::TTxFlags::ForceOnline); }
    ui32 ShardsCount() const { return ShardsCount_; }

    void SetKindSchema() { TxKind_ = NKikimrTxDataShard::TX_KIND_SCHEME; }
    void SetKindScan() { TxKind_ = NKikimrTxDataShard::TX_KIND_SCAN; }

    TBalanceCoverageBuilder * GetCoverageBuilder(ui64 shard);

    void SetCheck(std::function<bool(TFakeProxyTx&)> check) {
        Check = check;
    }

    bool CheckResult() {
        if (Check)
            return Check(*this);
        return true;
    }

protected:
    ui64 TxId_;
    NKikimrTxDataShard::ETransactionKind TxKind_;
    TString TxBody_;
    ui32 TxFlags_;
    ui32 ShardsCount_;
    THashMap<ui64, std::shared_ptr<TBalanceCoverageBuilder>> CoverageBuilders; // key - shard
    std::function<bool(TFakeProxyTx&)> Check;
};

///
class TFakeScanTx : public TFakeProxyTx {
public:
    TFakeScanTx(ui64 txId, const TString& txBody, ui32 flags = NDataShard::TTxFlags::Default)
        : TFakeProxyTx(txId, txBody, flags)
        , Status(IEngineFlat::EStatus::Unknown)
    {
        SetKindScan();
    }

    ui32 SetProgram(TTester& tester) override;
    ui32 GetShardProgram(ui32 idx, TString& outTxBody) override;
    void AddPlanStepShardResult(ui32 shardId, const TEvDataShard::TEvProposeTransactionResult * event, bool complete) override;
    YdbOld::ResultSet GetScanResult() const;
    IEngineFlat::EStatus GetStatus(bool atPropose) override;

private:
    YdbOld::ResultSet Result;
    IEngineFlat::EStatus Status;
};

///
class TFakeMiniKQLProxy {
public:
    using IEngineFlat = NMiniKQL::IEngineFlat;
    //using TEvProgressTransaction = NDataShard::TDataShard::TEvPrivate::TEvProgressTransaction;

    TFakeMiniKQLProxy(TTester& tester)
        : Tester(tester)
        , LastTxId_(tester.LastTxId)
        , LastStep_(tester.LastStep)
        , RebootOnDelay(false)
    {}

    // Propose + Plan (if needed) in own step
    IEngineFlat::EStatus ExecSchemeCreateTable(const TString& schemaText, const TVector<ui64>& shards);
    IEngineFlat::EStatus Execute(const TString& programText, NKikimrMiniKQL::TResult& out,
                                 bool waitForResult = true);
    IEngineFlat::EStatus Execute(const TString& programText, bool waitForResult = true) {
        NKikimrMiniKQL::TResult result;
        return Execute(programText, result, waitForResult);
    }

    void CheckedExecute(const TString& programText) {
        try {
            UNIT_ASSERT_EQUAL(Execute(programText), IEngineFlat::EStatus::Complete);
        } catch (TEmptyEventQueueException&) {
            Cout << "Event queue is empty at dispatch " << Tester.DispatchName << "\n";
            if (!Tester.AllowIncompleteResult)
                throw;
        }
    }

    void Enqueue(const TString& programText, std::function<bool(TFakeProxyTx&)> check = DoNothing,
                 ui32 flags = NDataShard::TTxFlags::ForceOnline);
    void EnqueueScan(const TString& programText, std::function<bool(TFakeProxyTx&)> check = DoNothing,
                 ui32 flags = NDataShard::TTxFlags::ForceOnline);
    void ExecQueue();

    static bool DoNothing(TFakeProxyTx&) {
        return true;
    }

    ui64 LastTxId() const { return LastTxId_; }

    void DelayReadSet(const TExpectedReadSet& rs, bool withReboot = false) {
        DelayedReadSets.emplace_back(rs);
        RebootOnDelay = withReboot;
    }

    void DelayData(const TExpectedReadSet::TWaitFor& shardTx) {
        DelayedData.emplace_back(shardTx);
    }

private:
    TTester& Tester;
    ui64& LastTxId_;
    ui64& LastStep_;
    TVector<TFakeProxyTx::TPtr> TxQueue;
    TMap<ui64, TActorId> ShardActors;
    //
    TVector<TExpectedReadSet> DelayedReadSets;
    TVector<TExpectedReadSet::TWaitFor> DelayedData;
    bool RebootOnDelay;

    void Propose(TFakeProxyTx& tx, bool holdImmediate = false);
    void ProposeSchemeCreateTable(TFakeProxyTx& tx, const TVector<ui64>& shards);
    void ProposeScheme(TFakeProxyTx& tx, const TVector<ui64>& shards,
        const std::function<NKikimrTxDataShard::TFlatSchemeTransaction(ui64)>& txBodyForShard);
    ui64 Plan(ui64 stepId, const TMap<ui64, TFakeProxyTx::TPtr>& txs, bool waitForResult = true);
    void ResolveShards(const TSet<ui64>& shards);
};

///
class TDatashardInitialEventsFilter {
public:
    TDatashardInitialEventsFilter(const TVector<ui64>& tabletIds);
    TTestActorRuntime::TEventFilter Prepare();
    bool operator()(TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event);

    TDatashardInitialEventsFilter(const TDatashardInitialEventsFilter&) = delete;
    TDatashardInitialEventsFilter(TDatashardInitialEventsFilter&&) = default;

    static TDatashardInitialEventsFilter CreateDefault() {
        TVector<ui64> tabletIds;
        tabletIds.push_back((ui64)TTestTxConfig::TxTablet0);
        tabletIds.push_back((ui64)TTestTxConfig::TxTablet1);
        tabletIds.push_back((ui64)TTestTxConfig::TxTablet2);
        return TDatashardInitialEventsFilter(tabletIds);
    }

    const TVector<ui64> Tablets() const { return TabletIds; }

private:
    const TVector<ui64> TabletIds;
    TVector<ui64> RemainTablets;
};

///
class TKeyExtractor : public TEngineHolder {
public:
    using TPKey = THolder<TKeyDesc>;

    TKeyExtractor(TTester& tester, TString programText);

    const TVector<TPKey>& GetKeys() const { return Engine->GetDbKeys(); }
};

THolder<NKqp::TEvKqp::TEvQueryRequest> MakeSQLRequest(const TString &sql,
                                                      bool dml = true);

void InitRoot(Tests::TServer::TPtr server,
              TActorId sender);

class TLambdaActor : public IActorCallback {
public:
    using TCallback = std::function<void(TAutoPtr<IEventHandle>&, const TActorContext&)>;
    using TNoCtxCallback = std::function<void(TAutoPtr<IEventHandle>&)>;

public:
    TLambdaActor(TCallback&& callback)
        : IActorCallback(static_cast<TReceiveFunc>(&TLambdaActor::StateWork))
        , Callback(std::move(callback))
    { }

    TLambdaActor(TNoCtxCallback&& callback)
        : TLambdaActor([callback = std::move(callback)](auto& ev, auto&) {
            callback(ev);
        })
    { }

private:
    STFUNC(StateWork) {
        Callback(ev, this->ActorContext());
    }

private:
    TCallback Callback;
};

enum class EShadowDataMode {
    Default,
    Enabled,
};

enum class EReplicationConsistency: int {
    Strong = 1,
    Weak = 2,
};

struct TShardedTableOptions {
    using TSelf = TShardedTableOptions;

    struct TColumn {
        TColumn(const TString& name, const TString& type, bool isKey, bool notNull)
            : Name(name)
            , Type(type)
            , IsKey(isKey)
            , NotNull(notNull) {}

        TString Name;
        TString Type;
        bool IsKey;
        bool NotNull;
    };

    struct TIndex {
        using EType = NKikimrSchemeOp::EIndexType;

        TString Name;
        TVector<TString> IndexColumns;
        TVector<TString> DataColumns = {};
        EType Type = EType::EIndexTypeGlobal;
    };

    struct TCdcStream {
        using EMode = NKikimrSchemeOp::ECdcStreamMode;
        using EFormat = NKikimrSchemeOp::ECdcStreamFormat;
        using EState = NKikimrSchemeOp::ECdcStreamState;

        TString Name;
        EMode Mode;
        EFormat Format;
        TMaybe<EState> InitialState;
        bool VirtualTimestamps = false;
    };

    using TAttributes = THashMap<TString, TString>;

#define TABLE_OPTION_IMPL(type, name, defaultValue) \
    TSelf& name(type value) {\
        name##_ = std::move(value); \
        return *this; \
    } \
    type name##_ = defaultValue

#define TABLE_OPTION(type, name, defaultValue) TABLE_OPTION_IMPL(type, name, defaultValue)

    TABLE_OPTION(ui64, Shards, 1);
    TABLE_OPTION(bool, EnableOutOfOrder, true);
    TABLE_OPTION(const NLocalDb::TCompactionPolicy*, Policy, nullptr);
    TABLE_OPTION(EShadowDataMode, ShadowData, EShadowDataMode::Default);
    TABLE_OPTION(TVector<TColumn>, Columns, (TVector<TColumn>{{"key", "Uint32", true, false}, {"value", "Uint32", false, false}}));
    TABLE_OPTION(TVector<TIndex>, Indexes, {});
    TABLE_OPTION(ui64, Followers, 0);
    TABLE_OPTION(bool, FollowerPromotion, false);
    TABLE_OPTION(bool, ExternalStorage, false);
    TABLE_OPTION(std::optional<ui64>, ExecutorCacheSize, std::nullopt);
    TABLE_OPTION(bool, Replicated, false);
    TABLE_OPTION(std::optional<EReplicationConsistency>, ReplicationConsistency, std::nullopt);
    TABLE_OPTION(TAttributes, Attributes, {});

#undef TABLE_OPTION
#undef TABLE_OPTION_IMPL
};

#define Y_UNIT_TEST_QUAD(N, OPT1, OPT2)                                                                                              \
    template<bool OPT1, bool OPT2> void N(NUnitTest::TTestContext&);                                                                 \
    struct TTestRegistration##N {                                                                                                    \
        TTestRegistration##N() {                                                                                                     \
            TCurrentTest::AddTest(#N "-" #OPT1 "-" #OPT2, static_cast<void (*)(NUnitTest::TTestContext&)>(&N<false, false>), false); \
            TCurrentTest::AddTest(#N "+" #OPT1 "-" #OPT2, static_cast<void (*)(NUnitTest::TTestContext&)>(&N<true, false>), false);  \
            TCurrentTest::AddTest(#N "-" #OPT1 "+" #OPT2, static_cast<void (*)(NUnitTest::TTestContext&)>(&N<false, true>), false);  \
            TCurrentTest::AddTest(#N "+" #OPT1 "+" #OPT2, static_cast<void (*)(NUnitTest::TTestContext&)>(&N<true, true>), false);   \
        }                                                                                                                            \
    };                                                                                                                               \
    static TTestRegistration##N testRegistration##N;                                                                                 \
    template<bool OPT1, bool OPT2>                                                                                                   \
    void N(NUnitTest::TTestContext&)

void CreateShardedTable(Tests::TServer::TPtr server,
                        TActorId sender,
                        const TString &root,
                        const TString &name,
                        const TShardedTableOptions &opts = TShardedTableOptions());

void CreateShardedTable(Tests::TServer::TPtr server,
                        TActorId sender,
                        const TString &root,
                        const TString &name,
                        ui64 shards,
                        bool enableOutOfOrder = true,
                        const NLocalDb::TCompactionPolicy* policy = nullptr,
                        EShadowDataMode shadowData = EShadowDataMode::Default);

ui64 AsyncCreateCopyTable(Tests::TServer::TPtr server,
                          TActorId sender,
                          const TString &root,
                          const TString &name,
                          const TString &from);

TVector<ui64> GetTableShards(Tests::TServer::TPtr server,
                             TActorId sender,
                             const TString &path);

NKikimrTxDataShard::TEvCompactTableResult CompactTable(
    TTestActorRuntime& runtime, ui64 shardId, const TTableId& tableId, bool compactBorrowed = false);

using TTableInfoMap = THashMap<TString, NKikimrTxDataShard::TEvGetInfoResponse::TUserTable>;

std::pair<TTableInfoMap, ui64> GetTables(Tests::TServer::TPtr server,
                                         ui64 tabletId);

TTableId ResolveTableId(
        Tests::TServer::TPtr server,
        TActorId sender,
        const TString& path);

NTable::TRowVersionRanges GetRemovedRowVersions(
        Tests::TServer::TPtr server,
        ui64 shardId);

void SendCreateVolatileSnapshot(
        TTestActorRuntime& runtime,
        const TActorId& sender,
        const TVector<TString>& tables,
        TDuration timeout = TDuration::Seconds(5));

TRowVersion GrabCreateVolatileSnapshotResult(
        TTestActorRuntime& runtime,
        const TActorId& sender);

TRowVersion CreateVolatileSnapshot(
        Tests::TServer::TPtr server,
        const TVector<TString>& tables,
        TDuration timeout = TDuration::Seconds(5));

bool RefreshVolatileSnapshot(
        Tests::TServer::TPtr server,
        const TVector<TString>& tables,
        TRowVersion snapshot);

bool DiscardVolatileSnapshot(
        Tests::TServer::TPtr server,
        const TVector<TString>& tables,
        TRowVersion snapshot);

struct TChange {
    i64 Offset;
    ui64 WriteTxId;
    ui32 Key;
    ui32 Value;
};

void ApplyChanges(
        const Tests::TServer::TPtr& server,
        ui64 shardId,
        const TTableId& tableId,
        const TString& sourceId,
        const TVector<TChange>& changes,
        NKikimrTxDataShard::TEvApplyReplicationChangesResult::EStatus expected =
            NKikimrTxDataShard::TEvApplyReplicationChangesResult::STATUS_OK);

TRowVersion CommitWrites(
        Tests::TServer::TPtr server,
        const TVector<TString>& tables,
        ui64 writeTxId);

ui64 AsyncDropTable(
        Tests::TServer::TPtr server,
        TActorId sender,
        const TString& workingDir,
        const TString& name);

ui64 AsyncSplitTable(
        Tests::TServer::TPtr server,
        TActorId sender,
        const TString& path,
        ui64 sourceTablet,
        ui32 splitKey);

ui64 AsyncMergeTable(
        Tests::TServer::TPtr server,
        TActorId sender,
        const TString& path,
        const TVector<ui64>& sourceTabletIds);

ui64 AsyncMoveTable(Tests::TServer::TPtr server,
        const TString& srcTable,
        const TString& dstTable);

ui64 AsyncAlterAddExtraColumn(
        Tests::TServer::TPtr server,
        const TString& workingDir,
        const TString& name);

ui64 AsyncAlterDropColumn(
        Tests::TServer::TPtr server,
        const TString& workingDir,
        const TString& name,
        const TString& colName);

ui64 AsyncAlterAndDisableShadow(
        Tests::TServer::TPtr server,
        const TString& workingDir,
        const TString& name,
        const NLocalDb::TCompactionPolicy* policy = nullptr);

ui64 AsyncAlterAddIndex(
        Tests::TServer::TPtr server,
        const TString& dbName,
        const TString& tablePath,
        const TShardedTableOptions::TIndex& indexDesc);

void CancelAddIndex(
        Tests::TServer::TPtr server,
        const TString& dbName,
        ui64 buildIndexId);

ui64 AsyncAlterDropIndex(
        Tests::TServer::TPtr server,
        const TString& workingDir,
        const TString& tableName,
        const TString& indexName);

ui64 AsyncAlterAddStream(
        Tests::TServer::TPtr server,
        const TString& workingDir,
        const TString& tableName,
        const TShardedTableOptions::TCdcStream& streamDesc);

ui64 AsyncAlterDisableStream(
        Tests::TServer::TPtr server,
        const TString& workingDir,
        const TString& tableName,
        const TString& streamName);

ui64 AsyncAlterDropStream(
        Tests::TServer::TPtr server,
        const TString& workingDir,
        const TString& tableName,
        const TString& streamName);

struct TReadShardedTableState {
    TActorId Sender;
    TActorId Worker;
    TString Result;
};

TReadShardedTableState StartReadShardedTable(
        Tests::TServer::TPtr server,
        const TString& path,
        TRowVersion snapshot = TRowVersion::Max(),
        bool pause = true,
        bool ordered = true);

void ResumeReadShardedTable(
        Tests::TServer::TPtr server,
        TReadShardedTableState& state);

TString ReadShardedTable(
        Tests::TServer::TPtr server,
        const TString& path,
        TRowVersion snapshot = TRowVersion::Max());

void WaitTxNotification(Tests::TServer::TPtr server, TActorId sender, ui64 txId);
void WaitTxNotification(Tests::TServer::TPtr server, ui64 txId);

void SimulateSleep(Tests::TServer::TPtr server, TDuration duration);
void SimulateSleep(TTestActorRuntime& runtime, TDuration duration);

THolder<NSchemeCache::TSchemeCacheNavigate> Navigate(
        TTestActorRuntime& runtime,
        const TActorId& sender,
        const TString& path,
        NSchemeCache::TSchemeCacheNavigate::EOp op = NSchemeCache::TSchemeCacheNavigate::EOp::OpTable);

THolder<NSchemeCache::TSchemeCacheNavigate> Ls(
        TTestActorRuntime& runtime,
        const TActorId& sender,
        const TString& path);

void SendSQL(Tests::TServer::TPtr server,
             TActorId sender,
             const TString &sql,
             bool dml = true);
void ExecSQL(Tests::TServer::TPtr server,
             TActorId sender,
             const TString &sql,
             bool dml = true,
             Ydb::StatusIds::StatusCode code = Ydb::StatusIds::SUCCESS);

struct IsTxResultComplete {
    bool operator()(IEventHandle& ev)
    {
        if (ev.GetTypeRewrite() == TEvDataShard::EvProposeTransactionResult) {
            auto status = ev.Get<TEvDataShard::TEvProposeTransactionResult>()->GetStatus();
            if (status == NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE)
                return true;
        }
        return false;
    }
};

void WaitTabletBecomesOffline(Tests::TServer::TPtr server, ui64 tabletId);

///
class TDisableDataShardLogBatching : public TNonCopyable {
public:
    TDisableDataShardLogBatching()
        : PrevValue(NDataShard::gAllowLogBatchingDefaultValue)
    {
        NDataShard::gAllowLogBatchingDefaultValue = false;
    }

    ~TDisableDataShardLogBatching() {
        NDataShard::gAllowLogBatchingDefaultValue = PrevValue;
    }

private:
    const bool PrevValue;
};

template<class TCondition>
void WaitFor(TTestActorRuntime& runtime, TCondition&& condition, const TString& description = "condition", size_t maxAttempts = 1) {
    for (size_t attempt = 0; attempt < maxAttempts; ++attempt) {
        if (condition()) {
            return;
        }
        Cerr << "... waiting for " << description << Endl;
        TDispatchOptions options;
        options.CustomFinalCondition = [&]() {
            return condition();
        };
        runtime.DispatchEvents(options);
    }
    UNIT_ASSERT_C(condition(), "... failed to wait for " << description);
}

}
