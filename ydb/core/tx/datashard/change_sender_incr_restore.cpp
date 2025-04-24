#include "change_exchange.h"
#include "change_exchange_impl.h"
#include "change_record.h"
#include "change_sender_table_base.h"
#include "datashard_impl.h"
#include "incr_restore_scan.h"

#include <ydb/core/change_exchange/change_sender.h>
#include <ydb/core/tablet_flat/tablet_flat_executor.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <util/generic/maybe.h>

namespace NKikimr::NDataShard {

class TIncrRestoreChangeSenderMain
    : public TActorBootstrapped<TIncrRestoreChangeSenderMain>
    , public NChangeExchange::TChangeSender
    , public NChangeExchange::IChangeSenderIdentity
    , public NChangeExchange::IChangeSenderPathResolver
    , public NChangeExchange::IChangeSenderFactory
    , private TSchemeChecksMixin<TIncrRestoreChangeSenderMain>
    , private TResolveUserTableState<TIncrRestoreChangeSenderMain>
    , private TResolveTargetTableState<TIncrRestoreChangeSenderMain>
    , private TResolveKeysState<TIncrRestoreChangeSenderMain>
{
    friend struct TSchemeChecksMixin;

    USE_STATE(ResolveUserTable);
    USE_STATE(ResolveTargetTable);
    USE_STATE(ResolveKeys);

    TStringBuf GetLogPrefix() const {
        if (!LogPrefix) {
            LogPrefix = TStringBuilder()
                << "[IncrRestoreChangeSenderMain]"
                << "[" << GetChangeSenderIdentity() << "]" // maybe better add something else
                << SelfId() /* contains brackets */ << " ";
        }

        return LogPrefix.GetRef();
    }

    static TSerializedTableRange GetFullRange(ui32 keyColumnsCount) {
        TVector<TCell> fromValues(keyColumnsCount);
        TVector<TCell> toValues;
        return TSerializedTableRange(fromValues, true, toValues, false);
    }

    bool IsResolving() const override {
        return IsResolveUserTableState()
            || IsResolveTargetTableState()
            || IsResolveKeysState();
    }

    TStringBuf CurrentStateName() const {
        if (IsResolveUserTableState()) {
            return "ResolveUserTable";
        } else if (IsResolveTargetTableState()) {
            return "ResolveTargetTable";
        } else if (IsResolveKeysState()) {
            return "ResolveKeys";
        } else {
            return "";
        }
    }

    void OnRetry(TResolveTargetTableState::TStateTag) {
        ResolveTargetTable();
    }

    void OnRetry(TResolveKeysState::TStateTag) {
        ResolveTargetTable();
    }

    void NextState(TResolveUserTableState::TStateTag) {
        Y_ENSURE(MainColumnToTag.contains("__ydb_incrBackupImpl_deleted"));
        ResolveTargetTable();
    }

    void NextState(TResolveTargetTableState::TStateTag) {
        ResolveKeys();
    }

    void NextState(TResolveKeysState::TStateTag) {
        if (!FirstServe) {
            FirstServe = true;
            Send(ChangeServer, new TEvIncrementalRestoreScan::TEvServe());
        }
        Serve();
    }

    void Retry() {
        Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup());
    }

    void Serve() {
        Become(&TThis::StateMain);
    }

    void LogCritAndRetry(const TString& error) {
        LOG_C(error);
        Retry();
    }

    void LogWarnAndRetry(const TString& error) {
        LOG_W(error);
        Retry();
    }

    /// Main

    STATEFN(StateMain) {
        return StateBase(ev);
    }

    void Resolve() override {
        ResolveTargetTable();
    }

    bool IsResolved() const override {
        return KeyDesc && KeyDesc->GetPartitions();
    }

    IActor* CreateSender(ui64 partitionId) const override {
        return CreateTableChangeSenderShard(
            SelfId(),
            DataShard,
            partitionId,
            TargetTablePathId,
            TagMap,
            ETableChangeSenderType::IncrementalRestore);
    }

    void Handle(NChangeExchange::TEvChangeExchange::TEvEnqueueRecords::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());
        EnqueueRecords(std::move(ev->Get()->Records));
    }

    void Handle(NChangeExchange::TEvChangeExchange::TEvRecords::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());
        ProcessRecords(std::move(ev->Get()->Records));
    }

    void Handle(NChangeExchange::TEvChangeExchange::TEvForgetRecords::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());
        ForgetRecords(std::move(ev->Get()->Records));
    }

    void Handle(NChangeExchange::TEvChangeExchangePrivate::TEvReady::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());
        OnReady(ev->Get()->PartitionId);

        if (NoMoreData && IsAllSendersReadyOrUninit()) {
            Send(ChangeServer, new TEvIncrementalRestoreScan::TEvFinished());
        }
    }

    void Handle(NChangeExchange::TEvChangeExchangePrivate::TEvGone::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());
        OnGone(ev->Get()->PartitionId);
    }

    void Handle(TEvChangeExchange::TEvRemoveSender::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());
        Y_ENSURE(ev->Get()->PathId == GetChangeSenderIdentity());

        RemoveRecords();
        PassAway();
    }

    void Handle(TEvIncrementalRestoreScan::TEvNoMoreData::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());
        NoMoreData = true;

        if (IsAllSendersReadyOrUninit()) {
            Send(ChangeServer, new TEvIncrementalRestoreScan::TEvFinished());
        }
    }

    void PassAway() override {
        KillSenders();
        TActorBootstrapped::PassAway();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::CHANGE_SENDER_INCR_RESTORE_ACTOR_MAIN;
    }

    explicit TIncrRestoreChangeSenderMain(const TActorId& changeServerActor, const TDataShardId& dataShard, const TTableId& userTableId, const TPathId& targetPathId)
        : TActorBootstrapped()
        , TChangeSender(this, this, this, this, changeServerActor)
        , DataShard(dataShard)
        , UserTableId(userTableId)
        , TargetTablePathId(targetPathId)
        , TargetTableVersion(0)
    {
    }

    void Bootstrap() {
        ResolveUserTable();
    }

    STFUNC(StateBase) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvIncrementalRestoreScan::TEvNoMoreData, Handle);
            hFunc(NChangeExchange::TEvChangeExchange::TEvEnqueueRecords, Handle);
            hFunc(NChangeExchange::TEvChangeExchange::TEvRecords, Handle);
            hFunc(NChangeExchange::TEvChangeExchange::TEvForgetRecords, Handle);
            hFunc(TEvChangeExchange::TEvRemoveSender, Handle);
            hFunc(NChangeExchange::TEvChangeExchangePrivate::TEvReady, Handle);
            hFunc(NChangeExchange::TEvChangeExchangePrivate::TEvGone, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

    TPathId GetChangeSenderIdentity() const override final {
        return TargetTablePathId;
    }

private:
    const TDataShardId DataShard;
    const TTableId UserTableId;
    mutable TMaybe<TString> LogPrefix;

    THashMap<TString, TTag> MainColumnToTag;
    TMap<TTag, TTag> TagMap; // from incrBackupTable to targetTable

    TPathId TargetTablePathId;
    ui64 TargetTableVersion;
    THolder<TKeyDesc> KeyDesc;
    bool NoMoreData = false;
    bool FirstServe = false;
}; // TIncrRestoreChangeSenderMain

IActor* CreateIncrRestoreChangeSender(const TActorId& changeServerActor, const TDataShardId& dataShard, const TTableId& userTableId, const TPathId& restoreTargetPathId) {
    return new TIncrRestoreChangeSenderMain(changeServerActor, dataShard, userTableId, restoreTargetPathId);
}

} // namespace NKikimr::NDataShard
