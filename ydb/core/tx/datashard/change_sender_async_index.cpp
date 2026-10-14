#include "change_exchange.h"
#include "change_exchange_impl.h"
#include "change_record.h"
#include "change_sender_table_base.h"
#include "datashard_impl.h"

#include <ydb/core/change_exchange/change_sender.h>
#include <ydb/core/tablet_flat/flat_row_eggs.h>
#include <ydb/core/tx/scheme_cache/helpers.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>
#include <yql/essentials/public/udf/udf_data_type.h>

#include <util/generic/maybe.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CHANGE_EXCHANGE

namespace NKikimr::NDataShard {

using ESenderType = TEvChangeExchange::ESenderType;

template <typename TDerived>
class TResolveIndexState
    : virtual public NSchemeCache::TSchemeCacheHelpers
{
    DEFINE_STATE_INTRO;

public:
    void ResolveIndex() {
        auto request = MakeHolder<TNavigate>();
        request->ResultSet.emplace_back(MakeNavigateEntry(AsDerived()->IndexPathId, TNavigate::OpList));

        AsDerived()->Send(MakeSchemeCacheID(), new TEvNavigate(request.Release()));
        AsDerived()->Become(&TDerived::StateResolveIndex);
    }

    STATEFN(State) {
        YDB_LOG_CREATE_CONTEXT(GetLogPrefix(),
            {"actorState", "State"});
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            sFunc(TEvents::TEvWakeup, ResolveIndex);
        default:
            return AsDerived()->StateBase(ev);
        }
    }

private:
    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& result = ev->Get()->Request;

        YDB_LOG_DEBUG("HandleIndex TEvTxProxySchemeCache::TEvNavigateKeySetResult",
            {"navigateResult", (result ? result->ToString(*AppData()->TypeRegistry) : "nullptr")});

        if (!AsDerived()->CheckNotEmpty(result)) {
            return;
        }

        if (!AsDerived()->CheckEntriesCount(result, 1)) {
            return;
        }

        const auto& entry = result->ResultSet.at(0);

        if (!AsDerived()->CheckTableId(entry, AsDerived()->IndexPathId)) {
            return;
        }

        if (!AsDerived()->CheckEntrySucceeded(entry)) {
            return;
        }

        if (!AsDerived()->CheckEntryKind(entry, TNavigate::KindIndex)) {
            return;
        }

        if (entry.Self && entry.Self->Info.GetPathState() == NKikimrSchemeOp::EPathStateDrop) {
            YDB_LOG_DEBUG("Async index is planned to drop, waiting for remove sender command");

            return AsDerived()->OnIndexUnderRemove();
        }

        Y_ENSURE(entry.ListNodeEntry->Children.size() == 1);
        const auto& indexTable = entry.ListNodeEntry->Children.at(0);

        Y_ENSURE(indexTable.Kind == TNavigate::KindTable);
        AsDerived()->TargetTablePathId = indexTable.PathId;

        AsDerived()->NextState(TStateTag{});
    }
};

class TAsyncIndexChangeSenderMain
    : public TActorBootstrapped<TAsyncIndexChangeSenderMain>
    , public NChangeExchange::TChangeSender
    , public NChangeExchange::IChangeSenderIdentity
    , public NChangeExchange::IChangeSenderPathResolver
    , public NChangeExchange::IChangeSenderFactory
    , private TSchemeChecksMixin<TAsyncIndexChangeSenderMain>
    , private TResolveUserTableState<TAsyncIndexChangeSenderMain>
    , private TResolveIndexState<TAsyncIndexChangeSenderMain>
    , private TResolveTargetTableState<TAsyncIndexChangeSenderMain>
    , private TResolveKeysState<TAsyncIndexChangeSenderMain>
{
    friend struct TSchemeChecksMixin;

    USE_STATE(ResolveUserTable);
    USE_STATE(ResolveIndex);
    USE_STATE(ResolveTargetTable);
    USE_STATE(ResolveKeys);

    NActors::NStructuredLog::TStructuredMessage GetLogPrefix() const {
        return YDB_LOG_CREATE_MESSAGE(
            {"actorClassName", "AsyncIndexChangeSenderMain"},
            {"selfId", SelfId()},
            {"tabletId", DataShard.TabletId},
            {"generation", DataShard.Generation});
    }

    static TSerializedTableRange GetFullRange(ui32 keyColumnsCount) {
        TVector<TCell> fromValues(keyColumnsCount);
        TVector<TCell> toValues;
        return TSerializedTableRange(fromValues, true, toValues, false);
    }

    bool IsResolving() const override {
        return IsResolveUserTableState()
            || IsResolveIndexState()
            || IsResolveTargetTableState()
            || IsResolveKeysState();
    }

    TStringBuf CurrentStateName() const {
        if (IsResolveUserTableState()) {
            return "ResolveUserTable";
        } else if (IsResolveIndexState()) {
            return "ResolveIndex";
        } else if (IsResolveTargetTableState()) {
            return "ResolveTargetTable";
        } else if (IsResolveKeysState()) {
            return "ResolveKeys";
        } else {
            return "";
        }
    }

    void OnRetry(TResolveTargetTableState::TStateTag) {
        ResolveIndex();
    }

    void OnRetry(TResolveKeysState::TStateTag) {
        ResolveIndex();
    }

    void OnIndexUnderRemove() {
        RemoveRecords();
        KillSenders();

        Become(&TThis::StatePendingRemove);
    }

    void NextState(TResolveUserTableState::TStateTag) {
        ResolveIndex();
    }

    void NextState(TResolveIndexState::TStateTag) {
        ResolveTargetTable();
    }

    void NextState(TResolveTargetTableState::TStateTag) {
        ResolveKeys();
    }

    void NextState(TResolveKeysState::TStateTag) {
        Serve();
    }

    void Retry() {
        Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup());
    }

    void Serve() {
        Become(&TThis::StateMain);
    }

    void LogCritAndRetry(const TString& error) {
        YDB_LOG_CRIT("Critical error during change exchange operation, retrying",
            {"errorMessage", error});
        Retry();
    }

    void LogWarnAndRetry(const TString& error) {
        YDB_LOG_WARN("Recoverable error during change exchange operation, retrying",
            {"errorMessage", error});
        Retry();
    }

    /// Main

    STATEFN(StateMain) {
        YDB_LOG_CREATE_CONTEXT(GetLogPrefix(),
            {"actorState", "StateMain"});
        return StateBase(ev);
    }

    void Resolve() override {
        ResolveIndex();
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
            ETableChangeSenderType::AsyncIndex);
    }

    void Handle(NChangeExchange::TEvChangeExchange::TEvEnqueueRecords::TPtr& ev) {
        YDB_LOG_DEBUG("Handle",
            {"eventDetails", ev->Get()->ToString()});
        EnqueueRecords(std::move(ev->Get()->Records));
    }

    void Handle(NChangeExchange::TEvChangeExchange::TEvRecords::TPtr& ev) {
        YDB_LOG_DEBUG("Handle",
            {"eventDetails", ev->Get()->ToString()});
        ProcessRecords(std::move(ev->Get()->Records));
    }

    void Handle(NChangeExchange::TEvChangeExchange::TEvForgetRecords::TPtr& ev) {
        YDB_LOG_DEBUG("Handle",
            {"eventDetails", ev->Get()->ToString()});
        ForgetRecords(std::move(ev->Get()->Records));
    }

    void Handle(NChangeExchange::TEvChangeExchangePrivate::TEvReady::TPtr& ev) {
        YDB_LOG_DEBUG("Handle",
            {"eventDetails", ev->Get()->ToString()});
        OnReady(ev->Get()->PartitionId);
    }

    void Handle(NChangeExchange::TEvChangeExchangePrivate::TEvGone::TPtr& ev) {
        YDB_LOG_DEBUG("Handle",
            {"eventDetails", ev->Get()->ToString()});
        OnGone(ev->Get()->PartitionId);
    }

    void Handle(TEvChangeExchange::TEvRemoveSender::TPtr& ev) {
        YDB_LOG_DEBUG("Handle",
            {"eventDetails", ev->Get()->ToString()});
        Y_ENSURE(ev->Get()->PathId == GetChangeSenderIdentity());

        RemoveRecords();
        PassAway();
    }

    void AutoRemove(NChangeExchange::TEvChangeExchange::TEvEnqueueRecords::TPtr& ev) {
        YDB_LOG_DEBUG("Handle",
            {"eventDetails", ev->Get()->ToString()});
        RemoveRecords(std::move(ev->Get()->Records));
    }

    void Handle(NMon::TEvRemoteHttpInfo::TPtr& ev, const TActorContext& ctx) {
        RenderHtmlPage(DataShard.TabletId, ev, ctx);
    }

    void PassAway() override {
        KillSenders();
        TActorBootstrapped::PassAway();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::CHANGE_SENDER_ASYNC_INDEX_ACTOR_MAIN;
    }

    explicit TAsyncIndexChangeSenderMain(const TDataShardId& dataShard, const TTableId& userTableId, const TPathId& indexPathId)
        : TActorBootstrapped()
        , TChangeSender(this, this, this, this, dataShard.ActorId)
        , IndexPathId(indexPathId)
        , DataShard(dataShard)
        , UserTableId(userTableId)
        , TargetTableVersion(0)
    {
    }

    void Bootstrap() {
        YDB_LOG_CREATE_CONTEXT(GetLogPrefix());
        ResolveUserTable();
    }

    STFUNC(StateBase) {
        YDB_LOG_CREATE_CONTEXT(GetLogPrefix(),
            {"actorState", "StateBase"});
        switch (ev->GetTypeRewrite()) {
            hFunc(NChangeExchange::TEvChangeExchange::TEvEnqueueRecords, Handle);
            hFunc(NChangeExchange::TEvChangeExchange::TEvRecords, Handle);
            hFunc(NChangeExchange::TEvChangeExchange::TEvForgetRecords, Handle);
            hFunc(TEvChangeExchange::TEvRemoveSender, Handle);
            hFunc(NChangeExchange::TEvChangeExchangePrivate::TEvReady, Handle);
            hFunc(NChangeExchange::TEvChangeExchangePrivate::TEvGone, Handle);
            HFunc(NMon::TEvRemoteHttpInfo, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

    STFUNC(StatePendingRemove) {
        YDB_LOG_CREATE_CONTEXT(GetLogPrefix(),
            {"actorState", "StatePendingRemove"});
        switch (ev->GetTypeRewrite()) {
            hFunc(NChangeExchange::TEvChangeExchange::TEvEnqueueRecords, AutoRemove);
            hFunc(TEvChangeExchange::TEvRemoveSender, Handle);
            HFunc(NMon::TEvRemoteHttpInfo, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

    TPathId GetChangeSenderIdentity() const override final {
        return IndexPathId;
    }

private:
    const TPathId IndexPathId;
    const TDataShardId DataShard;
    const TTableId UserTableId;

    THashMap<TString, TTag> MainColumnToTag;
    TMap<TTag, TTag> TagMap; // from main to index

    TPathId TargetTablePathId;
    ui64 TargetTableVersion;
    THolder<TKeyDesc> KeyDesc;
}; // TAsyncIndexChangeSenderMain

IActor* CreateAsyncIndexChangeSender(const TDataShardId& dataShard, const TTableId& userTableId, const TPathId& indexPathId) {
    return new TAsyncIndexChangeSenderMain(dataShard, userTableId, indexPathId);
}

}
