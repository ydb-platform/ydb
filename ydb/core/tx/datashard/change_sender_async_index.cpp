#include "change_exchange.h"
#include "change_exchange_impl.h"
#include "change_record.h"
#include "datashard_impl.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/change_exchange/change_sender_common_ops.h>
#include <ydb/core/change_exchange/change_sender_monitoring.h>
#include <ydb/core/tablet_flat/flat_row_eggs.h>
#include <ydb/core/tx/scheme_cache/helpers.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/yql/public/udf/udf_data_type.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <util/generic/maybe.h>

namespace NKikimr::NDataShard {

using namespace NTable;
using ESenderType = TEvChangeExchange::ESenderType;

class TAsyncIndexChangeSenderShard: public TActorBootstrapped<TAsyncIndexChangeSenderShard> {
    TStringBuf GetLogPrefix() const {
        if (!LogPrefix) {
            LogPrefix = TStringBuilder()
                << "[AsyncIndexChangeSenderShard]"
                << "[" << DataShard.TabletId << ":" << DataShard.Generation << "]"
                << "[" << ShardId << "]"
                << SelfId() /* contains brackets */ << " ";
        }

        return LogPrefix.GetRef();
    }

    /// GetProxyServices

    void GetProxyServices() {
        Send(MakeTxProxyID(), new TEvTxUserProxy::TEvGetProxyServicesRequest);
        Become(&TThis::StateGetProxyServices);
    }

    STATEFN(StateGetProxyServices) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxUserProxy::TEvGetProxyServicesResponse, Handle);
        default:
            return StateBase(ev);
        }
    }

    void Handle(TEvTxUserProxy::TEvGetProxyServicesResponse::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());

        LeaderPipeCache = ev->Get()->Services.LeaderPipeCache;
        Handshake();
    }

    /// Handshake

    void Handshake() {
        Send(DataShard.ActorId, new TDataShard::TEvPrivate::TEvConfirmReadonlyLease, 0, ++LeaseConfirmationCookie);
        Become(&TThis::StateHandshake);
    }

    STATEFN(StateHandshake) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TDataShard::TEvPrivate::TEvReadonlyLeaseConfirmation, Handle);
            hFunc(TEvChangeExchange::TEvStatus, Handshake);
        default:
            return StateBase(ev);
        }
    }

    void Handle(TDataShard::TEvPrivate::TEvReadonlyLeaseConfirmation::TPtr& ev) {
        if (ev->Cookie != LeaseConfirmationCookie) {
            LOG_W("Readonly lease confirmation cookie mismatch"
                << ": expected# " << LeaseConfirmationCookie
                << ", got# " << ev->Cookie);
            return;
        }

        auto handshake = MakeHolder<TEvChangeExchange::TEvHandshake>();
        handshake->Record.SetOrigin(DataShard.TabletId);
        handshake->Record.SetGeneration(DataShard.Generation);

        Send(LeaderPipeCache, new TEvPipeCache::TEvForward(handshake.Release(), ShardId, true));
    }

    void Handshake(TEvChangeExchange::TEvStatus::TPtr& ev) {
        LOG_D("Handshake " << ev->Get()->ToString());

        const auto& record = ev->Get()->Record;
        switch (record.GetStatus()) {
        case NKikimrChangeExchange::TEvStatus::STATUS_OK:
            LastRecordOrder = record.GetLastRecordOrder();
            return Ready();
        default:
            LOG_E("Handshake status"
                << ": status# " << static_cast<ui32>(record.GetStatus())
                << ", reason# " << static_cast<ui32>(record.GetReason()));
            return Leave();
        }
    }

    void Ready() {
        Send(Parent, new NChangeExchange::TEvChangeExchangePrivate::TEvReady(ShardId));
        Become(&TThis::StateWaitingRecords);
    }

    /// WaitingRecords

    STATEFN(StateWaitingRecords) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NChangeExchange::TEvChangeExchange::TEvRecords, Handle);
        default:
            return StateBase(ev);
        }
    }

    void Handle(NChangeExchange::TEvChangeExchange::TEvRecords::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());

        auto records = MakeHolder<TEvChangeExchange::TEvApplyRecords>();
        records->Record.SetOrigin(DataShard.TabletId);
        records->Record.SetGeneration(DataShard.Generation);

        auto& evRecords = std::get<std::shared_ptr<TChangeRecordContainer<NKikimr::NDataShard::TChangeRecord>>>(ev->Get()->Records)->Records;

        for (auto& recordPtr : evRecords) {
            const auto& record = *recordPtr;

            if (record.GetOrder() <= LastRecordOrder) {
                continue;
            }

            auto& proto = *records->Record.AddRecords();
            record.Serialize(proto);
            Adjust(proto);
        }

        if (!records->Record.RecordsSize()) {
            return Ready();
        }

        Send(LeaderPipeCache, new TEvPipeCache::TEvForward(records.Release(), ShardId, false));
        Become(&TThis::StateWaitingStatus);
    }

    void Adjust(NKikimrChangeExchange::TChangeRecord& record) const {
        record.SetPathOwnerId(IndexTablePathId.OwnerId);
        record.SetLocalPathId(IndexTablePathId.LocalPathId);

        Y_ABORT_UNLESS(record.HasAsyncIndex());
        AdjustTags(*record.MutableAsyncIndex());
    }

    void AdjustTags(NKikimrChangeExchange::TDataChange& record) const {
        AdjustTags(*record.MutableKey()->MutableTags());

        switch (record.GetRowOperationCase()) {
        case NKikimrChangeExchange::TDataChange::kUpsert:
            AdjustTags(*record.MutableUpsert()->MutableTags());
            break;
        case NKikimrChangeExchange::TDataChange::kReset:
            AdjustTags(*record.MutableReset()->MutableTags());
            break;
        default:
            break;
        }
    }

    void AdjustTags(google::protobuf::RepeatedField<ui32>& tags) const {
        for (int i = 0; i < tags.size(); ++i) {
            auto it = TagMap.find(tags[i]);
            Y_ABORT_UNLESS(it != TagMap.end());
            tags[i] = it->second;
        }
    }

    /// WaitingStatus

    STATEFN(StateWaitingStatus) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvChangeExchange::TEvStatus, Handle);
        default:
            return StateBase(ev);
        }
    }

    void Handle(TEvChangeExchange::TEvStatus::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());

        const auto& record = ev->Get()->Record;
        switch (record.GetStatus()) {
        case NKikimrChangeExchange::TEvStatus::STATUS_OK:
            LastRecordOrder = record.GetLastRecordOrder();
            return Ready();
        // TODO: REJECT?
        default:
            LOG_E("Apply status"
                << ": status# " << static_cast<ui32>(record.GetStatus())
                << ", reason# " << static_cast<ui32>(record.GetReason()));
            return Leave();
        }
    }

    bool CanRetry() const {
        if (CurrentStateFunc() != static_cast<TReceiveFunc>(&TThis::StateHandshake)) {
            return false;
        }

        return Attempt < MaxAttempts;
    }

    void Retry() {
        ++Attempt;
        Delay = Min(2 * Delay, MaxDelay);

        LOG_N("Retry"
            << ": attempt# " << Attempt
            << ", delay# " << Delay);

        const auto random = TDuration::FromValue(TAppData::RandomProvider->GenRand64() % Delay.MicroSeconds());
        Schedule(Delay + random, new TEvents::TEvWakeup());
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        if (ShardId != ev->Get()->TabletId) {
            return;
        }

        if (CanRetry()) {
            Unlink();
            Retry();
        } else {
            Leave();
        }
    }

    void Handle(NMon::TEvRemoteHttpInfo::TPtr& ev) {
        using namespace NChangeExchange;

        TStringStream html;

        HTML(html) {
            Header(html, "AsyncIndex partition change sender", DataShard.TabletId);

            SimplePanel(html, "Info", [this](IOutputStream& html) {
                HTML(html) {
                    DL_CLASS("dl-horizontal") {
                        TermDescLink(html, "ShardId", ShardId, TabletPath(ShardId));
                        TermDesc(html, "IndexTablePathId", IndexTablePathId);
                        TermDesc(html, "LeaderPipeCache", LeaderPipeCache);
                        TermDesc(html, "LastRecordOrder", LastRecordOrder);
                    }
                }
            });
        }

        Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes(html.Str()));
    }

    void Leave() {
        Send(Parent, new NChangeExchange::TEvChangeExchangePrivate::TEvGone(ShardId));
        PassAway();
    }

    void Unlink() {
        if (LeaderPipeCache) {
            Send(LeaderPipeCache, new TEvPipeCache::TEvUnlink(ShardId));
        }
    }

    void PassAway() override {
        Unlink();
        TActorBootstrapped::PassAway();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::CHANGE_SENDER_ASYNC_INDEX_ACTOR_PARTITION;
    }

    TAsyncIndexChangeSenderShard(const TActorId& parent, const TDataShardId& dataShard, ui64 shardId,
            const TPathId& indexTablePathId, const TMap<TTag, TTag>& tagMap)
        : Parent(parent)
        , DataShard(dataShard)
        , ShardId(shardId)
        , IndexTablePathId(indexTablePathId)
        , TagMap(tagMap)
        , LeaseConfirmationCookie(0)
        , LastRecordOrder(0)
    {
    }

    void Bootstrap() {
        GetProxyServices();
    }

    STATEFN(StateBase) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            hFunc(NMon::TEvRemoteHttpInfo, Handle);
            sFunc(TEvents::TEvWakeup, Handshake);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    const TActorId Parent;
    const TDataShardId DataShard;
    const ui64 ShardId;
    const TPathId IndexTablePathId;
    const TMap<TTag, TTag> TagMap; // from main to index
    mutable TMaybe<TString> LogPrefix;

    TActorId LeaderPipeCache;
    ui64 LeaseConfirmationCookie;
    ui64 LastRecordOrder;

    // Retry on delivery problem
    static constexpr ui32 MaxAttempts = 3;
    static constexpr auto MaxDelay = TDuration::MilliSeconds(50);
    ui32 Attempt = 0;
    TDuration Delay = TDuration::MilliSeconds(10);

}; // TAsyncIndexChangeSenderShard

class TAsyncIndexChangeSenderMain
    : public TActorBootstrapped<TAsyncIndexChangeSenderMain>
    , public NChangeExchange::TBaseChangeSender<TChangeRecord>
    , public NChangeExchange::IChangeSenderResolver
    , public NChangeExchange::ISenderFactory
    , private NSchemeCache::TSchemeCacheHelpers
{
    TStringBuf GetLogPrefix() const {
        if (!LogPrefix) {
            LogPrefix = TStringBuilder()
                << "[AsyncIndexChangeSenderMain]"
                << "[" << DataShard.TabletId << ":" << DataShard.Generation << "]"
                << SelfId() /* contains brackets */ << " ";
        }

        return LogPrefix.GetRef();
    }

    static TSerializedTableRange GetFullRange(ui32 keyColumnsCount) {
        TVector<TCell> fromValues(keyColumnsCount);
        TVector<TCell> toValues;
        return TSerializedTableRange(fromValues, true, toValues, false);
    }

    bool IsResolvingUserTable() const {
        return CurrentStateFunc() == static_cast<TReceiveFunc>(&TThis::StateResolveUserTable);
    }

    bool IsResolvingIndex() const {
        return CurrentStateFunc() == static_cast<TReceiveFunc>(&TThis::StateResolveIndex);
    }

    bool IsResolvingIndexTable() const {
        return CurrentStateFunc() == static_cast<TReceiveFunc>(&TThis::StateResolveIndexTable);
    }

    bool IsResolvingKeys() const {
        return CurrentStateFunc() == static_cast<TReceiveFunc>(&TThis::StateResolveKeys);
    }

    bool IsResolving() const override {
        return IsResolvingUserTable()
            || IsResolvingIndex()
            || IsResolvingIndexTable()
            || IsResolvingKeys();
    }

    TStringBuf CurrentStateName() const {
        if (IsResolvingUserTable()) {
            return "ResolveUserTable";
        } else if (IsResolvingIndex()) {
            return "ResolveIndex";
        } else if (IsResolvingIndexTable()) {
            return "ResolveIndexTable";
        } else if (IsResolvingKeys()) {
            return "ResolveKeys";
        } else {
            return "";
        }
    }

    void Retry() {
        Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup());
    }

    void LogCritAndRetry(const TString& error) {
        LOG_C(error);
        Retry();
    }

    void LogWarnAndRetry(const TString& error) {
        LOG_W(error);
        Retry();
    }

    template <typename CheckFunc, typename FailFunc, typename T, typename... Args>
    bool Check(CheckFunc checkFunc, FailFunc failFunc, const T& subject, Args&&... args) {
        return checkFunc(CurrentStateName(), subject, std::forward<Args>(args)..., std::bind(failFunc, this, std::placeholders::_1));
    }

    template <typename T>
    bool CheckNotEmpty(const TAutoPtr<T>& result) {
        return Check(&TSchemeCacheHelpers::CheckNotEmpty<T>, &TThis::LogCritAndRetry, result);
    }

    template <typename T>
    bool CheckEntriesCount(const TAutoPtr<T>& result, ui32 expected) {
        return Check(&TSchemeCacheHelpers::CheckEntriesCount<T>, &TThis::LogCritAndRetry, result, expected);
    }

    template <typename T>
    bool CheckTableId(const T& entry, const TTableId& expected) {
        return Check(&TSchemeCacheHelpers::CheckTableId<T>, &TThis::LogCritAndRetry, entry, expected);
    }

    template <typename T>
    bool CheckEntrySucceeded(const T& entry) {
        return Check(&TSchemeCacheHelpers::CheckEntrySucceeded<T>, &TThis::LogWarnAndRetry, entry);
    }

    template <typename T>
    bool CheckEntryKind(const T& entry, TNavigate::EKind expected) {
        return Check(&TSchemeCacheHelpers::CheckEntryKind<T>, &TThis::LogWarnAndRetry, entry, expected);
    }

    static TVector<ui64> MakePartitionIds(const TVector<TKeyDesc::TPartitionInfo>& partitions) {
        TVector<ui64> result(Reserve(partitions.size()));

        for (const auto& partition : partitions) {
            result.push_back(partition.ShardId); // partition = shard
        }

        return result;
    }

    /// ResolveUserTable

    void ResolveUserTable() {
        auto request = MakeHolder<TNavigate>();
        request->ResultSet.emplace_back(MakeNavigateEntry(UserTableId, TNavigate::OpTable));

        Send(MakeSchemeCacheID(), new TEvNavigate(request.Release()));
        Become(&TThis::StateResolveUserTable);
    }

    STATEFN(StateResolveUserTable) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleUserTable);
            sFunc(TEvents::TEvWakeup, ResolveUserTable);
        default:
            return StateBase(ev);
        }
    }

    void HandleUserTable(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& result = ev->Get()->Request;

        LOG_D("HandleUserTable TEvTxProxySchemeCache::TEvNavigateKeySetResult"
            << ": result# " << (result ? result->ToString(*AppData()->TypeRegistry) : "nullptr"));

        if (!CheckNotEmpty(result)) {
            return;
        }

        if (!CheckEntriesCount(result, 1)) {
            return;
        }

        const auto& entry = result->ResultSet.at(0);

        if (!CheckTableId(entry, UserTableId)) {
            return;
        }

        if (!CheckEntrySucceeded(entry)) {
            return;
        }

        if (!CheckEntryKind(entry, TNavigate::KindTable)) {
            return;
        }

        for (const auto& [tag, column] : entry.Columns) {
            Y_DEBUG_ABORT_UNLESS(!MainColumnToTag.contains(column.Name));
            MainColumnToTag.emplace(column.Name, tag);
        }

        ResolveIndex();
    }

    /// ResolveIndex

    void ResolveIndex() {
        auto request = MakeHolder<TNavigate>();
        request->ResultSet.emplace_back(MakeNavigateEntry(PathId, TNavigate::OpList));

        Send(MakeSchemeCacheID(), new TEvNavigate(request.Release()));
        Become(&TThis::StateResolveIndex);
    }

    STATEFN(StateResolveIndex) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleIndex);
            sFunc(TEvents::TEvWakeup, ResolveIndex);
        default:
            return StateBase(ev);
        }
    }

    void HandleIndex(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& result = ev->Get()->Request;

        LOG_D("HandleIndex TEvTxProxySchemeCache::TEvNavigateKeySetResult"
            << ": result# " << (result ? result->ToString(*AppData()->TypeRegistry) : "nullptr"));

        if (!CheckNotEmpty(result)) {
            return;
        }

        if (!CheckEntriesCount(result, 1)) {
            return;
        }

        const auto& entry = result->ResultSet.at(0);

        if (!CheckTableId(entry, PathId)) {
            return;
        }

        if (!CheckEntrySucceeded(entry)) {
            return;
        }

        if (!CheckEntryKind(entry, TNavigate::KindIndex)) {
            return;
        }

        if (entry.Self && entry.Self->Info.GetPathState() == NKikimrSchemeOp::EPathStateDrop) {
            LOG_D("Index is planned to drop, waiting for the EvRemoveSender command");

            RemoveRecords();
            KillSenders();
            return Become(&TThis::StatePendingRemove);
        }

        Y_ABORT_UNLESS(entry.ListNodeEntry->Children.size() == 1);
        const auto& indexTable = entry.ListNodeEntry->Children.at(0);

        Y_ABORT_UNLESS(indexTable.Kind == TNavigate::KindTable);
        IndexTablePathId = indexTable.PathId;

        ResolveIndexTable();
    }

    /// ResolveIndexTable

    void ResolveIndexTable() {
        auto request = MakeHolder<TNavigate>();
        request->ResultSet.emplace_back(MakeNavigateEntry(IndexTablePathId, TNavigate::OpTable));

        Send(MakeSchemeCacheID(), new TEvNavigate(request.Release()));
        Become(&TThis::StateResolveIndexTable);
    }

    STATEFN(StateResolveIndexTable) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleIndexTable);
            sFunc(TEvents::TEvWakeup, ResolveIndex);
        default:
            return StateBase(ev);
        }
    }

    void HandleIndexTable(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& result = ev->Get()->Request;

        LOG_D("HandleIndexTable TEvTxProxySchemeCache::TEvNavigateKeySetResult"
            << ": result# " << (result ? result->ToString(*AppData()->TypeRegistry) : "nullptr"));

        if (!CheckNotEmpty(result)) {
            return;
        }

        if (!CheckEntriesCount(result, 1)) {
            return;
        }

        const auto& entry = result->ResultSet.at(0);

        if (!CheckTableId(entry, IndexTablePathId)) {
            return;
        }

        if (!CheckEntrySucceeded(entry)) {
            return;
        }

        if (!CheckEntryKind(entry, TNavigate::KindTable)) {
            return;
        }

        TagMap.clear();
        TVector<NScheme::TTypeInfo> keyColumnTypes;

        for (const auto& [tag, column] : entry.Columns) {
            auto it = MainColumnToTag.find(column.Name);
            Y_ABORT_UNLESS(it != MainColumnToTag.end());

            Y_DEBUG_ABORT_UNLESS(!TagMap.contains(it->second));
            TagMap.emplace(it->second, tag);

            if (column.KeyOrder < 0) {
                continue;
            }

            if (keyColumnTypes.size() <= static_cast<ui32>(column.KeyOrder)) {
                keyColumnTypes.resize(column.KeyOrder + 1);
            }

            keyColumnTypes[column.KeyOrder] = column.PType;
        }

        KeyDesc = MakeHolder<TKeyDesc>(
            entry.TableId,
            GetFullRange(keyColumnTypes.size()).ToTableRange(),
            TKeyDesc::ERowOperation::Update,
            keyColumnTypes,
            TVector<TKeyDesc::TColumnOp>()
        );

        SetPartitioner(NChangeExchange::CreateSchemaBoundaryPartitioner<TChangeRecord>(*KeyDesc.Get()));

        ResolveKeys();
    }

    /// ResolveKeys

    void ResolveKeys() {
        auto request = MakeHolder<TResolve>();
        request->ResultSet.emplace_back(std::move(KeyDesc));

        Send(MakeSchemeCacheID(), new TEvResolve(request.Release()));
        Become(&TThis::StateResolveKeys);
    }

    STATEFN(StateResolveKeys) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, HandleKeys);
            sFunc(TEvents::TEvWakeup, ResolveIndex);
        default:
            return StateBase(ev);
        }
    }

    void HandleKeys(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev) {
        const auto& result = ev->Get()->Request;

        LOG_D("HandleKeys TEvTxProxySchemeCache::TEvResolveKeySetResult"
            << ": result# " << (result ? result->ToString(*AppData()->TypeRegistry) : "nullptr"));

        if (!CheckNotEmpty(result)) {
            return;
        }

        if (!CheckEntriesCount(result, 1)) {
            return;
        }

        auto& entry = result->ResultSet.at(0);

        if (!CheckTableId(entry, IndexTablePathId)) {
            return;
        }

        if (!CheckEntrySucceeded(entry)) {
            return;
        }

        if (!entry.KeyDescription->GetPartitions()) {
            LOG_W("Empty partitions list"
                << ": entry# " << entry.ToString(*AppData()->TypeRegistry));
            return Retry();
        }

        const bool versionChanged = !IndexTableVersion || IndexTableVersion != entry.GeneralVersion;
        IndexTableVersion = entry.GeneralVersion;

        KeyDesc = std::move(entry.KeyDescription);
        CreateSenders(MakePartitionIds(KeyDesc->GetPartitions()), versionChanged);

        Become(&TThis::StateMain);
    }

    /// Main

    STATEFN(StateMain) {
        return StateBase(ev);
    }

    void Resolve() override {
        ResolveIndex();
    }

    bool IsResolved() const override {
        return KeyDesc && KeyDesc->GetPartitions();
    }

    IActor* CreateSender(ui64 partitionId) const override {
        return new TAsyncIndexChangeSenderShard(SelfId(), DataShard, partitionId, IndexTablePathId, TagMap);
    }

    void Handle(NChangeExchange::TEvChangeExchange::TEvEnqueueRecords::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());
        EnqueueRecords(std::move(ev->Get()->Records));
    }

    void Handle(NChangeExchange::TEvChangeExchange::TEvRecords::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());
        auto& records = std::get<std::shared_ptr<TChangeRecordContainer<NKikimr::NDataShard::TChangeRecord>>>(ev->Get()->Records)->Records;
        ProcessRecords(std::move(records));
    }

    void Handle(NChangeExchange::TEvChangeExchange::TEvForgetRecords::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());
        ForgetRecords(std::move(ev->Get()->Records));
    }

    void Handle(NChangeExchange::TEvChangeExchangePrivate::TEvReady::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());
        OnReady(ev->Get()->PartitionId);
    }

    void Handle(NChangeExchange::TEvChangeExchangePrivate::TEvGone::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());
        OnGone(ev->Get()->PartitionId);
    }

    void Handle(TEvChangeExchange::TEvRemoveSender::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());
        Y_ABORT_UNLESS(ev->Get()->PathId == PathId);

        RemoveRecords();
        PassAway();
    }

    void AutoRemove(NChangeExchange::TEvChangeExchange::TEvEnqueueRecords::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());
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
        , TBaseChangeSender(this, this, this, dataShard.ActorId, indexPathId)
        , DataShard(dataShard)
        , UserTableId(userTableId)
        , IndexTableVersion(0)
    {
    }

    void Bootstrap() {
        ResolveUserTable();
    }

    STFUNC(StateBase) {
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
        switch (ev->GetTypeRewrite()) {
            hFunc(NChangeExchange::TEvChangeExchange::TEvEnqueueRecords, AutoRemove);
            hFunc(TEvChangeExchange::TEvRemoveSender, Handle);
            HFunc(NMon::TEvRemoteHttpInfo, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    const TDataShardId DataShard;
    const TTableId UserTableId;
    mutable TMaybe<TString> LogPrefix;

    THashMap<TString, TTag> MainColumnToTag;
    TMap<TTag, TTag> TagMap; // from main to index

    TPathId IndexTablePathId;
    ui64 IndexTableVersion;
    THolder<TKeyDesc> KeyDesc;

}; // TAsyncIndexChangeSenderMain

IActor* CreateAsyncIndexChangeSender(const TDataShardId& dataShard, const TTableId& userTableId, const TPathId& indexPathId) {
    return new TAsyncIndexChangeSenderMain(dataShard, userTableId, indexPathId);
}

}
