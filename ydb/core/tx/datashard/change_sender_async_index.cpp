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

struct TIndexPathId {
    TPathId Value;
};

struct TTargetTablePathId {
    TPathId Value;
};

class TBaseChangeSenderShard: public TActorBootstrapped<TBaseChangeSenderShard> {
    TStringBuf GetLogPrefix() const {
        if (!LogPrefix) {
            LogPrefix = TStringBuilder()
                << "[" << LogName << "ChangeSenderShard]"
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

        for (auto recordPtr : ev->Get()->GetRecords<TChangeRecord>()) {
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
        record.SetPathOwnerId(TargetTablePathId.OwnerId);
        record.SetLocalPathId(TargetTablePathId.LocalPathId);

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
                        TermDesc(html, "TargetTablePathId", TargetTablePathId);
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

    TBaseChangeSenderShard(
        const TString& logName,
        const TActorId& parent,
        const TDataShardId& dataShard,
        ui64 shardId,
        const TPathId& indexTablePathId,
        const TMap<TTag, TTag>& tagMap)
        : LogName(logName)
        , Parent(parent)
        , DataShard(dataShard)
        , ShardId(shardId)
        , TargetTablePathId(indexTablePathId)
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
    const TString LogName;
    const TActorId Parent;
    const TDataShardId DataShard;
    const ui64 ShardId;
    const TPathId TargetTablePathId;
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

}; // TBaseChangeSenderShard

struct IChangeSenderMain {
    enum class EState {
        Init,
        ResolveUserTable,
        ResolveIndex,
        ResolveTargetTable,
        ResolveKeys,
        Unknown,
    };

    virtual ~IChangeSenderMain() = default;
    virtual void ResolveUserTable() = 0;
    virtual void ResolveIndex() = 0;
    virtual void ResolveTargetTable() = 0;
    virtual void ResolveKeys() = 0;
    virtual void Serve() = 0;
    virtual EState GetState() = 0;
};

struct IChangeSenderMainImpl {
    using EState = IChangeSenderMain::EState;
    virtual ~IChangeSenderMainImpl() = default;
    virtual void NextState(IChangeSenderMain* self) const = 0;
    virtual const char* GetLogName() const = 0;
};

class TBaseChangeSenderMain
    : public TActorBootstrapped<TBaseChangeSenderMain>
    , public IChangeSenderMain
    , public NChangeExchange::TBaseChangeSender<TChangeRecord>
    , public NChangeExchange::IChangeSenderIdentity
    , public NChangeExchange::IChangeSenderResolver
    , public NChangeExchange::ISenderFactory
    , private NSchemeCache::TSchemeCacheHelpers
{
    TStringBuf GetLogPrefix() const {
        if (!LogPrefix) {
            LogPrefix = TStringBuilder()
                << "[" << Impl->GetLogName() << "ChangeSenderMain]"
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

    bool IsInit() const {
        return CurrentStateFunc() == static_cast<TReceiveFunc>(&TThis::StateBootstrap);
    }

    bool IsResolvingUserTable() const {
        return CurrentStateFunc() == static_cast<TReceiveFunc>(&TThis::StateResolveUserTable);
    }

    bool IsResolvingIndex() const {
        return CurrentStateFunc() == static_cast<TReceiveFunc>(&TThis::StateResolveIndex);
    }

    bool IsResolvingIndexTable() const {
        return CurrentStateFunc() == static_cast<TReceiveFunc>(&TThis::StateResolveTargetTable);
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
            return "ResolveTargetTable";
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

        Impl->NextState(this);
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

        if (!CheckTableId(entry, IndexPathId)) {
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
        TargetTablePathId = indexTable.PathId;

        Impl->NextState(this);
    }

    STATEFN(StateResolveTargetTable) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleTargetTable);
            sFunc(TEvents::TEvWakeup, ResolveIndex);
        default:
            return StateBase(ev);
        }
    }

    void HandleTargetTable(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& result = ev->Get()->Request;

        LOG_D("HandleTargetTable TEvTxProxySchemeCache::TEvNavigateKeySetResult"
            << ": result# " << (result ? result->ToString(*AppData()->TypeRegistry) : "nullptr"));

        if (!CheckNotEmpty(result)) {
            return;
        }

        if (!CheckEntriesCount(result, 1)) {
            return;
        }

        const auto& entry = result->ResultSet.at(0);

        if (!CheckTableId(entry, TargetTablePathId)) {
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

        Impl->NextState(this);
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

        if (!CheckTableId(entry, TargetTablePathId)) {
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

        const bool versionChanged = !TargetTableVersion || TargetTableVersion != entry.GeneralVersion;
        TargetTableVersion = entry.GeneralVersion;

        KeyDesc = std::move(entry.KeyDescription);
        CreateSenders(MakePartitionIds(KeyDesc->GetPartitions()), versionChanged);

        Impl->NextState(this);
    }

    /// Main

    STATEFN(StateMain) {
        return StateBase(ev);
    }

    void Resolve() override {
        Become(&TThis::StateBootstrap);
        Impl->NextState(this);
    }

    bool IsResolved() const override {
        return KeyDesc && KeyDesc->GetPartitions();
    }

    IActor* CreateSender(ui64 partitionId) const override {
        return new TBaseChangeSenderShard(Impl->GetLogName(), SelfId(), DataShard, partitionId, TargetTablePathId, TagMap);
    }

    void Handle(NChangeExchange::TEvChangeExchange::TEvEnqueueRecords::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());
        EnqueueRecords(std::move(ev->Get()->Records));
    }

    void Handle(NChangeExchange::TEvChangeExchange::TEvRecords::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());
        ProcessRecords(std::move(ev->Get()->GetRecords<TChangeRecord>()));
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
        Y_ABORT_UNLESS(ev->Get()->PathId == GetChangeSenderIdentity());

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

    explicit TBaseChangeSenderMain(
        THolder<IChangeSenderMainImpl> impl,
        const TDataShardId& dataShard,
        const TTableId& userTableId,
        const TIndexPathId& indexPathId)
        : TActorBootstrapped()
        , TBaseChangeSender(this, this, this, this, dataShard.ActorId)
        , IndexPathId(indexPathId.Value)
        , Impl(std::move(impl))
        , DataShard(dataShard)
        , UserTableId(userTableId)
        , TargetTableVersion(0)
    {
    }

    explicit TBaseChangeSenderMain(
        THolder<IChangeSenderMainImpl> impl,
        const TDataShardId& dataShard,
        const TTableId& userTableId,
        const TTargetTablePathId& targetTablePathId)
        : TActorBootstrapped()
        , TBaseChangeSender(this, this, this, this, dataShard.ActorId)
        , Impl(std::move(impl))
        , DataShard(dataShard)
        , UserTableId(userTableId)
        , TargetTablePathId(targetTablePathId.Value)
        , TargetTableVersion(0)
    {
    }

    void Bootstrap() {
        Impl->NextState(this);
    }

    TPathId GetChangeSenderIdentity() const override final {
        return IndexPathId;
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

    void ResolveUserTable() override final {
        auto request = MakeHolder<TNavigate>();
        request->ResultSet.emplace_back(MakeNavigateEntry(UserTableId, TNavigate::OpTable));

        Send(MakeSchemeCacheID(), new TEvNavigate(request.Release()));
        Become(&TThis::StateResolveUserTable);
    }

    void ResolveIndex() override final {
        auto request = MakeHolder<TNavigate>();
        request->ResultSet.emplace_back(MakeNavigateEntry(IndexPathId, TNavigate::OpList));

        Send(MakeSchemeCacheID(), new TEvNavigate(request.Release()));
        Become(&TThis::StateResolveIndex);
    }

    void ResolveTargetTable() override final {
        auto request = MakeHolder<TNavigate>();
        request->ResultSet.emplace_back(MakeNavigateEntry(TargetTablePathId, TNavigate::OpTable));

        Send(MakeSchemeCacheID(), new TEvNavigate(request.Release()));
        Become(&TThis::StateResolveTargetTable);
    }

    void ResolveKeys() override final {
        auto request = MakeHolder<TResolve>();
        request->ResultSet.emplace_back(std::move(KeyDesc));

        Send(MakeSchemeCacheID(), new TEvResolve(request.Release()));
        Become(&TThis::StateResolveKeys);
    }

    void Serve() override final {
        Become(&TThis::StateMain);
    }

    EState GetState() override final {
        if (IsInit()) {
            return EState::Init;
        } else if (IsResolvingUserTable()) {
            return EState::ResolveUserTable;
        } else if (IsResolvingIndex()) {
            return EState::ResolveIndex;
        } else if (IsResolvingIndexTable()) {
            return EState::ResolveTargetTable;
        } else if (IsResolvingKeys()) {
            return EState::ResolveKeys;
        } else {
            return EState::Unknown;
        }
    }

private:
    const TPathId IndexPathId;
    THolder<IChangeSenderMainImpl> Impl;
    const TDataShardId DataShard;
    const TTableId UserTableId;
    mutable TMaybe<TString> LogPrefix;

    THashMap<TString, TTag> MainColumnToTag;
    TMap<TTag, TTag> TagMap; // from main to target

    TPathId TargetTablePathId;
    ui64 TargetTableVersion;
    THolder<TKeyDesc> KeyDesc;
}; // TBaseChangeSenderMain

struct TAsyncIndexChangeSenderMainImpl
    : public IChangeSenderMainImpl
{
    void NextState(IChangeSenderMain* self) const override final {
        switch(self->GetState()) {
            case EState::Init:
                self->ResolveUserTable();
                break;
            case EState::ResolveUserTable:
                self->ResolveIndex();
                break;
            case EState::ResolveIndex:
                self->ResolveTargetTable();
                break;
            case EState::ResolveTargetTable:
                self->ResolveKeys();
                break;
            case EState::ResolveKeys:
                self->Serve();
                break;
            default:
                Y_ABORT("unreachable");
        }
    }

    const char* GetLogName() const override final {
        return "AsyncIndex";
    }
};

IActor* CreateAsyncIndexChangeSender(const TDataShardId& dataShard, const TTableId& userTableId, const TPathId& indexPathId) {
    return new TBaseChangeSenderMain(MakeHolder<TAsyncIndexChangeSenderMainImpl>(), dataShard, userTableId, TIndexPathId(indexPathId));
}

}
