#pragma once

#include <ydb/core/tx/scheme_cache/helpers.h>
#include <ydb/core/change_exchange/change_sender.h>
#include <ydb/core/change_exchange/change_sender_monitoring.h>

//

#include <ydb/core/tx/tx_proxy/proxy.h>
#include "datashard_impl.h"
#include <ydb/core/base/tablet_pipecache.h>

namespace NKikimr::NDataShard {

using namespace NTable;

#define DEFINE_STATE_INTRO \
    public: \
        struct TStateTag {}; \
    private: \
        const TDerived* AsDerived() const { \
            return static_cast<const TDerived*>(this); \
        } \
        TDerived* AsDerived() { \
            return static_cast<TDerived*>(this); \
        } \
        TStringBuf GetLogPrefix() const { \
            return AsDerived()->GetLogPrefix(); \
        }

#define USE_STATE(STATE) \
    friend class T ## STATE ## State; \
    STATEFN(State ## STATE) { \
        return T ## STATE ## State::State(ev); \
    } \
    bool Is ## STATE ## State() const { \
        return CurrentStateFunc() == static_cast<TReceiveFunc>(&TThis::State ## STATE); \
    }

template <typename TDerived>
class TResolveUserTableState
    : virtual public NSchemeCache::TSchemeCacheHelpers
{
    DEFINE_STATE_INTRO;

public:
    void ResolveUserTable() {
        auto request = MakeHolder<TNavigate>();
        request->ResultSet.emplace_back(MakeNavigateEntry(AsDerived()->UserTableId, TNavigate::OpTable));

        AsDerived()->Send(MakeSchemeCacheID(), new TEvNavigate(request.Release()));
        AsDerived()->Become(&TDerived::StateResolveUserTable);
    }

    STATEFN(State) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            sFunc(TEvents::TEvWakeup, ResolveUserTable);
        default:
            return AsDerived()->StateBase(ev);
        }
    }

private:
    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& result = ev->Get()->Request;

        LOG_D("HandleUserTable TEvTxProxySchemeCache::TEvNavigateKeySetResult"
            << ": result# " << (result ? result->ToString(*AppData()->TypeRegistry) : "nullptr"));

        if (!AsDerived()->CheckNotEmpty(result)) {
            return;
        }

        if (!AsDerived()->CheckEntriesCount(result, 1)) {
            return;
        }

        const auto& entry = result->ResultSet.at(0);

        if (!AsDerived()->CheckTableId(entry, AsDerived()->UserTableId)) {
            return;
        }

        if (!AsDerived()->CheckEntrySucceeded(entry)) {
            return;
        }

        if (!AsDerived()->CheckEntryKind(entry, TNavigate::KindTable)) {
            return;
        }

        for (const auto& [tag, column] : entry.Columns) {
            Y_DEBUG_ABORT_UNLESS(!AsDerived()->MainColumnToTag.contains(column.Name));
            AsDerived()->MainColumnToTag.emplace(column.Name, tag);
        }

        AsDerived()->NextState(TStateTag{});
    }
};

template <typename TDerived>
class TResolveTargetTableState
    : virtual public NSchemeCache::TSchemeCacheHelpers
{
    DEFINE_STATE_INTRO;

public:
    void ResolveTargetTable() {
        auto request = MakeHolder<TNavigate>();
        request->ResultSet.emplace_back(MakeNavigateEntry(AsDerived()->TargetTablePathId, TNavigate::OpTable));

        AsDerived()->Send(MakeSchemeCacheID(), new TEvNavigate(request.Release()));
        AsDerived()->Become(&TDerived::StateResolveTargetTable);
    }

    STATEFN(State) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            sFunc(TEvents::TEvWakeup, OnRetry);
        default:
            return AsDerived()->StateBase(ev);
        }
    }

private:
    void OnRetry() {
        AsDerived()->OnRetry(TStateTag{});
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& result = ev->Get()->Request;

        LOG_D("HandleTargetTable TEvTxProxySchemeCache::TEvNavigateKeySetResult"
            << ": result# " << (result ? result->ToString(*AppData()->TypeRegistry) : "nullptr"));

        if (!AsDerived()->CheckNotEmpty(result)) {
            return;
        }

        if (!AsDerived()->CheckEntriesCount(result, 1)) {
            return;
        }

        const auto& entry = result->ResultSet.at(0);

        if (!AsDerived()->CheckTableId(entry, AsDerived()->TargetTablePathId)) {
            return;
        }

        if (!AsDerived()->CheckEntrySucceeded(entry)) {
            return;
        }

        if (!AsDerived()->CheckEntryKind(entry, TNavigate::KindTable)) {
            return;
        }

        AsDerived()->TagMap.clear();
        TVector<NScheme::TTypeInfo> keyColumnTypes;

        for (const auto& [tag, column] : entry.Columns) {
            auto it = AsDerived()->MainColumnToTag.find(column.Name);
            Y_ABORT_UNLESS(it != AsDerived()->MainColumnToTag.end());

            Y_DEBUG_ABORT_UNLESS(!AsDerived()->TagMap.contains(it->second));
            AsDerived()->TagMap.emplace(it->second, tag);

            if (column.KeyOrder < 0) {
                continue;
            }

            if (keyColumnTypes.size() <= static_cast<ui32>(column.KeyOrder)) {
                keyColumnTypes.resize(column.KeyOrder + 1);
            }

            keyColumnTypes[column.KeyOrder] = column.PType;
        }

        AsDerived()->KeyDesc = MakeHolder<TKeyDesc>(
            entry.TableId,
            AsDerived()->GetFullRange(keyColumnTypes.size()).ToTableRange(),
            TKeyDesc::ERowOperation::Update,
            keyColumnTypes,
            TVector<TKeyDesc::TColumnOp>()
        );

        AsDerived()->SetPartitionResolver(CreateDefaultPartitionResolver(*AsDerived()->KeyDesc.Get()));

        AsDerived()->NextState(TStateTag{});
    }
};

template <typename TDerived>
class TResolveKeysState
    : virtual public NSchemeCache::TSchemeCacheHelpers
{
    DEFINE_STATE_INTRO;

public:
    void ResolveKeys() {
        auto request = MakeHolder<TResolve>();
        request->ResultSet.emplace_back(std::move(AsDerived()->KeyDesc));

        AsDerived()->Send(MakeSchemeCacheID(), new TEvResolve(request.Release()));
        AsDerived()->Become(&TDerived::StateResolveKeys);
    }

    STATEFN(State) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, Handle);
            sFunc(TEvents::TEvWakeup, OnRetry);
        default:
            return AsDerived()->StateBase(ev);
        }
    }

private:
    void OnRetry() {
        AsDerived()->OnRetry(TStateTag{});
    }

    void Handle(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev) {
        const auto& result = ev->Get()->Request;

        LOG_D("HandleKeys TEvTxProxySchemeCache::TEvResolveKeySetResult"
            << ": result# " << (result ? result->ToString(*AppData()->TypeRegistry) : "nullptr"));

        if (!AsDerived()->CheckNotEmpty(result)) {
            return;
        }

        if (!AsDerived()->CheckEntriesCount(result, 1)) {
            return;
        }

        auto& entry = result->ResultSet.at(0);

        if (!AsDerived()->CheckTableId(entry, AsDerived()->TargetTablePathId)) {
            return;
        }

        if (!AsDerived()->CheckEntrySucceeded(entry)) {
            return;
        }

        if (!entry.KeyDescription->GetPartitions()) {
            LOG_W("Empty partitions list"
                << ": entry# " << entry.ToString(*AppData()->TypeRegistry));
            return AsDerived()->Retry();
        }

        const bool versionChanged = !AsDerived()->TargetTableVersion || AsDerived()->TargetTableVersion != entry.GeneralVersion;
        AsDerived()->TargetTableVersion = entry.GeneralVersion;

        AsDerived()->KeyDesc = std::move(entry.KeyDescription);
        AsDerived()->CreateSenders(MakePartitionIds(AsDerived()->KeyDesc->GetPartitions()), versionChanged);

        AsDerived()->NextState(TStateTag{});
    }

    static TVector<ui64> MakePartitionIds(const TVector<TKeyDesc::TPartitionInfo>& partitions) {
        TVector<ui64> result(Reserve(partitions.size()));

        for (const auto& partition : partitions) {
            result.push_back(partition.ShardId); // partition = shard
        }

        return result;
    }
};

template <typename TDerived>
struct TSchemeChecksMixin
    : virtual private NSchemeCache::TSchemeCacheHelpers
{
    const TDerived* AsDerived() const {
        return static_cast<const TDerived*>(this);
    }

    TDerived* AsDerived() {
        return static_cast<TDerived*>(this);
    }

    template <typename CheckFunc, typename FailFunc, typename T, typename... Args>
    bool Check(CheckFunc checkFunc, FailFunc failFunc, const T& subject, Args&&... args) {
        return checkFunc(AsDerived()->CurrentStateName(), subject, std::forward<Args>(args)..., std::bind(failFunc, AsDerived(), std::placeholders::_1));
    }

    template <typename T>
    bool CheckNotEmpty(const TAutoPtr<T>& result) {
        return Check(&TSchemeCacheHelpers::CheckNotEmpty<T>, &TDerived::LogCritAndRetry, result);
    }

    template <typename T>
    bool CheckEntriesCount(const TAutoPtr<T>& result, ui32 expected) {
        return Check(&TSchemeCacheHelpers::CheckEntriesCount<T>, &TDerived::LogCritAndRetry, result, expected);
    }

    template <typename T>
    bool CheckTableId(const T& entry, const TTableId& expected) {
        return Check(&TSchemeCacheHelpers::CheckTableId<T>, &TDerived::LogCritAndRetry, entry, expected);
    }

    template <typename T>
    bool CheckEntrySucceeded(const T& entry) {
        return Check(&TSchemeCacheHelpers::CheckEntrySucceeded<T>, &TDerived::LogWarnAndRetry, entry);
    }

    template <typename T>
    bool CheckEntryKind(const T& entry, TNavigate::EKind expected) {
        return Check(&TSchemeCacheHelpers::CheckEntryKind<T>, &TDerived::LogWarnAndRetry, entry, expected);
    }

};

class TBaseChangeSenderShard: public TActorBootstrapped<TBaseChangeSenderShard> {
    TStringBuf GetLogPrefix() const {
        if (!LogPrefix) {
            LogPrefix = TStringBuilder()
                << "[BaseChangeSenderShard]"
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

    class TSerializer: public NChangeExchange::TBaseVisitor {
        NKikimrChangeExchange::TChangeRecord& Record;

    public:
        explicit TSerializer(NKikimrChangeExchange::TChangeRecord& record)
            : Record(record)
        {
        }

        void Visit(const TChangeRecord& record) override {
            record.Serialize(Record);
        }
    };

    void Handle(NChangeExchange::TEvChangeExchange::TEvRecords::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());

        auto records = MakeHolder<TEvChangeExchange::TEvApplyRecords>();
        records->Record.SetOrigin(DataShard.TabletId);
        records->Record.SetGeneration(DataShard.Generation);

        for (auto recordPtr : ev->Get()->Records) {
            const auto& record = *recordPtr;

            if (record.GetOrder() <= LastRecordOrder) {
                continue;
            }

            auto& proto = *records->Record.AddRecords();
            TSerializer serializer(proto);
            record.Accept(serializer);
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
            Header(html, "Base partition change sender", DataShard.TabletId);

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

    TBaseChangeSenderShard(const TActorId& parent, const TDataShardId& dataShard, ui64 shardId,
            const TPathId& indexTablePathId, const TMap<TTag, TTag>& tagMap)
        : Parent(parent)
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

} // namespace NKikimr::NDataShard
