#include "change_exchange.h"
#include "change_exchange_impl.h"
#include "change_record.h"
#include "change_record_cdc_serializer.h"
#include "datashard_user_table.h"

#include <ydb/core/change_exchange/change_sender_common_ops.h>
#include <ydb/core/change_exchange/change_sender_monitoring.h>
#include <ydb/core/persqueue/partition_key_range/partition_key_range.h>
#include <ydb/core/persqueue/writer/source_id_encoding.h>
#include <ydb/core/persqueue/writer/writer.h>
#include <ydb/core/tx/scheme_cache/helpers.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/services/lib/sharding/sharding.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <library/cpp/json/json_writer.h>

namespace NKikimr::NDataShard {

using namespace NPQ;
using ESenderType = TEvChangeExchange::ESenderType;

class TCdcChangeSenderPartition: public TActorBootstrapped<TCdcChangeSenderPartition> {
    TStringBuf GetLogPrefix() const {
        if (!LogPrefix) {
            LogPrefix = TStringBuilder()
                << "[CdcChangeSenderPartition]"
                << "[" << DataShard.TabletId << ":" << DataShard.Generation << "]"
                << "[" << PartitionId << "]"
                << "[" << ShardId << "]"
                << SelfId() /* contains brackets */ << " ";
        }

        return LogPrefix.GetRef();
    }

    /// Init

    void Init() {
        auto opts = TPartitionWriterOpts()
            .WithCheckState(true)
            .WithAutoRegister(true)
            .WithSourceId(SourceId);
        Writer = RegisterWithSameMailbox(CreatePartitionWriter(SelfId(), ShardId, PartitionId, opts));
        Become(&TThis::StateInit);
    }

    STATEFN(StateInit) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPartitionWriter::TEvInitResult, Handle);
        default:
            return StateBase(ev);
        }
    }

    void Handle(TEvPartitionWriter::TEvInitResult::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());

        const auto& result = *ev->Get();
        if (!result.IsSuccess()) {
            LOG_E("Error at 'Init': " << result.GetError().ToString());
            return Leave();
        }

        const auto& info = result.GetResult().SourceIdInfo;
        Y_ABORT_UNLESS(info.GetExplicit());

        MaxSeqNo = info.GetSeqNo();
        Ready();
    }

    void Ready() {
        Pending.clear();

        Send(Parent, new NChangeExchange::TEvChangeExchangePrivate::TEvReady(PartitionId));
        Become(&TThis::StateWaitingRecords);
    }

    /// WaitingRecords

    STATEFN(StateWaitingRecords) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NChangeExchange::TEvChangeExchange::TEvRecords, Handle);
            sFunc(TEvPartitionWriter::TEvWriteResponse, Lost);
        default:
            return StateBase(ev);
        }
    }

    void Handle(NChangeExchange::TEvChangeExchange::TEvRecords::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());
        NKikimrClient::TPersQueueRequest request;

        for (auto recordPtr : ev->Get()->GetRecords<TChangeRecord>()) {
            const auto& record = *recordPtr;

            if (record.GetSeqNo() <= MaxSeqNo) {
                continue;
            }

            auto& cmd = *request.MutablePartitionRequest()->AddCmdWrite();
            cmd.SetSourceId(NSourceIdEncoding::EncodeSimple(SourceId));
            cmd.SetIgnoreQuotaDeadline(true);
            Serializer->Serialize(cmd, record);

            Pending.push_back(record.GetSeqNo());
        }

        if (!Pending) {
            return Ready();
        }

        Write(std::move(request));
    }

    /// Write

    void Write(NKikimrClient::TPersQueueRequest&& request) {
        auto ev = MakeHolder<TEvPartitionWriter::TEvWriteRequest>();
        ev->Record = std::move(request);
        ev->Record.MutablePartitionRequest()->SetCookie(++Cookie);

        Send(Writer, std::move(ev));
        Become(&TThis::StateWrite);
    }

    STATEFN(StateWrite) {
        switch (ev->GetTypeRewrite()) {
            IgnoreFunc(TEvPartitionWriter::TEvWriteAccepted);
            hFunc(TEvPartitionWriter::TEvWriteResponse, Handle);
        default:
            return StateBase(ev);
        }
    }

    void Handle(TEvPartitionWriter::TEvWriteResponse::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());

        const auto& result = *ev->Get();
        if (!result.IsSuccess()) {
            LOG_E("Error at 'Write': " << result.DumpError());
            return Leave();
        }

        const auto& response = result.Record.GetPartitionResponse();
        if (response.GetCookie() != Cookie) {
            LOG_E("Cookie mismatch"
                << ": expected# " << Cookie
                << ", got# " << response.GetCookie());
            return Leave();
        }

        if (response.CmdWriteResultSize() != Pending.size()) {
            LOG_E("Write result size mismatch"
                << ": expected# " << Pending.size()
                << ", got# " << response.CmdWriteResultSize());
            return Leave();
        }

        for (ui32 i = 0; i < Pending.size(); ++i) {
            const auto expected = Pending.at(i);
            const auto got = response.GetCmdWriteResult(i).GetSeqNo();

            if (expected == got) {
                MaxSeqNo = Max(MaxSeqNo, got);
                continue;
            }

            LOG_E("SeqNo mismatch"
                << ": expected# " << expected
                << ", got# " << got);
            return Leave();
        }

        Ready();
    }

    void Handle(NMon::TEvRemoteHttpInfo::TPtr& ev) {
        using namespace NChangeExchange;

        TStringStream html;

        HTML(html) {
            Header(html, "CdcStream partition change sender", DataShard.TabletId);

            SimplePanel(html, "Info", [this](IOutputStream& html) {
                HTML(html) {
                    DL_CLASS("dl-horizontal") {
                        TermDesc(html, "PartitionId", PartitionId);
                        TermDescLink(html, "ShardId", ShardId, TabletPath(ShardId));
                        TermDesc(html, "SourceId", SourceId);
                        TermDesc(html, "Writer", Writer);
                        TermDesc(html, "MaxSeqNo", MaxSeqNo);
                        TermDesc(html, "Pending", Pending.size());
                        TermDesc(html, "Cookie", Cookie);
                    }
                }
            });
        }

        Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes(html.Str()));
    }

    void Disconnected() {
        LOG_D("Disconnected");

        if (CurrentStateFunc() != static_cast<TReceiveFunc>(&TThis::StateInit)) {
            return Leave();
        }

        CloseWriter();
        Schedule(TDuration::MilliSeconds(100), new TEvents::TEvWakeup());
    }

    void Lost() {
        LOG_W("Lost");
        Leave();
    }

    void Leave() {
        Send(Parent, new NChangeExchange::TEvChangeExchangePrivate::TEvGone(PartitionId));
        PassAway();
    }

    void CloseWriter() {
        if (const auto& writer = std::exchange(Writer, {})) {
            Send(writer, new TEvents::TEvPoisonPill());
        }
    }

    void PassAway() override {
        CloseWriter();
        TActorBootstrapped::PassAway();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::CHANGE_SENDER_CDC_ACTOR_PARTITION;
    }

    explicit TCdcChangeSenderPartition(
            const TActorId& parent,
            const TDataShardId& dataShard,
            ui32 partitionId,
            ui64 shardId,
            const TUserTable::TCdcStream& stream)
        : Parent(parent)
        , DataShard(dataShard)
        , PartitionId(partitionId)
        , ShardId(shardId)
        , SourceId(ToString(DataShard.TabletId))
        , Serializer(CreateChangeRecordSerializer({
            .StreamFormat = stream.Format,
            .StreamMode = stream.Mode,
            .AwsRegion = stream.AwsRegion.GetOrElse(AppData()->AwsCompatibilityConfig.GetAwsRegion()),
            .VirtualTimestamps = stream.VirtualTimestamps,
            .ShardId = DataShard.TabletId,
        }))
    {
    }

    void Bootstrap() {
        Init();
    }

    STATEFN(StateBase) {
        switch (ev->GetTypeRewrite()) {
            sFunc(TEvPartitionWriter::TEvDisconnected, Disconnected);
            hFunc(NMon::TEvRemoteHttpInfo, Handle);
            sFunc(TEvents::TEvWakeup, Init);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    const TActorId Parent;
    const TDataShardId DataShard;
    const ui32 PartitionId;
    const ui64 ShardId;
    const TString SourceId;
    THolder<IChangeRecordSerializer> Serializer;
    mutable TMaybe<TString> LogPrefix;

    TActorId Writer;
    i64 MaxSeqNo = 0;
    TVector<i64> Pending;
    ui64 Cookie = 0;

}; // TCdcChangeSenderPartition

class TMd5Partitioner final : public NChangeExchange::IChangeSenderPartitioner<TChangeRecord> {
public:
    TMd5Partitioner(size_t partitionCount)
        : PartitionCount(partitionCount) {
    }

    ui64 ResolvePartitionId(const typename TChangeRecord::TPtr& record) const override {
        using namespace NKikimr::NDataStreams::V1;
        const auto hashKey = HexBytesToDecimal(record->GetPartitionKey() /* MD5 */);
        return ShardFromDecimal(hashKey, PartitionCount);
    }

private:
    size_t PartitionCount;
};

class TBoundaryPartitioner final : public NChangeExchange::IChangeSenderPartitioner<TChangeRecord> {
public:
    TBoundaryPartitioner(const NKikimrSchemeOp::TPersQueueGroupDescription& config) {
        Chooser = NPQ::CreatePartitionChooser(config);
    }

    ui64 ResolvePartitionId(const typename TChangeRecord::TPtr& record) const override {
        auto* p = Chooser->GetPartition(record->GetPartitionKey());
        return p->TabletId;
    }

private:
    std::shared_ptr<NPQ::IPartitionChooser> Chooser;
};

class TCdcChangeSenderMain
    : public TActorBootstrapped<TCdcChangeSenderMain>
    , public NChangeExchange::TBaseChangeSender<TChangeRecord>
    , public NChangeExchange::IChangeSenderResolver
    , public NChangeExchange::ISenderFactory
    , private NSchemeCache::TSchemeCacheHelpers
{
    struct TPQPartitionInfo {
        ui32 PartitionId;
        ui64 ShardId;
        TPartitionKeyRange KeyRange;

        struct TLess {
            TConstArrayRef<NScheme::TTypeInfo> Schema;

            TLess(const TVector<NScheme::TTypeInfo>& schema)
                : Schema(schema)
            {
            }

            bool operator()(const TPQPartitionInfo& lhs, const TPQPartitionInfo& rhs) const {
                Y_ABORT_UNLESS(lhs.KeyRange.ToBound || rhs.KeyRange.ToBound);

                if (!lhs.KeyRange.ToBound) {
                    return false;
                }

                if (!rhs.KeyRange.ToBound) {
                    return true;
                }

                Y_ABORT_UNLESS(lhs.KeyRange.ToBound && rhs.KeyRange.ToBound);

                const int compares = CompareTypedCellVectors(
                    lhs.KeyRange.ToBound->GetCells().data(),
                    rhs.KeyRange.ToBound->GetCells().data(),
                    Schema.data(), Schema.size()
                );

                return (compares < 0);
            }

        }; // TLess

    }; // TPQPartitionInfo

    TStringBuf GetLogPrefix() const {
        if (!LogPrefix) {
            LogPrefix = TStringBuilder()
                << "[CdcChangeSenderMain]"
                << "[" << DataShard.TabletId << ":" << DataShard.Generation << "]"
                << SelfId() /* contains brackets */ << " ";
        }

        return LogPrefix.GetRef();
    }

    bool IsResolvingCdcStream() const {
        return CurrentStateFunc() == static_cast<TReceiveFunc>(&TThis::StateResolveCdcStream);
    }

    bool IsResolvingTopic() const {
        return CurrentStateFunc() == static_cast<TReceiveFunc>(&TThis::StateResolveTopic);
    }

    bool IsResolving() const override {
        return IsResolvingCdcStream()
            || IsResolvingTopic();
    }

    TStringBuf CurrentStateName() const {
        if (IsResolvingCdcStream()) {
            return "ResolveCdcStream";
        } else if (IsResolvingTopic()) {
            return "ResolveTopic";
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

    bool CheckNotEmpty(const TAutoPtr<TNavigate>& result) {
        return Check(&TSchemeCacheHelpers::CheckNotEmpty<TNavigate>, &TThis::LogCritAndRetry, result);
    }

    bool CheckEntriesCount(const TAutoPtr<TNavigate>& result, ui32 expected) {
        return Check(&TSchemeCacheHelpers::CheckEntriesCount<TNavigate>, &TThis::LogCritAndRetry, result, expected);
    }

    bool CheckTableId(const TNavigate::TEntry& entry, const TTableId& expected) {
        return Check(&TSchemeCacheHelpers::CheckTableId<TNavigate::TEntry>, &TThis::LogCritAndRetry, entry, expected);
    }

    bool CheckEntrySucceeded(const TNavigate::TEntry& entry) {
        return Check(&TSchemeCacheHelpers::CheckEntrySucceeded<TNavigate::TEntry>, &TThis::LogWarnAndRetry, entry);
    }

    bool CheckEntryKind(const TNavigate::TEntry& entry, TNavigate::EKind expected) {
        return Check(&TSchemeCacheHelpers::CheckEntryKind<TNavigate::TEntry>, &TThis::LogWarnAndRetry, entry, expected);
    }

    bool CheckNotEmpty(const TIntrusiveConstPtr<TNavigate::TCdcStreamInfo>& streamInfo) {
        if (streamInfo) {
            return true;
        }

        LogCritAndRetry(TStringBuilder() << "Empty stream info at '" << CurrentStateName() << "'");
        return false;
    }

    bool CheckNotEmpty(const TIntrusiveConstPtr<TNavigate::TPQGroupInfo>& pqInfo) {
        if (pqInfo) {
            return true;
        }

        LogCritAndRetry(TStringBuilder() << "Empty pq info at '" << CurrentStateName() << "'");
        return false;
    }

    static TVector<ui64> MakePartitionIds(const TVector<NKikimr::TKeyDesc::TPartitionInfo>& partitions) {
        TVector<ui64> result(Reserve(partitions.size()));

        for (const auto& partition : partitions) {
            result.push_back(partition.ShardId);
        }

        return result;
    }

    /// ResolveCdcStream

    void ResolveCdcStream() {
        auto request = MakeHolder<TNavigate>();
        request->ResultSet.emplace_back(MakeNavigateEntry(PathId, TNavigate::OpList));

        Send(MakeSchemeCacheID(), new TEvNavigate(request.Release()));
        Become(&TThis::StateResolveCdcStream);
    }

    STATEFN(StateResolveCdcStream) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleCdcStream);
            sFunc(TEvents::TEvWakeup, ResolveCdcStream);
        default:
            return StateBase(ev);
        }
    }

    void HandleCdcStream(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& result = ev->Get()->Request;

        LOG_D("Handle TEvTxProxySchemeCache::TEvNavigateKeySetResult"
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

        if (!CheckEntryKind(entry, TNavigate::KindCdcStream)) {
            return;
        }

        if (!CheckNotEmpty(entry.CdcStreamInfo)) {
            return;
        }

        if (entry.Self && entry.Self->Info.GetPathState() == NKikimrSchemeOp::EPathStateDrop) {
            LOG_D("Stream is planned to drop, waiting for the EvRemoveSender command");

            RemoveRecords();
            KillSenders();
            return Become(&TThis::StatePendingRemove);
        }

        Stream = TUserTable::TCdcStream(entry.CdcStreamInfo->Description);

        Y_ABORT_UNLESS(entry.ListNodeEntry->Children.size() == 1);
        const auto& topic = entry.ListNodeEntry->Children.at(0);

        Y_ABORT_UNLESS(topic.Kind == TNavigate::KindTopic);
        TopicPathId = topic.PathId;

        ResolveTopic();
    }

    /// ResolveTopic

    void ResolveTopic() {
        auto request = MakeHolder<TNavigate>();
        request->ResultSet.emplace_back(MakeNavigateEntry(TopicPathId, TNavigate::OpTopic));

        Send(MakeSchemeCacheID(), new TEvNavigate(request.Release()));
        Become(&TThis::StateResolveTopic);
    }

    STATEFN(StateResolveTopic) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleTopic);
            sFunc(TEvents::TEvWakeup, ResolveCdcStream);
        default:
            return StateBase(ev);
        }
    }

    void HandleTopic(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& result = ev->Get()->Request;

        LOG_D("Handle TEvTxProxySchemeCache::TEvNavigateKeySetResult"
            << ": result# " << (result ? result->ToString(*AppData()->TypeRegistry) : "nullptr"));

        if (!CheckNotEmpty(result)) {
            return;
        }

        if (!CheckEntriesCount(result, 1)) {
            return;
        }

        const auto& entry = result->ResultSet.at(0);

        if (!CheckTableId(entry, TopicPathId)) {
            return;
        }

        if (!CheckEntrySucceeded(entry)) {
            return;
        }

        if (!CheckEntryKind(entry, TNavigate::KindTopic)) {
            return;
        }

        if (!CheckNotEmpty(entry.PQGroupInfo)) {
            return;
        }

        const auto& pqDesc = entry.PQGroupInfo->Description;
        const auto& pqConfig = pqDesc.GetPQTabletConfig();

        TVector<NScheme::TTypeInfo> schema;
        PartitionToShard.clear();

        schema.reserve(pqConfig.PartitionKeySchemaSize());
        for (const auto& keySchema : pqConfig.GetPartitionKeySchema()) {
            // TODO: support pg types
            schema.push_back(NScheme::TTypeInfo(keySchema.GetTypeId()));
        }

        TSet<TPQPartitionInfo, TPQPartitionInfo::TLess> partitions(schema);

        for (const auto& partition : pqDesc.GetPartitions()) {
            const auto partitionId = partition.GetPartitionId();
            const auto shardId = partition.GetTabletId();

            PartitionToShard.emplace(partitionId, shardId);

            auto keyRange = TPartitionKeyRange::Parse(partition.GetKeyRange());
            Y_ABORT_UNLESS(!keyRange.FromBound || keyRange.FromBound->GetCells().size() == schema.size());
            Y_ABORT_UNLESS(!keyRange.ToBound || keyRange.ToBound->GetCells().size() == schema.size());

            partitions.insert({partitionId, shardId, std::move(keyRange)});
        }

        // used to validate
        bool isFirst = true;
        const TPQPartitionInfo* prev = nullptr;

        TVector<NKikimr::TKeyDesc::TPartitionInfo> partitioning;
        partitioning.reserve(partitions.size());
        for (const auto& cur : partitions) {
            if (isFirst) {
                isFirst = false;
                Y_ABORT_UNLESS(!cur.KeyRange.FromBound.Defined());
            } else {
                Y_ABORT_UNLESS(cur.KeyRange.FromBound.Defined());
                Y_ABORT_UNLESS(prev);
                Y_ABORT_UNLESS(prev->KeyRange.ToBound.Defined());
                // TODO: compare cells
            }

            auto& part = partitioning.emplace_back(cur.PartitionId); // TODO: double-check that it is right partitioning

            if (cur.KeyRange.ToBound) {
                part.Range = NKikimr::TKeyDesc::TPartitionRangeInfo{
                    .EndKeyPrefix = *cur.KeyRange.ToBound,
                };
            } else {
                part.Range = NKikimr::TKeyDesc::TPartitionRangeInfo{};
            }

            prev = &cur;
        }

        if (prev) {
            Y_ABORT_UNLESS(!prev->KeyRange.ToBound.Defined());
        }

        const auto topicVersion = entry.Self->Info.GetVersion().GetGeneralVersion();
        const bool versionChanged = !TopicVersion || TopicVersion != topicVersion;
        TopicVersion = topicVersion;

        KeyDesc = NKikimr::TKeyDesc::CreateMiniKeyDesc(schema);
        KeyDesc->Partitioning = std::make_shared<TVector<NKikimr::TKeyDesc::TPartitionInfo>>(std::move(partitioning));

        if (::NKikimrPQ::TPQTabletConfig::TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_DISABLED != pqConfig.GetPartitionStrategy().GetPartitionStrategyType()) {
            SetPartitioner(new TBoundaryPartitioner(pqDesc));
        } else if (NKikimrSchemeOp::ECdcStreamFormatProto == Stream.Format) {
            SetPartitioner(NChangeExchange::CreateSchemaBoundaryPartitioner<TChangeRecord>(*KeyDesc.Get()));
        } else {
            SetPartitioner(new TMd5Partitioner(KeyDesc->GetPartitions().size()));
        }

        CreateSenders(MakePartitionIds(*KeyDesc->Partitioning), versionChanged);
        Become(&TThis::StateMain);
    }

    /// Main

    STATEFN(StateMain) {
        return StateBase(ev);
    }

    void Resolve() override {
        ResolveCdcStream();
    }

    bool IsResolved() const override {
        return KeyDesc && KeyDesc->Partitioning;
    }

    IActor* CreateSender(ui64 partitionId) const override {
        Y_ABORT_UNLESS(PartitionToShard.contains(partitionId));
        const auto shardId = PartitionToShard.at(partitionId);
        return new TCdcChangeSenderPartition(SelfId(), DataShard, partitionId, shardId, Stream);
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
        return NKikimrServices::TActivity::CHANGE_SENDER_CDC_ACTOR_MAIN;
    }

    explicit TCdcChangeSenderMain(const TDataShardId& dataShard, const TPathId& streamPathId)
        : TActorBootstrapped()
        , TBaseChangeSender(this, this, this, dataShard.ActorId, streamPathId)
        , DataShard(dataShard)
        , TopicVersion(0)
    {
    }

    void Bootstrap() {
        ResolveCdcStream();
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
    mutable TMaybe<TString> LogPrefix;

    TUserTable::TCdcStream Stream;
    TPathId TopicPathId;
    ui64 TopicVersion;
    THolder<NKikimr::TKeyDesc> KeyDesc;
    THashMap<ui32, ui64> PartitionToShard;

}; // TCdcChangeSenderMain

IActor* CreateCdcStreamChangeSender(const TDataShardId& dataShard, const TPathId& streamPathId) {
    return new TCdcChangeSenderMain(dataShard, streamPathId);
}

}
