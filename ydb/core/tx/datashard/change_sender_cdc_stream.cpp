#include "change_exchange.h"
#include "change_exchange_impl.h"
#include "change_record.h"
#include "change_record_cdc_serializer.h"
#include "datashard_user_table.h"

#include <ydb/core/change_exchange/change_sender.h>
#include <ydb/core/change_exchange/change_sender_monitoring.h>
#include <ydb/core/change_exchange/util.h>
#include <ydb/core/persqueue/writer/source_id_encoding.h>
#include <ydb/core/persqueue/writer/writer.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/tx/cdc_aggregator/public/events.h>
#include <ydb/core/tx/scheme_cache/helpers.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/util/backoff.h>
#include <ydb/library/aclib/user_context.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/services/lib/sharding/sharding.h>

#include <library/cpp/json/json_writer.h>

#include <optional>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CHANGE_EXCHANGE

namespace NKikimr::NDataShard {

using namespace NPQ;
using ESenderType = TEvChangeExchange::ESenderType;

class TSerializer: public NChangeExchange::TBaseVisitor {
    IChangeRecordSerializer& Serializer;
    NKikimrClient::TPersQueuePartitionRequest::TCmdWrite& Cmd;

public:
    explicit TSerializer(IChangeRecordSerializer& serializer, NKikimrClient::TPersQueuePartitionRequest::TCmdWrite& cmd)
        : Serializer(serializer)
        , Cmd(cmd)
    {
    }

    void Visit(const TChangeRecord& record) override {
        Serializer.Serialize(Cmd, record);
    }
};

static IChangeRecordSerializer* CreateCdcStreamSerializer(const TUserTable::TCdcStream& stream, ui64 shardId) {
    return CreateChangeRecordSerializer({
        .StreamFormat = stream.Format,
        .StreamMode = stream.Mode,
        .AwsRegion = stream.AwsRegion.GetOrElse(AppData()->AwsCompatibilityConfig.GetAwsRegion()),
        .VirtualTimestamps = stream.VirtualTimestamps,
        .ShardId = shardId,
        .UserSIDs = stream.UserSIDs,
        .TraceIds = stream.TraceIds,
        .Compression = stream.Compression,
        .CompressionLevel = stream.CompressionLevel,
    });
}

class TCdcChangeSenderPartition: public TActorBootstrapped<TCdcChangeSenderPartition> {

    NActors::NStructuredLog::TStructuredMessage GetLogPrefix() const {
        return YDB_LOG_CREATE_MESSAGE(
            {"actorClassName", "CdcChangeSenderPartition"},
            {"selfId", SelfId()},
            {"tabletId", DataShard.TabletId},
            {"generation", DataShard.Generation},
            {"partitionId", PartitionId},
            {"shardId", ShardId});
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
        YDB_LOG_CREATE_CONTEXT(GetLogPrefix(),
            {"actorState", "StateInit"});
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPartitionWriter::TEvInitResult, Handle);
        default:
            return StateBase(ev);
        }
    }

    void Handle(TEvPartitionWriter::TEvInitResult::TPtr& ev) {
        YDB_LOG_DEBUG("Handle",
            {"eventDetails", ev->Get()->ToString()});

        const auto& result = *ev->Get();
        if (!result.IsSuccess()) {
            YDB_LOG_ERROR("CDC partition writer initialization failed",
                {"initErrorMessage", result.GetError()});
            return Leave();
        }

        const auto& info = result.GetResult().SourceIdInfo;
        Y_ENSURE(info.GetExplicit());

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
        YDB_LOG_CREATE_CONTEXT(GetLogPrefix(),
            {"actorState", "StateWaitingRecords"});
        switch (ev->GetTypeRewrite()) {
            hFunc(NChangeExchange::TEvChangeExchange::TEvRecords, Handle);
            sFunc(TEvPartitionWriter::TEvWriteResponse, Lost);
        default:
            return StateBase(ev);
        }
    }

    void Handle(NChangeExchange::TEvChangeExchange::TEvRecords::TPtr& ev) {
        YDB_LOG_DEBUG("Handle",
            {"eventDetails", ev->Get()->ToString()});
        NKikimrClient::TPersQueueRequest request;

        for (auto recordPtr : ev->Get()->Records) {
            const auto& record = *recordPtr;
            const auto seqNo = static_cast<i64>(record.GetOrder());

            if (seqNo <= MaxSeqNo) {
                continue;
            }

            auto& cmd = *request.MutablePartitionRequest()->AddCmdWrite();
            cmd.SetSourceId(NSourceIdEncoding::EncodeSimple(SourceId));
            cmd.SetIgnoreQuotaDeadline(true);

            TSerializer serializer(*Serializer, cmd);
            record.Accept(serializer);

            Pending.push_back(seqNo);
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
        YDB_LOG_CREATE_CONTEXT(GetLogPrefix(),
            {"actorState", "StateWrite"});
        switch (ev->GetTypeRewrite()) {
            IgnoreFunc(TEvPartitionWriter::TEvWriteAccepted);
            hFunc(TEvPartitionWriter::TEvWriteResponse, Handle);
        default:
            return StateBase(ev);
        }
    }

    void Handle(TEvPartitionWriter::TEvWriteResponse::TPtr& ev) {
        YDB_LOG_DEBUG("Handle",
            {"eventDetails", ev->Get()->ToString()});

        const auto& result = *ev->Get();
        if (!result.IsSuccess()) {
            YDB_LOG_ERROR("CDC partition writer write failed",
                {"writeErrorMessage", result.DumpError()});
            return Leave();
        }

        const auto& response = result.Record.GetPartitionResponse();
        if (response.GetCookie() != Cookie) {
            YDB_LOG_ERROR("PersQueue write cookie mismatch",
                {"expectedCookie", Cookie},
                {"actualCookie", response.GetCookie()});
            return Leave();
        }

        if (response.CmdWriteResultSize() != Pending.size()) {
            YDB_LOG_ERROR("PersQueue write result count mismatch",
                {"expectedCount", Pending.size()},
                {"actualCount", response.CmdWriteResultSize()});
            return Leave();
        }

        for (ui32 i = 0; i < Pending.size(); ++i) {
            const auto expected = Pending.at(i);
            const auto got = response.GetCmdWriteResult(i).GetSeqNo();

            if (expected == got) {
                MaxSeqNo = Max(MaxSeqNo, got);
                continue;
            }

            YDB_LOG_ERROR("PersQueue sequence number mismatch",
                {"expectedSeqNo", expected},
                {"actualSeqNo", got});
            return Leave();
        }

        Ready();
    }

    void Handle(NMon::TEvRemoteHttpInfo::TPtr& ev) {
        using namespace NChangeExchange;

        const auto tabletAppPath = AppData()->FeatureFlags.GetEnableTabletDevUiSecurePath()
            ? ETabletAppPath::Secure
            : ETabletAppPath::Plain;

        TStringStream html;

        HTML(html) {
            Header(html, "CdcStream partition change sender", DataShard.TabletId, tabletAppPath);

            SimplePanel(html, "Info", [this, tabletAppPath](IOutputStream& html) {
                HTML(html) {
                    DL_CLASS("dl-horizontal") {
                        TermDesc(html, "PartitionId", PartitionId);
                        TermDescLink(html, "ShardId", ShardId, TabletPath(ShardId, tabletAppPath));
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
        YDB_LOG_DEBUG("PersQueue pipe disconnected during initialization");

        if (CurrentStateFunc() != static_cast<TReceiveFunc>(&TThis::StateInit)) {
            return Leave();
        }

        CloseWriter();
        Schedule(TDuration::MilliSeconds(100), new TEvents::TEvWakeup());
    }

    void Lost() {
        YDB_LOG_WARN("CDC stream partition change sender lost");
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
        , Serializer(CreateCdcStreamSerializer(stream, DataShard.TabletId))
    {
    }

    void Bootstrap() {
        YDB_LOG_CREATE_CONTEXT(GetLogPrefix());
        Init();
    }

    STATEFN(StateBase) {
        YDB_LOG_CREATE_CONTEXT(GetLogPrefix(),
            {"actorState", "StateBase"});
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

    TActorId Writer;
    i64 MaxSeqNo = 0;
    TVector<i64> Pending;
    ui64 Cookie = 0;

}; // TCdcChangeSenderPartition

class TMd5PartitionResolver final: public NChangeExchange::TBasePartitionResolver {
public:
    TMd5PartitionResolver(size_t partitionCount)
        : PartitionCount(partitionCount)
    {
    }

    void Visit(const TChangeRecord& record) override {
        using namespace NKikimr::NDataStreams::V1;
        const auto hashKey = HexBytesToDecimal(record.GetPartitionKey() /* MD5 */);
        SetPartitionId(ShardFromDecimal(hashKey, PartitionCount));
    }

private:
    size_t PartitionCount;
};

class TBoundaryPartitionResolver final: public NChangeExchange::TBasePartitionResolver {
public:
    TBoundaryPartitionResolver(const std::shared_ptr<NPQ::IPartitionChooser>& chooser) {
        Chooser = chooser;
    }

    void Visit(const TChangeRecord& record) override {
        auto* p = Chooser->GetPartition(record.GetPartitionKey());
        SetPartitionId(p->PartitionId);
    }

private:
    std::shared_ptr<NPQ::IPartitionChooser> Chooser;
};

class TCdcChangeSenderMain
    : public TActorBootstrapped<TCdcChangeSenderMain>
    , public NChangeExchange::TChangeSender
    , public NChangeExchange::IChangeSenderIdentity
    , public NChangeExchange::IChangeSenderPathResolver
    , public NChangeExchange::IChangeSenderFactory
    , private NSchemeCache::TSchemeCacheHelpers
{
    NActors::NStructuredLog::TStructuredMessage GetLogPrefix() const {
        return YDB_LOG_CREATE_MESSAGE(
            {"actorClassName", "CdcChangeSenderMain"},
            {"selfId", SelfId()},
            {"tabletId", DataShard.TabletId});
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
        YDB_LOG_CRIT("Critical error during change exchange operation, retrying",
            {"errorMessage", error});
        Retry();
    }

    void LogWarnAndRetry(const TString& error) {
        YDB_LOG_WARN("Recoverable error during change exchange operation, retrying",
            {"errorMessage", error});
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

    /// ResolveCdcStream

    void ResolveCdcStream() {
        auto request = MakeHolder<TNavigate>();
        request->ResultSet.emplace_back(MakeNavigateEntry(StreamPathId, TNavigate::OpList));

        Send(MakeSchemeCacheID(), new TEvNavigate(request.Release()));
        Become(&TThis::StateResolveCdcStream);
    }

    STATEFN(StateResolveCdcStream) {
        YDB_LOG_CREATE_CONTEXT(GetLogPrefix(),
            {"actorState", "StateResolveCdcStream"});
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleCdcStream);
            sFunc(TEvents::TEvWakeup, ResolveCdcStream);
        default:
            return StateBase(ev);
        }
    }

    void HandleCdcStream(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& result = ev->Get()->Request;

        YDB_LOG_DEBUG("Handle TEvTxProxySchemeCache::TEvNavigateKeySetResult",
            {"navigateResult", (result ? result->ToString(*AppData()->TypeRegistry) : "nullptr")});

        if (!CheckNotEmpty(result)) {
            return;
        }

        if (!CheckEntriesCount(result, 1)) {
            return;
        }

        const auto& entry = result->ResultSet.at(0);

        if (!CheckTableId(entry, StreamPathId)) {
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
            YDB_LOG_DEBUG("CDC stream is planned to drop, waiting for remove sender command");

            RemoveRecords();
            RemoveHeartbeats(Heartbeats.end());
            KillSenders();
            return Become(&TThis::StatePendingRemove);
        }

        Stream = TUserTable::TCdcStream(entry.CdcStreamInfo->Description);

        if (Stream.HeartbeatMode == NKikimrSchemeOp::ECdcStreamHeartbeatModeAggregated && !Stream.AggregatorTabletId) {
            YDB_LOG_ERROR("CDC stream has aggregated heartbeat mode without aggregator tablet, using direct heartbeats");
        }

        if (!IsHeartbeatAggregated()) {
            RemoveHeartbeats(Heartbeats.end());
            Unconfirmed.reset();
        }

        Y_ENSURE(entry.ListNodeEntry->Children.size() == 1);
        const auto& topic = entry.ListNodeEntry->Children.at(0);

        Y_ENSURE(topic.Kind == TNavigate::KindTopic);
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
        YDB_LOG_CREATE_CONTEXT(GetLogPrefix(),
            {"actorState", "StateResolveTopic"});
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleTopic);
            sFunc(TEvents::TEvWakeup, ResolveCdcStream);
        default:
            return StateBase(ev);
        }
    }

    void HandleTopic(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& result = ev->Get()->Request;

        YDB_LOG_DEBUG("Handle TEvTxProxySchemeCache::TEvNavigateKeySetResult",
            {"navigateResult", (result ? result->ToString(*AppData()->TypeRegistry) : "nullptr")});

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

        const auto topicVersion = entry.Self->Info.GetVersion().GetGeneralVersion();
        if (TopicVersion && TopicVersion == topicVersion) {
            CreateSenders();
            return Become(&TThis::StateMain);
        }

        TopicVersion = topicVersion;

        const auto& pqDesc = entry.PQGroupInfo->Description;
        const auto& pqConfig = pqDesc.GetPQTabletConfig();

        PartitionToShard.clear();
        for (const auto& partition : pqDesc.GetPartitions()) {
            if (NKikimrPQ::ETopicPartitionStatus::Active == partition.GetStatus()) {
                PartitionToShard.emplace(partition.GetPartitionId(), partition.GetTabletId());
            }
        }

        const bool topicAutoPartitioning = IsTopicAutoPartitioningEnabled(pqConfig.GetPartitionStrategy().GetPartitionStrategyType());
        if (!topicAutoPartitioning && !entry.PQGroupInfo->Schema) {
            return LogWarnAndRetry("Empty schema and disabled auto-partitioning");
        }

        KeyDesc = NKikimr::TKeyDesc::CreateMiniKeyDesc(entry.PQGroupInfo->Schema);
        Y_ENSURE(entry.PQGroupInfo->Partitioning);
        KeyDesc->Partitioning = std::make_shared<TPartitioning>(entry.PQGroupInfo->Partitioning);

        if (topicAutoPartitioning) {
            Y_ENSURE(entry.PQGroupInfo->PartitionChooser);
            SetPartitionResolver(new TBoundaryPartitionResolver(entry.PQGroupInfo->PartitionChooser));
        } else if (NKikimrSchemeOp::ECdcStreamFormatProto == Stream.Format) {
            SetPartitionResolver(CreateDefaultPartitionResolver(*KeyDesc.Get()));
        } else {
            SetPartitionResolver(new TMd5PartitionResolver(KeyDesc->GetPartitions().size()));
        }

        CreateSenders(NChangeExchange::MakePartitionIds(KeyDesc->GetPartitions()));
        Become(&TThis::StateMain);
    }

    static bool IsTopicAutoPartitioningEnabled(NKikimrPQ::TPQTabletConfig::TPartitionStrategyType strategy) {
        return strategy != NKikimrPQ::TPQTabletConfig::TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_DISABLED;
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
        Y_ENSURE(PartitionToShard.contains(partitionId));
        const auto shardId = PartitionToShard.at(partitionId);
        return new TCdcChangeSenderPartition(SelfId(), DataShard, partitionId, shardId, Stream);
    }

    void Handle(NChangeExchange::TEvChangeExchange::TEvEnqueueRecords::TPtr& ev) {
        YDB_LOG_DEBUG("Handle",
            {"eventDetails", ev->Get()->ToString()});
        for (const auto& record : ev->Get()->Records) {
            Undelivered.insert(record.Order);
        }
        EnqueueRecords(std::move(ev->Get()->Records));
    }

    void Handle(NChangeExchange::TEvChangeExchange::TEvRecords::TPtr& ev) {
        YDB_LOG_DEBUG("Handle",
            {"eventDetails", ev->Get()->ToString()});
        TVector<NChangeExchange::IChangeRecord::TPtr> records;
        for (auto& record : ev->Get()->Records) {
            if (record->GetKind() == NChangeExchange::IChangeRecord::EKind::CdcHeartbeat && IsHeartbeatAggregated()) {
                HoldHeartbeat(*record);
            } else {
                records.push_back(std::move(record));
            }
        }
        ProcessRecords(std::move(records));
        SendReadyHeartbeat();
    }

    void Handle(NChangeExchange::TEvChangeExchange::TEvForgetRecords::TPtr& ev) {
        YDB_LOG_DEBUG("Handle",
            {"eventDetails", ev->Get()->ToString()});
        for (const auto order : ev->Get()->Records) {
            Undelivered.erase(order);
        }
        ForgetRecords(std::move(ev->Get()->Records));
        SendReadyHeartbeat();
    }

    void Handle(NChangeExchange::TEvChangeExchangePrivate::TEvReady::TPtr& ev) {
        YDB_LOG_DEBUG("Handle",
            {"eventDetails", ev->Get()->ToString()});
        OnReady(ev->Get()->PartitionId);
        SendReadyHeartbeat();
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
        RemoveHeartbeats(Heartbeats.end());
        PassAway();
    }

    bool IsHeartbeatAggregated() const {
        return Stream.HeartbeatMode == NKikimrSchemeOp::ECdcStreamHeartbeatModeAggregated
            && Stream.AggregatorTabletId;
    }

    void HoldHeartbeat(const NChangeExchange::IChangeRecord& record) {
        NKikimrClient::TPersQueuePartitionRequest::TCmdWrite cmd;
        THolder<IChangeRecordSerializer> serializer(CreateCdcStreamSerializer(Stream, DataShard.TabletId));
        TSerializer visitor(*serializer, cmd);
        record.Accept(visitor);

        Undelivered.erase(record.GetOrder());
        Heartbeats.emplace(record.GetOrder(), NPQ::THeartbeat::Parse(cmd.GetHeartbeat()));
        ForgetRecords({record.GetOrder()});
    }

    void OnRecordsRemoved(const TVector<ui64>& orders) override {
        for (const auto order : orders) {
            Undelivered.erase(order);
        }
    }

    void SendReadyHeartbeat() {
        const auto ready = Undelivered ? Heartbeats.lower_bound(*Undelivered.begin()) : Heartbeats.end();
        if (ready == Heartbeats.begin()) {
            return;
        }

        Unconfirmed = std::prev(ready)->second;
        RemoveHeartbeats(ready);
        ScheduleWriteHeartbeat(TDuration::Zero());
    }

    void ScheduleWriteHeartbeat(TDuration delay) {
        auto ev = MakeHolder<NCdcAggregator::TEvCdcAggregatorService::TEvWriteHeartbeat>();
        ev->RecipientTabletId = Stream.AggregatorTabletId;
        ev->SourceDataShardId = DataShard.TabletId;
        ev->Heartbeat = *Unconfirmed;

        auto* handle = new IEventHandle(NCdcAggregator::MakeCdcAggregatorServiceId(SelfId().NodeId()), SelfId(),
            ev.Release(), IEventHandle::FlagTrackDelivery, ++HeartbeatCookie);
        if (delay) {
            TActivationContext::Schedule(delay, handle);
        } else {
            TActivationContext::Send(handle);
        }
    }

    void RemoveHeartbeats(TMap<ui64, NPQ::THeartbeat>::iterator end) {
        TVector<ui64> remove;
        for (auto it = Heartbeats.begin(); it != end; it = Heartbeats.erase(it)) {
            remove.push_back(it->first);
        }

        if (remove) {
            RemoveRecords(std::move(remove));
        }
    }

    void Handle(NCdcAggregator::TEvCdcAggregatorService::TEvWriteHeartbeatResult::TPtr& ev) {
        YDB_LOG_DEBUG("Handle",
            {"eventDetails", ev->Get()->ToString()});

        using EStatus = NCdcAggregator::TEvCdcAggregatorService::TEvWriteHeartbeatResult::EStatus;
        const auto& result = *ev->Get();
        if (!Unconfirmed || ev->Cookie != HeartbeatCookie) {
            return;
        }

        if (result.Status == EStatus::Written && result.WrittenVersion >= Unconfirmed->Version) {
            Unconfirmed.reset();
            HeartbeatBackoff.Reset();
            return;
        }

        YDB_LOG_WARN("CdcAggregatorService has not written the heartbeat, retrying",
            {"eventDetails", result.ToString()});
        ScheduleWriteHeartbeat(result.Status == EStatus::Rejected ? RejectedHeartbeatRetryDelay : HeartbeatBackoff.Next());
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        if (Unconfirmed && ev->Cookie == HeartbeatCookie) {
            ScheduleWriteHeartbeat(HeartbeatBackoff.Next());
        }
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
        return NKikimrServices::TActivity::CHANGE_SENDER_CDC_ACTOR_MAIN;
    }

    explicit TCdcChangeSenderMain(const TDataShardId& dataShard, const TPathId& streamPathId)
        : TActorBootstrapped()
        , TChangeSender(this, this, this, this, dataShard.ActorId)
        , StreamPathId(streamPathId)
        , DataShard(dataShard)
        , TopicVersion(0)
    {
    }

    TPathId GetChangeSenderIdentity() const override final {
        return StreamPathId;
    }

    void Bootstrap() {
        ResolveCdcStream();
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
            hFunc(NCdcAggregator::TEvCdcAggregatorService::TEvWriteHeartbeatResult, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);
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

private:
    const TPathId StreamPathId;
    const TDataShardId DataShard;

    TUserTable::TCdcStream Stream;
    TPathId TopicPathId;
    ui64 TopicVersion;
    THolder<NKikimr::TKeyDesc> KeyDesc;
    THashMap<ui32, ui64> PartitionToShard;

    static constexpr TDuration RejectedHeartbeatRetryDelay = TDuration::Minutes(1);

    TSet<ui64> Undelivered;
    TMap<ui64, NPQ::THeartbeat> Heartbeats;
    std::optional<NPQ::THeartbeat> Unconfirmed;
    ui64 HeartbeatCookie = 0;
    TBackoff HeartbeatBackoff = TBackoff(TDuration::MilliSeconds(100), TDuration::Seconds(10));

}; // TCdcChangeSenderMain

IActor* CreateCdcStreamChangeSender(const TDataShardId& dataShard, const TPathId& streamPathId) {
    return new TCdcChangeSenderMain(dataShard, streamPathId);
}

}


#undef YDB_LOG_THIS_FILE_COMPONENT
