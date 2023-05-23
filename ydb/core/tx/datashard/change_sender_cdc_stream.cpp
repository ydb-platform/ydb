#include "change_exchange.h"
#include "change_exchange_impl.h"
#include "change_sender_common_ops.h"
#include "change_sender_monitoring.h"
#include "datashard_user_table.h"

#include <ydb/core/persqueue/partition_key_range/partition_key_range.h>
#include <ydb/core/persqueue/writer/source_id_encoding.h>
#include <ydb/core/persqueue/writer/writer.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/services/lib/sharding/sharding.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>
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
        Y_VERIFY(info.GetExplicit());

        MaxSeqNo = info.GetSeqNo();
        Ready();
    }

    void Ready() {
        Pending.clear();

        Send(Parent, new TEvChangeExchangePrivate::TEvReady(PartitionId));
        Become(&TThis::StateWaitingRecords);
    }

    /// WaitingRecords

    STATEFN(StateWaitingRecords) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvChangeExchange::TEvRecords, Handle);
            sFunc(TEvPartitionWriter::TEvWriteResponse, Lost);
        default:
            return StateBase(ev);
        }
    }

    void Handle(TEvChangeExchange::TEvRecords::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());
        NKikimrClient::TPersQueueRequest request;

        const auto awsJsonOpts = TChangeRecord::TAwsJsonOptions{
            .AwsRegion = Stream.AwsRegion.GetOrElse(AppData()->AwsCompatibilityConfig.GetAwsRegion()),
            .StreamMode = Stream.Mode,
            .ShardId = DataShard.TabletId,
        };

        for (const auto& record : ev->Get()->Records) {
            if (record.GetSeqNo() <= MaxSeqNo) {
                continue;
            }

            auto& cmd = *request.MutablePartitionRequest()->AddCmdWrite();
            cmd.SetSeqNo(record.GetSeqNo());
            cmd.SetSourceId(NSourceIdEncoding::EncodeSimple(SourceId));
            cmd.SetCreateTimeMS(record.GetApproximateCreationDateTime().MilliSeconds());
            cmd.SetIgnoreQuotaDeadline(true);

            NKikimrPQClient::TDataChunk data;
            data.SetCodec(0 /* CODEC_RAW */);

            switch (Stream.Format) {
                case NKikimrSchemeOp::ECdcStreamFormatProto: {
                    NKikimrChangeExchange::TChangeRecord protoRecord;
                    record.SerializeToProto(protoRecord);
                    data.SetData(protoRecord.SerializeAsString());
                    break;
                }

                case NKikimrSchemeOp::ECdcStreamFormatJson:
                case NKikimrSchemeOp::ECdcStreamFormatDynamoDBStreamsJson: {
                    NJson::TJsonValue json;
                    if (Stream.Format == NKikimrSchemeOp::ECdcStreamFormatDynamoDBStreamsJson) {
                        record.SerializeToDynamoDBStreamsJson(json, awsJsonOpts);
                    } else {
                        record.SerializeToYdbJson(json, Stream.VirtualTimestamps);
                    }

                    TStringStream str;
                    NJson::TJsonWriterConfig jsonConfig;
                    jsonConfig.ValidateUtf8 = false;
                    jsonConfig.WriteNanAsString = true;
                    WriteJson(&str, &json, jsonConfig);

                    data.SetData(str.Str());
                    cmd.SetPartitionKey(record.GetPartitionKey());
                    break;
                }

                default: {
                    LOG_E("Unknown format"
                        << ": format# " << static_cast<int>(Stream.Format));
                    return Leave();
                }
            }

            cmd.SetData(data.SerializeAsString());
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
        Leave();
    }

    void Lost() {
        LOG_W("Lost");
        Leave();
    }

    void Leave() {
        Send(Parent, new TEvChangeExchangePrivate::TEvGone(PartitionId));
        PassAway();
    }

    void PassAway() override {
        if (Writer) {
            Send(Writer, new TEvents::TEvPoisonPill());
        }

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
        , Stream(stream)
        , SourceId(ToString(DataShard.TabletId))
    {
    }

    void Bootstrap() {
        auto opts = TPartitionWriterOpts()
            .WithCheckState(true)
            .WithAutoRegister(true);
        Writer = RegisterWithSameMailbox(CreatePartitionWriter(SelfId(), ShardId, PartitionId, SourceId, opts));
        Become(&TThis::StateInit);
    }

    STATEFN(StateBase) {
        switch (ev->GetTypeRewrite()) {
            sFunc(TEvPartitionWriter::TEvDisconnected, Disconnected);
            hFunc(NMon::TEvRemoteHttpInfo, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    const TActorId Parent;
    const TDataShardId DataShard;
    const ui32 PartitionId;
    const ui64 ShardId;
    const TUserTable::TCdcStream Stream;
    const TString SourceId;
    mutable TMaybe<TString> LogPrefix;

    TActorId Writer;
    i64 MaxSeqNo = 0;
    TVector<i64> Pending;
    ui64 Cookie = 0;

}; // TCdcChangeSenderPartition

class TCdcChangeSenderMain
    : public TActorBootstrapped<TCdcChangeSenderMain>
    , public TBaseChangeSender
    , public IChangeSenderResolver
    , private TSchemeCacheHelpers
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
                Y_VERIFY(lhs.KeyRange.ToBound || rhs.KeyRange.ToBound);

                if (!lhs.KeyRange.ToBound) {
                    return false;
                }

                if (!rhs.KeyRange.ToBound) {
                    return true;
                }

                Y_VERIFY(lhs.KeyRange.ToBound && rhs.KeyRange.ToBound);

                const int compares = CompareTypedCellVectors(
                    lhs.KeyRange.ToBound->GetCells().data(),
                    rhs.KeyRange.ToBound->GetCells().data(),
                    Schema.data(), Schema.size()
                );

                return (compares < 0);
            }

        }; // TLess

    }; // TPQPartitionInfo

    struct TKeyDesc {
        struct TPartitionInfo {
            ui32 PartitionId;
            ui64 ShardId;
            TSerializedCellVec EndKeyPrefix;
            // just a hint
            static constexpr bool IsInclusive = false;
            static constexpr bool IsPoint = false;

            explicit TPartitionInfo(const TPQPartitionInfo& info)
                : PartitionId(info.PartitionId)
                , ShardId(info.ShardId)
            {
                if (info.KeyRange.ToBound) {
                    EndKeyPrefix = *info.KeyRange.ToBound;
                }
            }

        }; // TPartitionInfo

        TVector<NScheme::TTypeInfo> Schema;
        TVector<TPartitionInfo> Partitions;

    }; // TKeyDesc

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

    static TVector<ui64> MakePartitionIds(const TVector<TKeyDesc::TPartitionInfo>& partitions) {
        TVector<ui64> result(Reserve(partitions.size()));

        for (const auto& partition : partitions) {
            result.push_back(partition.PartitionId);
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

        Y_VERIFY(entry.ListNodeEntry->Children.size() == 1);
        const auto& topic = entry.ListNodeEntry->Children.at(0);

        Y_VERIFY(topic.Kind == TNavigate::KindTopic);
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

        KeyDesc = MakeHolder<TKeyDesc>();
        PartitionToShard.clear();

        KeyDesc->Schema.reserve(pqConfig.PartitionKeySchemaSize());
        for (const auto& keySchema : pqConfig.GetPartitionKeySchema()) {
            // TODO: support pg types
            KeyDesc->Schema.push_back(NScheme::TTypeInfo(keySchema.GetTypeId()));
        }

        TSet<TPQPartitionInfo, TPQPartitionInfo::TLess> partitions(KeyDesc->Schema);
        THashSet<ui64> shards;

        for (const auto& partition : pqDesc.GetPartitions()) {
            const auto partitionId = partition.GetPartitionId();
            const auto shardId = partition.GetTabletId();

            PartitionToShard.emplace(partitionId, shardId);

            auto keyRange = TPartitionKeyRange::Parse(partition.GetKeyRange());
            Y_VERIFY(!keyRange.FromBound || keyRange.FromBound->GetCells().size() == KeyDesc->Schema.size());
            Y_VERIFY(!keyRange.ToBound || keyRange.ToBound->GetCells().size() == KeyDesc->Schema.size());

            partitions.insert({partitionId, shardId, std::move(keyRange)});
            shards.insert(shardId);
        }

        // used to validate
        bool isFirst = true;
        const TPQPartitionInfo* prev = nullptr;

        KeyDesc->Partitions.reserve(partitions.size());
        for (const auto& cur : partitions) {
            if (isFirst) {
                isFirst = false;
                Y_VERIFY(!cur.KeyRange.FromBound.Defined());
            } else {
                Y_VERIFY(cur.KeyRange.FromBound.Defined());
                Y_VERIFY(prev);
                Y_VERIFY(prev->KeyRange.ToBound.Defined());
                // TODO: compare cells
            }

            KeyDesc->Partitions.emplace_back(cur);
            prev = &cur;
        }

        if (prev) {
            Y_VERIFY(!prev->KeyRange.ToBound.Defined());
        }

        CreateSenders(MakePartitionIds(KeyDesc->Partitions));
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
        return KeyDesc && KeyDesc->Partitions;
    }

    ui64 GetPartitionId(const TChangeRecord& record) const override {
        Y_VERIFY(KeyDesc);
        Y_VERIFY(KeyDesc->Partitions);

        switch (Stream.Format) {
            case NKikimrSchemeOp::ECdcStreamFormatProto: {
                const auto range = TTableRange(record.GetKey());
                Y_VERIFY(range.Point);

                TVector<TKeyDesc::TPartitionInfo>::const_iterator it = LowerBound(
                    KeyDesc->Partitions.begin(), KeyDesc->Partitions.end(), true,
                    [&](const TKeyDesc::TPartitionInfo& partition, bool) {
                        const int compares = CompareBorders<true, false>(
                            partition.EndKeyPrefix.GetCells(), range.From,
                            partition.IsInclusive || partition.IsPoint,
                            range.InclusiveFrom || range.Point, KeyDesc->Schema
                        );

                        return (compares < 0);
                    }
                );

                Y_VERIFY(it != KeyDesc->Partitions.end());
                return it->PartitionId;
            }

            case NKikimrSchemeOp::ECdcStreamFormatJson:
            case NKikimrSchemeOp::ECdcStreamFormatDynamoDBStreamsJson: {
                using namespace NKikimr::NDataStreams::V1;
                const auto hashKey = HexBytesToDecimal(record.GetPartitionKey() /* MD5 */);
                return ShardFromDecimal(hashKey, KeyDesc->Partitions.size());
            }

            default: {
                Y_FAIL_S("Unknown format"
                    << ": format# " << static_cast<int>(Stream.Format));
            }
        }
    }

    IActor* CreateSender(ui64 partitionId) override {
        Y_VERIFY(PartitionToShard.contains(partitionId));
        const auto shardId = PartitionToShard.at(partitionId);
        return new TCdcChangeSenderPartition(SelfId(), DataShard, partitionId, shardId, Stream);
    }

    void Handle(TEvChangeExchange::TEvEnqueueRecords::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());
        EnqueueRecords(std::move(ev->Get()->Records));
    }

    void Handle(TEvChangeExchange::TEvRecords::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());
        ProcessRecords(std::move(ev->Get()->Records));
    }

    void Handle(TEvChangeExchange::TEvForgetRecords::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());
        ForgetRecords(std::move(ev->Get()->Records));
    }

    void Handle(TEvChangeExchangePrivate::TEvReady::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());
        OnReady(ev->Get()->PartitionId);
    }

    void Handle(TEvChangeExchangePrivate::TEvGone::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());
        OnGone(ev->Get()->PartitionId);
    }

    void Handle(TEvChangeExchange::TEvRemoveSender::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());
        Y_VERIFY(ev->Get()->PathId == PathId);

        RemoveRecords();
        PassAway();
    }

    void AutoRemove(TEvChangeExchange::TEvEnqueueRecords::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());
        RemoveRecords(std::move(ev->Get()->Records));
    }

    void Handle(NMon::TEvRemoteHttpInfo::TPtr& ev, const TActorContext& ctx) {
        RenderHtmlPage(ESenderType::CdcStream, ev, ctx);
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
        , TBaseChangeSender(this, this, dataShard, streamPathId)
    {
    }

    void Bootstrap() {
        ResolveCdcStream();
    }

    STFUNC(StateBase) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvChangeExchange::TEvEnqueueRecords, Handle);
            hFunc(TEvChangeExchange::TEvRecords, Handle);
            hFunc(TEvChangeExchange::TEvForgetRecords, Handle);
            hFunc(TEvChangeExchange::TEvRemoveSender, Handle);
            hFunc(TEvChangeExchangePrivate::TEvReady, Handle);
            hFunc(TEvChangeExchangePrivate::TEvGone, Handle);
            HFunc(NMon::TEvRemoteHttpInfo, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

    STFUNC(StatePendingRemove) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvChangeExchange::TEvEnqueueRecords, AutoRemove);
            hFunc(TEvChangeExchange::TEvRemoveSender, Handle);
            HFunc(NMon::TEvRemoteHttpInfo, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    mutable TMaybe<TString> LogPrefix;

    TUserTable::TCdcStream Stream;
    TPathId TopicPathId;
    THolder<TKeyDesc> KeyDesc;
    THashMap<ui32, ui64> PartitionToShard;

}; // TCdcChangeSenderMain

IActor* CreateCdcStreamChangeSender(const TDataShardId& dataShard, const TPathId& streamPathId) {
    return new TCdcChangeSenderMain(dataShard, streamPathId);
}

}
