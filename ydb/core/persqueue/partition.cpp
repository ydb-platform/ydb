#include "event_helpers.h"
#include "mirrorer.h"
#include "partition.h"
#include "read.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/quoter.h>
#include <ydb/core/protos/counters_pq.pb.h>
#include <ydb/core/protos/msgbus.pb.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <ydb/public/lib/base/msgbus.h>
#include <library/cpp/html/pcdata/pcdata.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/time_provider/time_provider.h>
#include <util/folder/path.h>
#include <util/string/escape.h>
#include <util/system/byteorder.h>

#define VERIFY_RESULT_BLOB(blob, pos) \
    Y_VERIFY(!blob.Data.empty(), "Empty data. SourceId: %s, SeqNo: %" PRIu64, blob.SourceId.data(), blob.SeqNo); \
    Y_VERIFY(blob.SeqNo <= (ui64)Max<i64>(), "SeqNo is too big: %" PRIu64, blob.SeqNo);

namespace NKikimr::NPQ {

static const ui32 BATCH_UNPACK_SIZE_BORDER = 500_KB;
static const ui32 MAX_WRITE_CYCLE_SIZE = 16_MB;
static const ui32 MAX_USER_ACTS = 1000;
static const TDuration WAKE_TIMEOUT = TDuration::Seconds(5);
static const ui32 MAX_INLINE_SIZE = 1000;
static const ui32 LEVEL0 = 32;
static const TDuration UPDATE_AVAIL_SIZE_INTERVAL = TDuration::MilliSeconds(100);
static const TString WRITE_QUOTA_ROOT_PATH = "write-quota";
static const ui32 MAX_USERS = 1000;
static const ui64 SET_OFFSET_COOKIE = 1;
static const ui32 MAX_TXS = 1000;

auto GetStepAndTxId(ui64 step, ui64 txId)
{
    return std::make_pair(step, txId);
}

template<class E>
auto GetStepAndTxId(const E& event)
{
    return GetStepAndTxId(event.Step, event.TxId);
}

struct TPartition::THasDataReq {
    ui64 Num;
    ui64 Offset;
    TActorId Sender;
    TMaybe<ui64> Cookie;
    TString ClientId;

    bool operator < (const THasDataReq& req) const {
        return Num < req.Num;
    }
};

struct TPartition::THasDataDeadline {
    TInstant Deadline;
    TPartition::THasDataReq Request;

    bool operator < (const THasDataDeadline& dl) const {
        return Deadline < dl.Deadline || Deadline == dl.Deadline && Request < dl.Request;
    }
};

struct TMirrorerInfo {
    TMirrorerInfo(const TActorId& actor, const TTabletCountersBase& baseline)
    : Actor(actor) {
        Baseline.Populate(baseline);
    }

    TActorId Actor;
    TTabletCountersBase Baseline;
};

class TKeyLevel {
public:
    friend IOutputStream& operator <<(IOutputStream& out, const TKeyLevel& value);

    TKeyLevel(ui32 border)
    : Border_(border)
    , Sum_(0)
    , RecsCount_(0)
    , InternalPartsCount_(0) {}

    void Clear() {
        Keys_.clear();
        Sum_ = 0;
        RecsCount_ = 0;
        InternalPartsCount_ = 0;
    }

    ui32 KeysCount() const {
        return Keys_.size();
    }

    ui32 RecsCount() const {
        return RecsCount_;
    }

    ui16 InternalPartsCount() const {
        return InternalPartsCount_;
    }

    bool NeedCompaction() const {
        return Sum_ >= Border_;
    }

    std::pair<TKey, ui32> Compact() {
        Y_VERIFY(!Keys_.empty());
        TKey tmp(Keys_.front().first);
        tmp.SetCount(RecsCount_);
        tmp.SetInternalPartsCount(InternalPartsCount_);
        std::pair<TKey, ui32> res(tmp, Sum_);
        Clear();
        return res;
    }

    std::pair<TKey, ui32> PopFront() {
        Y_VERIFY(!Keys_.empty());
        Sum_ -= Keys_.front().second;
        RecsCount_ -= Keys_.front().first.GetCount();
        InternalPartsCount_ -= Keys_.front().first.GetInternalPartsCount();
        auto res = Keys_.front();
        Keys_.pop_front();
        return res;
    }

    std::pair<TKey, ui32> PopBack() {
        Y_VERIFY(!Keys_.empty());
        Sum_ -= Keys_.back().second;
        RecsCount_ -= Keys_.back().first.GetCount();
        InternalPartsCount_ -= Keys_.back().first.GetInternalPartsCount();
        auto res = Keys_.back();
        Keys_.pop_back();
        return res;
    }

    ui32 Sum() const {
        return Sum_;
    }

    const TKey& GetKey(const ui32 pos) const {
        Y_VERIFY(pos < Keys_.size());
        return Keys_[pos].first;
    }

    const ui32& GetSize(const ui32 pos) const {
        Y_VERIFY(pos < Keys_.size());
        return Keys_[pos].second;
    }

    void PushKeyToFront(const TKey& key, ui32 size) {
        Sum_ += size;
        RecsCount_ += key.GetCount();
        InternalPartsCount_ += key.GetInternalPartsCount();
        Keys_.push_front(std::make_pair(key, size));
    }

    void AddKey(const TKey& key, ui32 size) {
        Sum_ += size;
        RecsCount_ += key.GetCount();
        InternalPartsCount_ += key.GetInternalPartsCount();
        Keys_.push_back(std::make_pair(key, size));
    }

    ui32 Border() const {
        return Border_;
    }

private:
    const ui32 Border_;
    std::deque<std::pair<TKey, ui32>> Keys_;
    ui32 Sum_;
    ui32 RecsCount_;
    ui16 InternalPartsCount_;
};

void HtmlOutput(IOutputStream& out, const TString& line, const std::deque<std::pair<TKey, ui32>>& keys) {
    HTML(out) {
        TABLE() {
        TABLEHEAD() {
            TABLER() {
                TABLEH() {out << line;}
            }
        }
        TABLEBODY() {
            TABLER() {
                TABLEH() {out << "offset";}
                for (auto& p: keys) {
                    TABLED() {out << p.first.GetOffset();}
                }
            }
            TABLER() {
                TABLEH() {out << "partNo";}
                for (auto& p: keys) {
                    TABLED() {out << p.first.GetPartNo();}
                }
            }
            TABLER() {
                TABLEH() {out << "size";}
                for (auto& p: keys) {
                    TABLED() {out << p.second;}
                }
            }
        }
        }
    }
}

IOutputStream& operator <<(IOutputStream& out, const TKeyLevel& value) {
    TStringStream str;
    str << "count=" << value.Keys_.size() << " sum=" << value.Sum_ << " border=" << value.Border_ << " recs= " << value.RecsCount_ << ":";
    HtmlOutput(out, str.Str(), value.Keys_);
    return out;
}


ui64 GetOffsetEstimate(const std::deque<TDataKey>& container, TInstant timestamp, ui64 offset) {
    if (container.empty()) {
        return offset;
    }
    auto it = std::lower_bound(container.begin(), container.end(), timestamp,
                    [](const TDataKey& p, const TInstant timestamp) { return timestamp > p.Timestamp; });
    if (it == container.end()) {
        return offset;
    } else {
        return it->Key.GetOffset();
    }
}

bool IsQuotingEnabled(const NKikimrPQ::TPQConfig& pqConfig,
                      bool isLocalDC)
{
    const auto& quotingConfig = pqConfig.GetQuotingConfig();
    return isLocalDC && !pqConfig.GetTopicsAreFirstClassCitizen() && quotingConfig.GetEnableQuoting();
}

void CalcTopicWriteQuotaParams(const NKikimrPQ::TPQConfig& pqConfig,
                               bool isLocalDC,
                               NPersQueue::TTopicConverterPtr topicConverter,
                               ui64 tabletId,
                               const TActorContext& ctx,
                               TString& topicWriteQuoterPath,
                               TString& topicWriteQuotaResourcePath)
{
    if (IsQuotingEnabled(pqConfig, isLocalDC)) { // Mirrored topics are not quoted in local dc.
        const auto& quotingConfig = pqConfig.GetQuotingConfig();

        Y_VERIFY(quotingConfig.GetTopicWriteQuotaEntityToLimit() != NKikimrPQ::TPQConfig::TQuotingConfig::UNSPECIFIED);

        // ToDo[migration] - double check
        auto topicPath = topicConverter->GetFederationPath();

        // ToDo[migration] - separate quoter paths?
        auto topicParts = SplitPath(topicPath); // account/folder/topic // account is first element
        if (topicParts.size() < 2) {
            LOG_WARN_S(ctx, NKikimrServices::PERSQUEUE,
                       "tablet " << tabletId << " topic '" << topicPath << "' Bad topic name. Disable quoting for topic");
            return;
        }
        topicParts[0] = WRITE_QUOTA_ROOT_PATH; // write-quota/folder/topic

        topicWriteQuotaResourcePath = JoinPath(topicParts);
        topicWriteQuoterPath = TStringBuilder() << quotingConfig.GetQuotersDirectoryPath() << "/" << topicConverter->GetAccount();

        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE,
                    "topicWriteQuutaResourcePath '" << topicWriteQuotaResourcePath
                    << "' topicWriteQuoterPath '" << topicWriteQuoterPath
                    << "' account '" << topicConverter->GetAccount()
                    << "'"
        );
    }
}

void TPartition::ReplyError(const TActorContext& ctx, const ui64 dst, NPersQueue::NErrorCode::EErrorCode errorCode, const TString& error) {
    ReplyPersQueueError(
        dst == 0 ? ctx.SelfID : Tablet, ctx, TabletID, TopicConverter->GetClientsideName(), Partition,
        TabletCounters, NKikimrServices::PERSQUEUE, dst, errorCode, error, true
    );
}

void TPartition::ReplyPropose(const TActorContext& ctx,
                              const NKikimrPQ::TEvProposeTransaction& event,
                              NKikimrPQ::TEvProposeTransactionResult::EStatus statusCode)
{
    ctx.Send(ActorIdFromProto(event.GetSource()),
             MakeReplyPropose(event, statusCode).Release());
}

void TPartition::ReplyOk(const TActorContext& ctx, const ui64 dst) {
    ctx.Send(Tablet, MakeReplyOk(dst).Release());
}

void TPartition::ReplyOwnerOk(const TActorContext& ctx, const ui64 dst, const TString& cookie) {
    THolder<TEvPQ::TEvProxyResponse> response = MakeHolder<TEvPQ::TEvProxyResponse>(dst);
    NKikimrClient::TResponse& resp = response->Response;
    resp.SetStatus(NMsgBusProxy::MSTATUS_OK);
    resp.SetErrorCode(NPersQueue::NErrorCode::OK);
    resp.MutablePartitionResponse()->MutableCmdGetOwnershipResult()->SetOwnerCookie(cookie);
    ctx.Send(Tablet, response.Release());
}

void TPartition::ReplyWrite(
    const TActorContext& ctx, const ui64 dst, const TString& sourceId, const ui64 seqNo, const ui16 partNo, const ui16 totalParts,
    const ui64 offset, const TInstant writeTimestamp,  bool already, const ui64 maxSeqNo,
    const TDuration partitionQuotedTime, const TDuration topicQuotedTime, const TDuration queueTime, const TDuration writeTime) {
    Y_VERIFY(offset <= (ui64)Max<i64>(), "Offset is too big: %" PRIu64, offset);
    Y_VERIFY(seqNo <= (ui64)Max<i64>(), "SeqNo is too big: %" PRIu64, seqNo);

    THolder<TEvPQ::TEvProxyResponse> response = MakeHolder<TEvPQ::TEvProxyResponse>(dst);
    NKikimrClient::TResponse& resp = response->Response;
    resp.SetStatus(NMsgBusProxy::MSTATUS_OK);
    resp.SetErrorCode(NPersQueue::NErrorCode::OK);
    auto write = resp.MutablePartitionResponse()->AddCmdWriteResult();
    write->SetSourceId(sourceId);
    write->SetSeqNo(seqNo);
    write->SetWriteTimestampMS(writeTimestamp.MilliSeconds());
    if (totalParts > 1)
        write->SetPartNo(partNo);
    write->SetAlreadyWritten(already);
    if (already)
        write->SetMaxSeqNo(maxSeqNo);
    write->SetOffset(offset);

    write->SetPartitionQuotedTimeMs(partitionQuotedTime.MilliSeconds());
    write->SetTopicQuotedTimeMs(topicQuotedTime.MilliSeconds());
    write->SetTotalTimeInPartitionQueueMs(queueTime.MilliSeconds());
    write->SetWriteTimeMs(writeTime.MilliSeconds());

    ctx.Send(Tablet, response.Release());
}


void TPartition::ReplyGetClientOffsetOk(const TActorContext& ctx, const ui64 dst, const i64 offset,
    const TInstant writeTimestamp, const TInstant createTimestamp) {
    ctx.Send(Tablet, MakeReplyGetClientOffsetOk(dst, offset, writeTimestamp, createTimestamp).Release());
}


static void RequestRange(const TActorContext& ctx, const TActorId& dst, ui32 partition,
                         TKeyPrefix::EType c, bool includeData = false, const TString& key = "", bool dropTmp = false) {
    THolder<TEvKeyValue::TEvRequest> request(new TEvKeyValue::TEvRequest);
    auto read = request->Record.AddCmdReadRange();
    auto range = read->MutableRange();
    TKeyPrefix from(c, partition);
    if (!key.empty()) {
        Y_VERIFY(key.StartsWith(TStringBuf(from.Data(), from.Size())));
        from.Clear();
        from.Append(key.data(), key.size());
    }
    range->SetFrom(from.Data(), from.Size());

    TKeyPrefix to(c, partition + 1);
    range->SetTo(to.Data(), to.Size());

    if(includeData)
        read->SetIncludeData(true);

    if (dropTmp) {
        auto del = request->Record.AddCmdDeleteRange();
        auto range = del->MutableRange();
        TKeyPrefix from(TKeyPrefix::TypeTmpData, partition);
        range->SetFrom(from.Data(), from.Size());

        TKeyPrefix to(TKeyPrefix::TypeTmpData, partition + 1);
        range->SetTo(to.Data(), to.Size());
    }

    ctx.Send(dst, request.Release());
}


NKikimrClient::TKeyValueRequest::EStorageChannel GetChannel(ui32 i) {
    return NKikimrClient::TKeyValueRequest::EStorageChannel(NKikimrClient::TKeyValueRequest::MAIN + i);
}


void AddCheckDiskRequest(TEvKeyValue::TEvRequest *request, ui32 numChannels) {
    for (ui32 i = 0; i < numChannels; ++i) {
        request->Record.AddCmdGetStatus()->SetStorageChannel(GetChannel(i));
    }
}


static void RequestDiskStatus(const TActorContext& ctx, const TActorId& dst, ui32 numChannels) {
    THolder<TEvKeyValue::TEvRequest> request(new TEvKeyValue::TEvRequest);

    AddCheckDiskRequest(request.Get(), numChannels);

    ctx.Send(dst, request.Release());
}


void RequestInfoRange(const TActorContext& ctx, const TActorId& dst, ui32 partition, const TString& key) {
    RequestRange(ctx, dst, partition, TKeyPrefix::TypeInfo, true, key, key == "");
}

void RequestMetaRead(const TActorContext& ctx, const TActorId& dst, ui32 partition) {
    auto addKey = [](NKikimrClient::TKeyValueRequest& request, TKeyPrefix::EType type, ui32 partition) {
        auto read = request.AddCmdRead();
        TKeyPrefix key{type, partition};
        read->SetKey(key.Data(), key.Size());
    };

    THolder<TEvKeyValue::TEvRequest> request(new TEvKeyValue::TEvRequest);

    addKey(request->Record, TKeyPrefix::TypeMeta, partition);
    addKey(request->Record, TKeyPrefix::TypeTxMeta, partition);

    ctx.Send(dst, request.Release());
}

void RequestData(const TActorContext& ctx, const TActorId& dst, const TVector<TString>& keys) {
    THolder<TEvKeyValue::TEvRequest> request(new TEvKeyValue::TEvRequest);
    for (auto& key: keys) {
        auto read = request->Record.AddCmdRead();
        read->SetKey(key);
    }
    ctx.Send(dst, request.Release());
}

void RequestDataRange(const TActorContext& ctx, const TActorId& dst, ui32 partition, const TString& key) {
    RequestRange(ctx, dst, partition, TKeyPrefix::TypeData, false, key);
}


void TPartition::FillReadFromTimestamps(const NKikimrPQ::TPQTabletConfig& config, const TActorContext& ctx) {
    TSet<TString> hasReadRule;

    for (auto& userInfo : UsersInfoStorage->GetAll()) {
        userInfo.second.ReadFromTimestamp = TInstant::Zero();
        userInfo.second.HasReadRule = false;
        hasReadRule.insert(userInfo.first);
    }
    for (ui32 i = 0; i < config.ReadRulesSize(); ++i) {
        const auto& consumer = config.GetReadRules(i);
        auto& userInfo = UsersInfoStorage->GetOrCreate(consumer, ctx, 0);
        userInfo.HasReadRule = true;
        ui64 rrGen = i < config.ReadRuleGenerationsSize() ? config.GetReadRuleGenerations(i) : 0;
        if (userInfo.ReadRuleGeneration != rrGen) {
            THolder<TEvPQ::TEvSetClientInfo> event = MakeHolder<TEvPQ::TEvSetClientInfo>(0, consumer, 0, "", 0, 0,
                                                TEvPQ::TEvSetClientInfo::ESCI_INIT_READ_RULE, rrGen);
            //
            // TODO(abcdef): заменить на вызов ProcessUserAct
            //
            AddUserAct(event.Release());
            userInfo.Session = "";
            userInfo.Offset = 0;
            if (userInfo.Important) {
                userInfo.Offset = StartOffset;
            }
            userInfo.Step = userInfo.Generation = 0;
        }
        hasReadRule.erase(consumer);
        TInstant ts = i < config.ReadFromTimestampsMsSize() ? TInstant::MilliSeconds(config.GetReadFromTimestampsMs(i)) : TInstant::Zero();
        if (!ts) ts += TDuration::MilliSeconds(1);
        if (!userInfo.ReadFromTimestamp || userInfo.ReadFromTimestamp > ts)
            userInfo.ReadFromTimestamp = ts;
    }
    for (auto& consumer : hasReadRule) {
        auto& userInfo = UsersInfoStorage->GetOrCreate(consumer, ctx);
        THolder<TEvPQ::TEvSetClientInfo> event = MakeHolder<TEvPQ::TEvSetClientInfo>(0, consumer,
                                                                               0, "", 0, 0, TEvPQ::TEvSetClientInfo::ESCI_DROP_READ_RULE, 0);
        if (!userInfo.Important && userInfo.LabeledCounters) {
            ctx.Send(Tablet, new TEvPQ::TEvPartitionLabeledCountersDrop(Partition, userInfo.LabeledCounters->GetGroup()));
        }
        userInfo.Session = "";
        userInfo.Offset = 0;
        userInfo.Step = userInfo.Generation = 0;
        //
        // TODO(abcdef): заменить на вызов ProcessUserAct
        //
        AddUserAct(event.Release());
    }
}

TPartition::TPartition(ui64 tabletId, ui32 partition, const TActorId& tablet, const TActorId& blobCache,
                       const NPersQueue::TTopicConverterPtr& topicConverter, bool isLocalDC, TString dcId, bool isServerless,
                       const NKikimrPQ::TPQTabletConfig& tabletConfig, const TTabletCountersBase& counters, bool subDomainOutOfSpace,
                       bool newPartition,
                       TVector<TTransaction> distrTxs)
    : TabletID(tabletId)
    , Partition(partition)
    , TabletConfig(tabletConfig)
    , Counters(counters)
    , TopicConverter(topicConverter)
    , IsLocalDC(isLocalDC)
    , DCId(std::move(dcId))
    , StartOffset(0)
    , EndOffset(0)
    , WriteInflightSize(0)
    , Tablet(tablet)
    , BlobCache(blobCache)
    , InitState(WaitConfig)
    , PartitionedBlob(partition, 0, 0, 0, 0, 0, Head, NewHead, true, false, 8_MB)
    , NewHeadKey{TKey{}, 0, TInstant::Zero(), 0}
    , BodySize(0)
    , MaxWriteResponsesSize(0)
    , GapSize(0)
    , IsServerless(isServerless)
    , ReadingTimestamp(false)
    , Cookie(0)
    , InitDuration(TDuration::Zero())
    , InitDone(false)
    , NewPartition(newPartition)
    , Subscriber(partition, TabletCounters, Tablet)
    , WriteCycleSize(0)
    , WriteNewSize(0)
    , WriteNewSizeInternal(0)
    , WriteNewSizeUncompressed(0)
    , WriteNewMessages(0)
    , WriteNewMessagesInternal(0)
    , DiskIsFull(false)
    , SubDomainOutOfSpace(subDomainOutOfSpace)
    , HasDataReqNum(0)
    , AvgWriteBytes{{TDuration::Seconds(1), 1000}, {TDuration::Minutes(1), 1000}, {TDuration::Hours(1), 2000}, {TDuration::Days(1), 2000}}
    , AvgQuotaBytes{{TDuration::Seconds(1), 1000}, {TDuration::Minutes(1), 1000}, {TDuration::Hours(1), 2000}, {TDuration::Days(1), 2000}}
    , ReservedSize(0)
    , Channel(0)
    , WriteBufferIsFullCounter(nullptr)
    , WriteLagMs(TDuration::Minutes(1), 100)
{
    TabletCounters.Populate(Counters);

    if (!distrTxs.empty()) {
        std::move(distrTxs.begin(), distrTxs.end(),
                  std::back_inserter(DistrTxs));
        TxInProgress = DistrTxs.front().Predicate.Defined();
    }
}

void TPartition::HandleMonitoring(TEvPQ::TEvMonRequest::TPtr& ev, const TActorContext& ctx) {
    TVector<TString> res;
    TString str;
    if (CurrentStateFunc() == &TThis::StateInit) {
        str = "State is StateInit";
    } else if (CurrentStateFunc() == &TThis::StateIdle) {
        str = "State is StateIdle";
    } else if (CurrentStateFunc() == &TThis::StateWrite) {
        str = "State is StateWrite";
    } else {
        Y_FAIL("");
    }
    TStringStream out;
    out << "Partition " << i32(Partition) << ": " << str;  res.push_back(out.Str()); out.Clear();
    if (DiskIsFull) {
        out << "DISK IS FULL";
        res.push_back(out.Str());
        out.Clear();
    }
    if (WaitingForSubDomainQuota(ctx)) {
        out << "SubDomain is out of space";
        res.push_back(out.Str());
        out.Clear();
    }
    out << "StartOffset: " << StartOffset; res.push_back(out.Str()); out.Clear();
    out << "EndOffset: " << EndOffset; res.push_back(out.Str()); out.Clear();
    out << "CreationTime: " << CreationTime.ToStringLocalUpToSeconds(); res.push_back(out.Str()); out.Clear();
    out << "InitDuration: " << InitDuration.ToString(); res.push_back(out.Str()); out.Clear();
    out << "TotalCount: " << (Head.GetNextOffset() - StartOffset); res.push_back(out.Str()); out.Clear();
    out << "TotalSize: " << Size(); res.push_back(out.Str()); out.Clear();
    out << "LastOffset: " << (Head.GetNextOffset()); res.push_back(out.Str()); out.Clear();
    out << "HeadOffset: " << Head.Offset << ", count: " << Head.GetCount(); res.push_back(out.Str()); out.Clear();
    out << "WriteInflightSize: " << WriteInflightSize; res.push_back(out.Str()); out.Clear();
    out << "ReservedBytesSize: " << ReservedSize; res.push_back(out.Str()); out.Clear();
    out << "OwnerPipes: " << OwnerPipes.size(); res.push_back(out.Str()); out.Clear();
    out << "Owners: " << Owners.size(); res.push_back(out.Str()); out.Clear();
    out << "Currently writing: " << Responses.size(); res.push_back(out.Str()); out.Clear();
    out << "MaxCurrently writing: " << MaxWriteResponsesSize; res.push_back(out.Str()); out.Clear();
    out << "DataKeysBody size: " << DataKeysBody.size(); res.push_back(out.Str()); out.Clear();
    for (ui32 i = 0; i < DataKeysHead.size(); ++i) {
        out << "DataKeysHead[" << i << "] size: " << DataKeysHead[i].KeysCount() << " sum: " << DataKeysHead[i].Sum()
            << " border: " << DataKeysHead[i].Border() << " recs: " << DataKeysHead[i].RecsCount() << " intCount: " << DataKeysHead[i].InternalPartsCount();
        res.push_back(out.Str()); out.Clear();
    }
    for (auto& avg : AvgWriteBytes) {
        out << "AvgWriteSize per " << avg.GetDuration().ToString() << " is " << avg.GetValue() << " bytes";
        res.push_back(out.Str()); out.Clear();
    }
    out << Config.DebugString(); res.push_back(out.Str()); out.Clear();
    HTML(out) {
        DIV_CLASS_ID("tab-pane fade", Sprintf("partition_%u", ui32(Partition))) {
            TABLE_SORTABLE_CLASS("table") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() {out << "Type";}
                        TABLEH() {out << "Pos";}
                        TABLEH() {out << "timestamp";}
                        TABLEH() {out << "Offset";}
                        TABLEH() {out << "PartNo";}
                        TABLEH() {out << "Count";}
                        TABLEH() {out << "InternalPartsCount";}
                        TABLEH() {out << "Size";}
                    }
                }
                TABLEBODY() {
                    ui32 i = 0;
                    for (auto& d: DataKeysBody) {
                        TABLER() {
                            TABLED() {out << "DataBody";}
                            TABLED() {out << i++;}
                            TABLED() {out << ToStringLocalTimeUpToSeconds(d.Timestamp);}
                            TABLED() {out << d.Key.GetOffset();}
                            TABLED() {out << d.Key.GetPartNo();}
                            TABLED() {out << d.Key.GetCount();}
                            TABLED() {out << d.Key.GetInternalPartsCount();}
                            TABLED() {out << d.Size;}
                        }
                    }
                    ui32 currentLevel = 0;
                    for (ui32 p = 0; p < HeadKeys.size(); ++p) {
                        ui32 size  = HeadKeys[p].Size;
                        while (currentLevel + 1 < TotalLevels && size < CompactLevelBorder[currentLevel + 1])
                            ++currentLevel;
                        Y_VERIFY(size < CompactLevelBorder[currentLevel]);
                        TABLER() {
                            TABLED() {out << "DataHead[" << currentLevel << "]";}
                            TABLED() {out << i++;}
                            TABLED() {out << ToStringLocalTimeUpToSeconds(HeadKeys[p].Timestamp);}
                            TABLED() {out << HeadKeys[p].Key.GetOffset();}
                            TABLED() {out << HeadKeys[p].Key.GetPartNo();}
                            TABLED() {out << HeadKeys[p].Key.GetCount();}
                            TABLED() {out << HeadKeys[p].Key.GetInternalPartsCount();}
                            TABLED() {out << size;}
                        }
                    }
                }
            }

            TABLE_SORTABLE_CLASS("table") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() {out << "GapStartOffset";}
                        TABLEH() {out << "GapEndOffset";}
                        TABLEH() {out << "GapSize";}
                        TABLEH() {out << "id";}
                    }
                }
                ui32 i = 0;
                TABLEBODY() {
                    for (auto& d: GapOffsets) {
                        TABLER() {
                            TABLED() {out << d.first;}
                            TABLED() {out << d.second;}
                            TABLED() {out << (d.second - d.first);}
                            TABLED() {out << (i++);}
                        }
                    }
                    if (!DataKeysBody.empty() && DataKeysBody.back().Key.GetOffset() + DataKeysBody.back().Key.GetCount() < Head.Offset) {
                        TABLER() {
                            TABLED() {out << (DataKeysBody.back().Key.GetOffset() + DataKeysBody.back().Key.GetCount());}
                            TABLED() {out << Head.Offset;}
                            TABLED() {out << (Head.Offset - (DataKeysBody.back().Key.GetOffset() + DataKeysBody.back().Key.GetCount()));}
                            TABLED() {out << (i++);}
                        }

                    }
                }
            }


            TABLE_SORTABLE_CLASS("table") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() {out << "SourceId";}
                        TABLEH() {out << "SeqNo";}
                        TABLEH() {out << "Offset";}
                        TABLEH() {out << "WriteTimestamp";}
                        TABLEH() {out << "CreateTimestamp";}
                        TABLEH() {out << "Explicit";}
                        TABLEH() {out << "State";}
                    }
                }
                TABLEBODY() {
                    for (const auto& [sourceId, sourceIdInfo]: SourceIdStorage.GetInMemorySourceIds()) {
                        TABLER() {
                            TABLED() {out << EncodeHtmlPcdata(EscapeC(sourceId));}
                            TABLED() {out << sourceIdInfo.SeqNo;}
                            TABLED() {out << sourceIdInfo.Offset;}
                            TABLED() {out << ToStringLocalTimeUpToSeconds(sourceIdInfo.WriteTimestamp);}
                            TABLED() {out << ToStringLocalTimeUpToSeconds(sourceIdInfo.CreateTimestamp);}
                            TABLED() {out << (sourceIdInfo.Explicit ? "true" : "false");}
                            TABLED() {out << sourceIdInfo.State;}
                        }
                    }
                }
            }
            TABLE_SORTABLE_CLASS("table") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() {out << "user";}
                        TABLEH() {out << "offset";}
                        TABLEH() {out << "lag";}
                        TABLEH() {out << "ReadFromTimestamp";}
                        TABLEH() {out << "WriteTimestamp";}
                        TABLEH() {out << "CreateTimestamp";}
                        TABLEH() {out << "ReadOffset";}
                        TABLEH() {out << "ReadWriteTimestamp";}
                        TABLEH() {out << "ReadCreateTimestamp";}
                        TABLEH() {out << "ReadOffsetRewindSum";}
                        TABLEH() {out << "ActiveReads";}
                        TABLEH() {out << "Subscriptions";}
                    }
                }
                TABLEBODY() {
                    for (auto& d: UsersInfoStorage->GetAll()) {
                        TABLER() {
                            TABLED() {out << EncodeHtmlPcdata(d.first);}
                            TABLED() {out << d.second.Offset;}
                            TABLED() {out << (EndOffset - d.second.Offset);}
                            TABLED() {out << ToStringLocalTimeUpToSeconds(d.second.ReadFromTimestamp);}
                            TABLED() {out << ToStringLocalTimeUpToSeconds(d.second.WriteTimestamp);}
                            TABLED() {out << ToStringLocalTimeUpToSeconds(d.second.CreateTimestamp);}
                            TABLED() {out << (d.second.GetReadOffset());}
                            TABLED() {out << ToStringLocalTimeUpToSeconds(d.second.GetReadWriteTimestamp());}
                            TABLED() {out << ToStringLocalTimeUpToSeconds(d.second.GetReadCreateTimestamp());}
                            TABLED() {out << (d.second.ReadOffsetRewindSum);}
                            TABLED() {out << d.second.ActiveReads;}
                            TABLED() {out << d.second.Subscriptions;}
                        }
                    }
                }
            }
        }
    }

    ctx.Send(ev->Sender, new TEvPQ::TEvMonResponse(Partition, res, out.Str()));
}

void TPartition::RequestConfig(const TActorContext& ctx)
{
    auto event = MakeHolder<TEvKeyValue::TEvRequest>();
    auto read = event->Record.AddCmdRead();
    read->SetKey(GetKeyConfig());
    ctx.Send(Tablet, event.Release());
}

void TPartition::HandleConfig(const NKikimrClient::TResponse& res, const TActorContext& ctx)
{
    auto& response = res.GetReadResult(0);

    switch (response.GetStatus()) {
    case NKikimrProto::OK:
        Y_VERIFY(Config.ParseFromString(response.GetValue()));
        Y_VERIFY(Config.GetVersion() <= TabletConfig.GetVersion());
        if (Config.GetVersion() < TabletConfig.GetVersion()) {
            auto event = MakeHolder<TEvPQ::TEvChangePartitionConfig>(TopicConverter,
                                                                     TabletConfig);
            PushFrontDistrTx(event.Release());
        }
        break;
    case NKikimrProto::NODATA:
        Config = TabletConfig;
        break;
    case NKikimrProto::ERROR:
        LOG_ERROR_S(ctx, NKikimrServices::PERSQUEUE,
                    "Partition " << Partition << " can't read config");
        ctx.Send(Tablet, new TEvents::TEvPoisonPill());
        break;
    default:
        Cerr << "ERROR " << response.GetStatus() << "\n";
        Y_FAIL("bad status");
    };

    InitState = WaitDiskStatus;
    Initialize(ctx);
}

void TPartition::Bootstrap(const TActorContext& ctx) {
    Y_VERIFY(InitState == WaitConfig);
    RequestConfig(ctx);
    Become(&TThis::StateInit);
}

void TPartition::Initialize(const TActorContext& ctx) {
    CreationTime = ctx.Now();
    WriteCycleStartTime = ctx.Now();
    WriteQuota.ConstructInPlace(Config.GetPartitionConfig().GetBurstSize(),
                                Config.GetPartitionConfig().GetWriteSpeedInBytesPerSecond(),
                                ctx.Now());
    WriteTimestamp = ctx.Now();
    LastUsedStorageMeterTimestamp = ctx.Now();
    WriteTimestampEstimate = ManageWriteTimestampEstimate ? ctx.Now() : TInstant::Zero();

    CloudId = Config.GetYcCloudId();
    DbId = Config.GetYdbDatabaseId();
    DbPath = Config.GetYdbDatabasePath();
    FolderId = Config.GetYcFolderId();

    CalcTopicWriteQuotaParams(AppData()->PQConfig,
                              IsLocalDC,
                              TopicConverter,
                              TabletID,
                              ctx,
                              TopicWriteQuoterPath,
                              TopicWriteQuotaResourcePath);

    UsersInfoStorage.ConstructInPlace(DCId,
                                      TabletID,
                                      TopicConverter,
                                      Partition,
                                      Counters,
                                      Config,
                                      CloudId,
                                      DbId,
                                      Config.GetYdbDatabasePath(),
                                      IsServerless,
                                      FolderId);
    TotalChannelWritesByHead.resize(Config.GetPartitionConfig().GetNumChannels());

    if (Config.GetPartitionConfig().HasMirrorFrom()) {
        ManageWriteTimestampEstimate = !Config.GetPartitionConfig().GetMirrorFrom().GetSyncWriteTime();
    } else {
        ManageWriteTimestampEstimate = IsLocalDC;
    }

    if (AppData()->PQConfig.GetTopicsAreFirstClassCitizen()) {
        PartitionCountersLabeled.Reset(new TPartitionLabeledCounters(EscapeBadChars(TopicConverter->GetClientsideName()),
                                                                     Partition,
                                                                     Config.GetYdbDatabasePath()));
    } else {
        PartitionCountersLabeled.Reset(new TPartitionLabeledCounters(TopicConverter->GetClientsideName(),
                                                                     Partition));
    }

    UsersInfoStorage->Init(Tablet, SelfId(), ctx);

    Y_VERIFY(AppData(ctx)->PQConfig.GetMaxBlobsPerLevel() > 0);
    ui32 border = LEVEL0;
    MaxSizeCheck = 0;
    MaxBlobSize = AppData(ctx)->PQConfig.GetMaxBlobSize();
    PartitionedBlob = TPartitionedBlob(Partition, 0, 0, 0, 0, 0, Head, NewHead, true, false, MaxBlobSize);
    for (ui32 i = 0; i < TotalLevels; ++i) {
        CompactLevelBorder.push_back(border);
        MaxSizeCheck += border;
        Y_VERIFY(i + 1 < TotalLevels && border < MaxBlobSize || i + 1 == TotalLevels && border == MaxBlobSize);
        border *= AppData(ctx)->PQConfig.GetMaxBlobsPerLevel();
        border = Min(border, MaxBlobSize);
    }
    TotalMaxCount = AppData(ctx)->PQConfig.GetMaxBlobsPerLevel() * TotalLevels;

    std::reverse(CompactLevelBorder.begin(), CompactLevelBorder.end());

    for (ui32 i = 0; i < TotalLevels; ++i) {
        DataKeysHead.push_back(TKeyLevel(CompactLevelBorder[i]));
    }

    for (const auto& readQuota : Config.GetPartitionConfig().GetReadQuota()) {
        auto &userInfo = UsersInfoStorage->GetOrCreate(readQuota.GetClientId(), ctx);
        userInfo.ReadQuota.UpdateConfig(readQuota.GetBurstSize(), readQuota.GetSpeedInBytesPerSecond());
    }

    LOG_INFO_S(ctx, NKikimrServices::PERSQUEUE, "bootstrapping " << Partition << " " << ctx.SelfID);

    if (NewPartition) {
        InitComplete(ctx);
    } else {
        Y_VERIFY(InitState == WaitDiskStatus);
        RequestDiskStatus(ctx, Tablet, Config.GetPartitionConfig().GetNumChannels());
        Become(&TThis::StateInit);
    }

    if (AppData(ctx)->Counters) {
        if (AppData()->PQConfig.GetTopicsAreFirstClassCitizen()) {
            SetupStreamCounters(ctx);
        } else {
            SetupTopicCounters(ctx);
        }
    }
}

void TPartition::EmplaceResponse(TMessage&& message, const TActorContext& ctx) {
    Responses.emplace_back(
        message.Body,
        WriteQuota->GetQuotedTime(ctx.Now()) - message.QuotedTime,
        (ctx.Now() - TInstant::Zero()) - message.QueueTime,
        ctx.Now()
    );
}

void TPartition::SetupTopicCounters(const TActorContext& ctx) {
    auto counters = AppData(ctx)->Counters;
    auto labels = NPersQueue::GetLabels(TopicConverter);
    const TString suffix = IsLocalDC ? "Original" : "Mirrored";

    WriteBufferIsFullCounter.SetCounter(
        NPersQueue::GetCounters(counters, "writingTime", TopicConverter),
            {{"host", DCId},
            {"Partition", ToString<ui32>(Partition)}},
            {"sensor", "BufferFullTime" + suffix, true});

    auto subGroup = GetServiceCounters(counters, "pqproxy|writeTimeLag");
    InputTimeLag = THolder<NKikimr::NPQ::TPercentileCounter>(new NKikimr::NPQ::TPercentileCounter(
        subGroup, labels, {{"sensor", "TimeLags" + suffix}}, "Interval",
        TVector<std::pair<ui64, TString>>{
            {100, "100ms"}, {200, "200ms"}, {500, "500ms"}, {1000, "1000ms"},
            {2000, "2000ms"}, {5000, "5000ms"}, {10'000, "10000ms"}, {30'000, "30000ms"},
            {60'000, "60000ms"}, {180'000,"180000ms"}, {9'999'999, "999999ms"}}, true));


    subGroup = GetServiceCounters(counters, "pqproxy|writeInfo");
    MessageSize = THolder<NKikimr::NPQ::TPercentileCounter>(new NKikimr::NPQ::TPercentileCounter(
        subGroup, labels, {{"sensor", "MessageSize" + suffix}}, "Size",
        TVector<std::pair<ui64, TString>>{
            {1_KB, "1kb"}, {5_KB, "5kb"}, {10_KB, "10kb"},
            {20_KB, "20kb"}, {50_KB, "50kb"}, {100_KB, "100kb"}, {200_KB, "200kb"},
            {512_KB, "512kb"},{1024_KB, "1024kb"}, {2048_KB,"2048kb"}, {5120_KB, "5120kb"},
            {10240_KB, "10240kb"}, {65536_KB, "65536kb"}, {999'999'999, "99999999kb"}}, true));

    subGroup = GetServiceCounters(counters, "pqproxy|writeSession");
    BytesWrittenTotal = NKikimr::NPQ::TMultiCounter(subGroup, labels, {}, {"BytesWritten" + suffix}, true);
    BytesWrittenUncompressed = NKikimr::NPQ::TMultiCounter(subGroup, labels, {}, {"UncompressedBytesWritten" + suffix}, true);
    BytesWrittenComp = NKikimr::NPQ::TMultiCounter(subGroup, labels, {}, {"CompactedBytesWritten" + suffix}, true);
    MsgsWrittenTotal = NKikimr::NPQ::TMultiCounter(subGroup, labels, {}, {"MessagesWritten" + suffix}, true);

    TVector<NPersQueue::TPQLabelsInfo> aggr = {{{{"Account", TopicConverter->GetAccount()}}, {"total"}}};
    ui32 border = AppData(ctx)->PQConfig.GetWriteLatencyBigMs();
    subGroup = GetServiceCounters(counters, "pqproxy|SLI");
    WriteLatency = NKikimr::NPQ::CreateSLIDurationCounter(subGroup, aggr, "Write", border,
                                                          {100, 200, 500, 1000, 1500, 2000,
                                                           5000, 10'000, 30'000, 99'999'999});
    SLIBigLatency = NKikimr::NPQ::TMultiCounter(subGroup, aggr, {}, {"WriteBigLatency"}, true, "sensor", false);
    WritesTotal = NKikimr::NPQ::TMultiCounter(subGroup, aggr, {}, {"WritesTotal"}, true, "sensor", false);
    if (IsQuotingEnabled() && !TopicWriteQuotaResourcePath.empty()) {
        TopicWriteQuotaWaitCounter = THolder<NKikimr::NPQ::TPercentileCounter>(
            new NKikimr::NPQ::TPercentileCounter(
                GetServiceCounters(counters, "pqproxy|topicWriteQuotaWait"), labels,
                    {{"sensor", "TopicWriteQuotaWait" + suffix}}, "Interval",
                        TVector<std::pair<ui64, TString>>{
                            {0, "0ms"}, {1, "1ms"}, {5, "5ms"}, {10, "10ms"},
                            {20, "20ms"}, {50, "50ms"}, {100, "100ms"}, {500, "500ms"},
                            {1000, "1000ms"}, {2500, "2500ms"}, {5000, "5000ms"},
                            {10'000, "10000ms"}, {9'999'999, "999999ms"}}, true));
    }

    PartitionWriteQuotaWaitCounter = THolder<NKikimr::NPQ::TPercentileCounter>(
        new NKikimr::NPQ::TPercentileCounter(GetServiceCounters(counters, "pqproxy|partitionWriteQuotaWait"),
            labels, {{"sensor", "PartitionWriteQuotaWait" + suffix}}, "Interval",
                TVector<std::pair<ui64, TString>>{
                    {0, "0ms"}, {1, "1ms"}, {5, "5ms"}, {10, "10ms"},
                    {20, "20ms"}, {50, "50ms"}, {100, "100ms"}, {500, "500ms"},
                    {1000, "1000ms"}, {2500, "2500ms"}, {5000, "5000ms"},
                    {10'000, "10000ms"}, {9'999'999, "999999ms"}}, true));
}

void TPartition::SetupStreamCounters(const TActorContext& ctx) {
    const auto topicName = TopicConverter->GetModernName();
    auto counters = AppData(ctx)->Counters;
    auto subgroups = NPersQueue::GetSubgroupsForTopic(TopicConverter, CloudId, DbId, DbPath, FolderId);
/*
    WriteBufferIsFullCounter.SetCounter(
        NPersQueue::GetCountersForTopic(counters, IsServerless),
        {
         {"database", DbPath},
         {"cloud_id", CloudId},
         {"folder_id", FolderId},
         {"database_id", DbId},
         {"topic", TopicConverter->GetFederationPath()},
         {"host", DCId},
         {"partition", ToString<ui32>(Partition)}},
        {"name", "api.grpc.topic.stream_write.buffer_brimmed_milliseconds", true});
*/

    subgroups.push_back({"name", "topic.write.lag_milliseconds"});

    InputTimeLag = THolder<NKikimr::NPQ::TPercentileCounter>(new NKikimr::NPQ::TPercentileCounter(
        NPersQueue::GetCountersForTopic(counters, IsServerless), {},
                    subgroups, "bin",
                    TVector<std::pair<ui64, TString>>{
                        {100, "100"}, {200, "200"}, {500, "500"},
                        {1000, "1000"}, {2000, "2000"}, {5000, "5000"},
                        {10'000, "10000"}, {30'000, "30000"}, {60'000, "60000"},
                        {180'000,"180000"}, {9'999'999, "999999"}}, true));

    subgroups.back().second = "topic.write.message_size_bytes";
    MessageSize = THolder<NKikimr::NPQ::TPercentileCounter>(new NKikimr::NPQ::TPercentileCounter(
        NPersQueue::GetCountersForTopic(counters, IsServerless), {},
                    subgroups, "bin",
                    TVector<std::pair<ui64, TString>>{
                        {1024, "1024"}, {5120, "5120"}, {10'240, "10240"},
                        {20'480, "20480"}, {51'200, "51200"}, {102'400, "102400"},
                        {204'800, "204800"}, {524'288, "524288"},{1'048'576, "1048576"},
                        {2'097'152,"2097152"}, {5'242'880, "5242880"}, {10'485'760, "10485760"},
                        {67'108'864, "67108864"}, {999'999'999, "99999999"}}, true));

    subgroups.pop_back();
    BytesWrittenGrpc = NKikimr::NPQ::TMultiCounter(
        NPersQueue::GetCountersForTopic(counters, IsServerless), {}, subgroups,
                    {"api.grpc.topic.stream_write.bytes"} , true, "name");
    BytesWrittenTotal = NKikimr::NPQ::TMultiCounter(
        NPersQueue::GetCountersForTopic(counters, IsServerless), {}, subgroups,
                    {"topic.write.bytes"} , true, "name");

    MsgsWrittenGrpc = NKikimr::NPQ::TMultiCounter(
        NPersQueue::GetCountersForTopic(counters, IsServerless), {}, subgroups,
                    {"api.grpc.topic.stream_write.messages"}, true, "name");
    MsgsWrittenTotal = NKikimr::NPQ::TMultiCounter(
        NPersQueue::GetCountersForTopic(counters, IsServerless), {}, subgroups,
                    {"topic.write.messages"}, true, "name");


    BytesWrittenUncompressed = NKikimr::NPQ::TMultiCounter(

        NPersQueue::GetCountersForTopic(counters, IsServerless), {}, subgroups,
                    {"topic.write.uncompressed_bytes"}, true, "name");

    TVector<NPersQueue::TPQLabelsInfo> aggr = {{{{"Account", TopicConverter->GetAccount()}}, {"total"}}};
    ui32 border = AppData(ctx)->PQConfig.GetWriteLatencyBigMs();
    auto subGroup = GetServiceCounters(counters, "pqproxy|SLI");
    WriteLatency = NKikimr::NPQ::CreateSLIDurationCounter(subGroup, aggr, "Write", border,
                                                          {100, 200, 500, 1000, 1500, 2000,
                                                           5000, 10'000, 30'000, 99'999'999});
    SLIBigLatency = NKikimr::NPQ::TMultiCounter(subGroup, aggr, {}, {"WriteBigLatency"}, true, "name", false);
    WritesTotal = NKikimr::NPQ::TMultiCounter(subGroup, aggr, {}, {"WritesTotal"}, true, "name", false);
    if (IsQuotingEnabled() && !TopicWriteQuotaResourcePath.empty()) {
        subgroups.push_back({"name", "api.grpc.topic.stream_write.topic_throttled_milliseconds"});
        TopicWriteQuotaWaitCounter = THolder<NKikimr::NPQ::TPercentileCounter>(
            new NKikimr::NPQ::TPercentileCounter(
                NPersQueue::GetCountersForTopic(counters, IsServerless), {},
                            subgroups, "bin",
                            TVector<std::pair<ui64, TString>>{
                                {0, "0"}, {1, "1"}, {5, "5"}, {10, "10"},
                                {20, "20"}, {50, "50"}, {100, "100"}, {500, "500"},
                                {1000, "1000"}, {2500, "2500"}, {5000, "5000"},
                                {10'000, "10000"}, {9'999'999, "999999"}}, true));
        subgroups.pop_back();
    }

    subgroups.push_back({"name", "api.grpc.topic.stream_write.partition_throttled_milliseconds"});
    PartitionWriteQuotaWaitCounter = THolder<NKikimr::NPQ::TPercentileCounter>(
        new NKikimr::NPQ::TPercentileCounter(
            NPersQueue::GetCountersForTopic(counters, IsServerless), {}, subgroups, "bin",
                        TVector<std::pair<ui64, TString>>{
                            {0, "0"}, {1, "1"}, {5, "5"}, {10, "10"},
                            {20, "20"}, {50, "50"}, {100, "100"}, {500, "500"},
                            {1000, "1000"}, {2500, "2500"}, {5000, "5000"},
                            {10'000, "10000"}, {9'999'999, "999999"}}, true));
}

void TPartition::ProcessHasDataRequests(const TActorContext& ctx) {
    if (!InitDone)
        return;
    for (auto it = HasDataRequests.begin(); it != HasDataRequests.end();) {
        if (it->Offset < EndOffset) {
            TAutoPtr<TEvPersQueue::TEvHasDataInfoResponse> res(new TEvPersQueue::TEvHasDataInfoResponse());
            res->Record.SetEndOffset(EndOffset);
            res->Record.SetSizeLag(GetSizeLag(it->Offset));
            res->Record.SetWriteTimestampEstimateMS(WriteTimestampEstimate.MilliSeconds());
            if (it->Cookie)
                res->Record.SetCookie(*(it->Cookie));
            ctx.Send(it->Sender, res.Release());
            if (!it->ClientId.empty()) {
                auto& userInfo = UsersInfoStorage->GetOrCreate(it->ClientId, ctx);
                userInfo.ForgetSubscription(ctx.Now());
            }
            it = HasDataRequests.erase(it);
        } else {
            break;
        }
    }
    for (auto it = HasDataDeadlines.begin(); it != HasDataDeadlines.end();) {
        if (it->Deadline <= ctx.Now()) {
            auto jt = HasDataRequests.find(it->Request);
            if (jt != HasDataRequests.end()) {
                TAutoPtr<TEvPersQueue::TEvHasDataInfoResponse> res(new TEvPersQueue::TEvHasDataInfoResponse());
                res->Record.SetEndOffset(EndOffset);
                res->Record.SetSizeLag(0);
                res->Record.SetWriteTimestampEstimateMS(WriteTimestampEstimate.MilliSeconds());
                if (it->Request.Cookie)
                    res->Record.SetCookie(*(it->Request.Cookie));
                ctx.Send(it->Request.Sender, res.Release());
                if (!it->Request.ClientId.empty()) {
                    auto& userInfo = UsersInfoStorage->GetOrCreate(it->Request.ClientId, ctx);
                    userInfo.ForgetSubscription(ctx.Now());
                }
                HasDataRequests.erase(jt);
            }
            it = HasDataDeadlines.erase(it);
        } else {
            break;
        }
    }
}


void TPartition::UpdateAvailableSize(const TActorContext& ctx) {
    FilterDeadlinedWrites(ctx);

    auto now = ctx.Now();
    WriteQuota->Update(now);
    for (auto &c : UsersInfoStorage->GetAll()) {
        while (true) {
            if (!c.second.ReadQuota.CanExaust(now) && !c.second.ReadRequests.empty()) {
                break;
            }
            if (!c.second.ReadRequests.empty()) {
                auto ri(std::move(c.second.ReadRequests.front().first));
                auto cookie = c.second.ReadRequests.front().second;
                c.second.ReadRequests.pop_front();
                ProcessRead(ctx, std::move(ri), cookie, false);
            } else
                break;
        }
    }
    ScheduleUpdateAvailableSize(ctx);
}

void TPartition::HandleOnIdle(TEvPQ::TEvUpdateAvailableSize::TPtr&, const TActorContext& ctx) {
    UpdateAvailableSize(ctx);
    HandleWrites(ctx);
}

void TPartition::HandleOnWrite(TEvPQ::TEvUpdateAvailableSize::TPtr&, const TActorContext& ctx) {
    UpdateAvailableSize(ctx);
}


ui64 TPartition::MeteringDataSize(const TActorContext& ctx) const {
    ui64 size = Size();
    if (!DataKeysBody.empty()) {
        size -= DataKeysBody.front().Size;
    }
    auto expired = ctx.Now() - TDuration::Seconds(Config.GetPartitionConfig().GetLifetimeSeconds());
    for(size_t i = 0; i < HeadKeys.size(); ++i) {
        auto& key = HeadKeys[i];
        if (expired < key.Timestamp) {
            break;
        }
        size -= key.Size;
    }
    Y_VERIFY(size >= 0, "Metering data size must be positive");
    return size;
}

ui64 TPartition::GetUsedStorage(const TActorContext& ctx) {
    auto duration = ctx.Now() - LastUsedStorageMeterTimestamp;
    LastUsedStorageMeterTimestamp = ctx.Now();
    ui64 size = MeteringDataSize(ctx);
    return size * duration.MilliSeconds() / 1000 / 1_MB; // mb*seconds
}


void TPartition::HandleWakeup(const TActorContext& ctx) {
    FilterDeadlinedWrites(ctx);

    ctx.Schedule(WAKE_TIMEOUT, new TEvents::TEvWakeup());
    ctx.Send(Tablet, new TEvPQ::TEvPartitionCounters(Partition, TabletCounters));

    ui64 usedStorage = GetUsedStorage(ctx);
    if (usedStorage > 0) {
        ctx.Send(Tablet, new TEvPQ::TEvMetering(EMeteringJson::UsedStorageV1, usedStorage));
    }

    ReportCounters(ctx);

    ProcessHasDataRequests(ctx);

    auto now = ctx.Now();
    for (auto& userInfo : UsersInfoStorage->GetAll()) {
        userInfo.second.UpdateReadingTimeAndState(now);
        for (auto& avg : userInfo.second.AvgReadBytes) {
            avg.Update(now);
        }
    }
    WriteBufferIsFullCounter.UpdateWorkingTime(now);

    WriteLagMs.Update(0, now);

    for (auto& avg : AvgWriteBytes) {
        avg.Update(now);
    }
    for (auto& avg : AvgQuotaBytes) {
        avg.Update(now);
    }

    if (CurrentStateFunc() == &TThis::StateWrite) {//Write will handle all itself
        return;
    }
    Y_VERIFY(CurrentStateFunc() == &TThis::StateIdle);

    if (ManageWriteTimestampEstimate)
        WriteTimestampEstimate = now;

    THolder <TEvKeyValue::TEvRequest> request = MakeHolder<TEvKeyValue::TEvRequest>();
    bool haveChanges = CleanUp(request.Get(), false, ctx);
    if (DiskIsFull) {
        AddCheckDiskRequest(request.Get(), Config.GetPartitionConfig().GetNumChannels());
        haveChanges = true;
    }

    if (haveChanges) {
        WriteCycleStartTime = ctx.Now();
        WriteStartTime = ctx.Now();
        TopicQuotaWaitTimeForCurrentBlob = TDuration::Zero();
        WritesTotal.Inc();
        Become(&TThis::StateWrite);
        AddMetaKey(request.Get());
        ctx.Send(Tablet, request.Release());
    }
}

void TPartition::AddMetaKey(TEvKeyValue::TEvRequest* request) {
    //Set Start Offset
    auto write = request->Record.AddCmdWrite();
    TKeyPrefix ikey(TKeyPrefix::TypeMeta, Partition);

    NKikimrPQ::TPartitionMeta meta;
    meta.SetStartOffset(StartOffset);
    meta.SetEndOffset(Max(NewHead.GetNextOffset(), EndOffset));
    meta.SetSubDomainOutOfSpace(SubDomainOutOfSpace);

    TString out;
    Y_PROTOBUF_SUPPRESS_NODISCARD meta.SerializeToString(&out);

    write->SetKey(ikey.Data(), ikey.Size());
    write->SetValue(out.c_str(), out.size());
    write->SetStorageChannel(NKikimrClient::TKeyValueRequest::INLINE);

}

bool TPartition::CleanUp(TEvKeyValue::TEvRequest* request, bool hasWrites, const TActorContext& ctx) {
    bool haveChanges = CleanUpBlobs(request, hasWrites, ctx);
    LOG_DEBUG(ctx, NKikimrServices::PERSQUEUE, TStringBuilder() << "Have " <<
              request->Record.CmdDeleteRangeSize() << " items to delete old stuff");

    haveChanges |= SourceIdStorage.DropOldSourceIds(request, ctx.Now(), StartOffset, Partition,
                                                    Config.GetPartitionConfig());
    if (haveChanges) {
        SourceIdStorage.MarkOwnersForDeletedSourceId(Owners);
    }
    LOG_DEBUG(ctx, NKikimrServices::PERSQUEUE, TStringBuilder() << "Have " <<
              request->Record.CmdDeleteRangeSize() << " items to delete all stuff");
    LOG_TRACE(ctx, NKikimrServices::PERSQUEUE, TStringBuilder() << "Delete command " << request->ToString());

    return haveChanges;
}

bool TPartition::CleanUpBlobs(TEvKeyValue::TEvRequest *request, bool hasWrites, const TActorContext& ctx) {
    if (StartOffset == EndOffset || DataKeysBody.size() <= 1)
        return false;

    const auto& partConfig = Config.GetPartitionConfig();
    ui64 minOffset = EndOffset;
    for (const auto& importantClientId : partConfig.GetImportantClientId()) {
        TUserInfo* userInfo = UsersInfoStorage->GetIfExists(importantClientId);
        ui64 curOffset = StartOffset;
        if (userInfo && userInfo->Offset >= 0) //-1 means no offset
            curOffset = userInfo->Offset;
        minOffset = Min<ui64>(minOffset, curOffset);
    }

    bool hasDrop = false;
    ui64 endOffset = StartOffset;

    const std::optional<ui64> storageLimit = partConfig.HasStorageLimitBytes()
        ? std::optional<ui64>{partConfig.GetStorageLimitBytes()} : std::nullopt;
    const TDuration lifetimeLimit{TDuration::Seconds(partConfig.GetLifetimeSeconds())};

    if (DataKeysBody.size() > 1) {
        auto retentionCondition = [&]() -> bool {
            const auto bodySize = BodySize - DataKeysBody.front().Size;
            const bool timeRetention = (ctx.Now() >= (DataKeysBody.front().Timestamp + lifetimeLimit));
            return storageLimit.has_value()
                ? ((bodySize >= *storageLimit) || timeRetention)
                : timeRetention;
        };

        while (DataKeysBody.size() > 1 &&
               retentionCondition() &&
               (minOffset > DataKeysBody[1].Key.GetOffset() ||
                (minOffset == DataKeysBody[1].Key.GetOffset() &&
                 DataKeysBody[1].Key.GetPartNo() == 0))) { // all offsets from blob[0] are readed, and don't delete last blob
            BodySize -= DataKeysBody.front().Size;

            DataKeysBody.pop_front();
            if (!GapOffsets.empty() && DataKeysBody.front().Key.GetOffset() == GapOffsets.front().second) {
                GapSize -= GapOffsets.front().second - GapOffsets.front().first;
                GapOffsets.pop_front();
            }
            hasDrop = true;
        }

        Y_VERIFY(!DataKeysBody.empty());

        endOffset = DataKeysBody.front().Key.GetOffset();
        if (DataKeysBody.front().Key.GetPartNo() > 0) {
            ++endOffset;
        }
    }

    TDataKey lastKey = HeadKeys.empty() ? DataKeysBody.back() : HeadKeys.back();

    if (!hasWrites &&
        ctx.Now() >= lastKey.Timestamp + lifetimeLimit &&
        minOffset == EndOffset &&
        false) { // disable drop of all data
        Y_VERIFY(!HeadKeys.empty() || !DataKeysBody.empty());

        Y_VERIFY(CompactedKeys.empty());
        Y_VERIFY(NewHead.PackedSize == 0);
        Y_VERIFY(NewHeadKey.Size == 0);

        Y_VERIFY(EndOffset == Head.GetNextOffset());
        Y_VERIFY(EndOffset == NewHead.GetNextOffset() || NewHead.GetNextOffset() == 0);

        hasDrop = true;

        BodySize = 0;
        DataKeysBody.clear();
        GapSize = 0;
        GapOffsets.clear();

        for (ui32 i = 0; i < TotalLevels; ++i) {
            DataKeysHead[i].Clear();
        }
        HeadKeys.clear();
        Head.Clear();
        Head.Offset = EndOffset;
        NewHead.Clear();
        NewHead.Offset = EndOffset;
        endOffset = EndOffset;
    } else {
        if (hasDrop) {
            lastKey = DataKeysBody.front();
        }
    }

    if (!hasDrop)
        return false;

    StartOffset = endOffset;

    TKey key(TKeyPrefix::TypeData, Partition, 0, 0, 0, 0); //will drop all that could not be dropped before of case of full disks

    auto del = request->Record.AddCmdDeleteRange();
    auto range = del->MutableRange();
    range->SetFrom(key.Data(), key.Size());
    range->SetIncludeFrom(true);
    range->SetTo(lastKey.Key.Data(), lastKey.Key.Size());
    range->SetIncludeTo(StartOffset == EndOffset);

    return true;
}

void TPartition::Handle(TEvPersQueue::TEvHasDataInfo::TPtr& ev, const TActorContext& ctx) {
    auto& record = ev->Get()->Record;
    Y_VERIFY(record.HasSender());

    TActorId sender = ActorIdFromProto(record.GetSender());
    if (InitDone && EndOffset > (ui64)record.GetOffset()) { //already has data, answer right now
        TAutoPtr<TEvPersQueue::TEvHasDataInfoResponse> res(new TEvPersQueue::TEvHasDataInfoResponse());
        res->Record.SetEndOffset(EndOffset);
        res->Record.SetSizeLag(GetSizeLag(record.GetOffset()));
        res->Record.SetWriteTimestampEstimateMS(WriteTimestampEstimate.MilliSeconds());
        if (record.HasCookie())
            res->Record.SetCookie(record.GetCookie());
        ctx.Send(sender, res.Release());
        return;
    } else {
        THasDataReq req{++HasDataReqNum, (ui64)record.GetOffset(), sender, record.HasCookie() ? TMaybe<ui64>(record.GetCookie()) : TMaybe<ui64>(),
                                                                                        record.HasClientId() && InitDone ? record.GetClientId() : ""};
        THasDataDeadline dl{TInstant::MilliSeconds(record.GetDeadline()), req};
        auto res = HasDataRequests.insert(req);
        HasDataDeadlines.insert(dl);
        Y_VERIFY(res.second);

        if (InitDone && record.HasClientId() && !record.GetClientId().empty()) {
            auto& userInfo = UsersInfoStorage->GetOrCreate(record.GetClientId(), ctx);
            ++userInfo.Subscriptions;
            userInfo.UpdateReadOffset((i64)EndOffset - 1, ctx.Now(), ctx.Now(), ctx.Now());
            userInfo.UpdateReadingTimeAndState(ctx.Now());
        }
    }
}

void TPartition::Handle(TEvPQ::TEvMirrorerCounters::TPtr& ev, const TActorContext& /*ctx*/) {
    if (Mirrorer) {
        auto diff = ev->Get()->Counters.MakeDiffForAggr(Mirrorer->Baseline);
        TabletCounters.Populate(*diff.Get());
        ev->Get()->Counters.RememberCurrentStateAsBaseline(Mirrorer->Baseline);
    }
}

void TPartition::Handle(NReadSpeedLimiterEvents::TEvCounters::TPtr& ev, const TActorContext& /*ctx*/) {
    auto userInfo = UsersInfoStorage->GetIfExists(ev->Get()->User);
    if (userInfo && userInfo->ReadSpeedLimiter) {
        auto diff = ev->Get()->Counters.MakeDiffForAggr(userInfo->ReadSpeedLimiter->Baseline);
        TabletCounters.Populate(*diff.Get());
        ev->Get()->Counters.RememberCurrentStateAsBaseline(userInfo->ReadSpeedLimiter->Baseline);
    }
}

void TPartition::Handle(TEvents::TEvPoisonPill::TPtr&, const TActorContext& ctx) {
    // Reply to all outstanding requests in order to destroy corresponding actors

    TStringBuilder ss;
    ss << "Tablet is restarting, topic '" << TopicConverter->GetClientsideName() << "'";

    for (const auto& ev : WaitToChangeOwner) {
        ReplyError(ctx, ev->Cookie, NPersQueue::NErrorCode::INITIALIZING, ss);
    }

    for (const auto& w : Requests) {
        ReplyError(ctx, w.GetCookie(), NPersQueue::NErrorCode::INITIALIZING, ss);
    }

    for (const auto& wr : Responses) {
        ReplyError(ctx, wr.GetCookie(), NPersQueue::NErrorCode::INITIALIZING, TStringBuilder() << ss << " (WriteResponses)");
    }

    for (const auto& ri : ReadInfo) {
        ReplyError(ctx, ri.second.Destination, NPersQueue::NErrorCode::INITIALIZING,
            TStringBuilder() << ss << " (ReadInfo) cookie " << ri.first);
    }

    if (Mirrorer) {
        Send(Mirrorer->Actor, new TEvents::TEvPoisonPill());
    }

    if (UsersInfoStorage.Defined()) {
        UsersInfoStorage->Clear(ctx);
    }

    Die(ctx);
}

void TPartition::CancelAllWritesOnIdle(const TActorContext& ctx) {
    for (const auto& w : Requests) {
        ReplyError(ctx, w.GetCookie(), NPersQueue::NErrorCode::WRITE_ERROR_DISK_IS_FULL, "Disk is full");
        if (w.IsWrite()) {
            const auto& msg = w.GetWrite().Msg;
            TabletCounters.Cumulative()[COUNTER_PQ_WRITE_ERROR].Increment(1);
            TabletCounters.Cumulative()[COUNTER_PQ_WRITE_BYTES_ERROR].Increment(msg.Data.size() + msg.SourceId.size());
            WriteInflightSize -= msg.Data.size();
        }
    }

    UpdateWriteBufferIsFullState(ctx.Now());
    Requests.clear();
    Y_VERIFY(Responses.empty());

    ProcessReserveRequests(ctx);
}


void TPartition::FailBadClient(const TActorContext& ctx) {
    for (auto it = Owners.begin(); it != Owners.end();) {
        it = DropOwner(it, ctx);
    }
    Y_VERIFY(Owners.empty());
    Y_VERIFY(ReservedSize == 0);

    for (const auto& w : Requests) {
        ReplyError(ctx, w.GetCookie(), NPersQueue::NErrorCode::BAD_REQUEST, "previous write request failed");
        if (w.IsWrite()) {
            const auto& msg = w.GetWrite().Msg;
            TabletCounters.Cumulative()[COUNTER_PQ_WRITE_ERROR].Increment(1);
            TabletCounters.Cumulative()[COUNTER_PQ_WRITE_BYTES_ERROR].Increment(msg.Data.size() + msg.SourceId.size());
            WriteInflightSize -= msg.Data.size();
        }
    }
    UpdateWriteBufferIsFullState(ctx.Now());
    Requests.clear();
    for (const auto& w : Responses) {
        ReplyError(ctx, w.GetCookie(), NPersQueue::NErrorCode::BAD_REQUEST, "previous write request failed");
        if (w.IsWrite())
            TabletCounters.Cumulative()[COUNTER_PQ_WRITE_ERROR].Increment(1);
    }
    TabletCounters.Cumulative()[COUNTER_PQ_WRITE_BYTES_ERROR].Increment(WriteNewSize);
    Responses.clear();

    ProcessChangeOwnerRequests(ctx);
    ProcessReserveRequests(ctx);
}


bool CheckDiskStatus(const TStorageStatusFlags status) {
    return !status.Check(NKikimrBlobStorage::StatusDiskSpaceLightYellowMove);
}

void TPartition::HandleGetDiskStatus(const NKikimrClient::TResponse& response, const TActorContext& ctx) {
    bool diskIsOk = true;
    for (ui32 i = 0; i < response.GetStatusResultSize(); ++i) {
        auto& res = response.GetGetStatusResult(i);

        if (res.GetStatus() != NKikimrProto::OK) {
            LOG_ERROR_S(
                    ctx, NKikimrServices::PERSQUEUE,
                    "commands for topic '" << TopicConverter->GetClientsideName() << "' partition " << Partition <<
                        " are not processed at all, got KV error in CmdGetStatus " << res.GetStatus()
            );
            ctx.Send(Tablet, new TEvents::TEvPoisonPill());
            return;
        }
        diskIsOk = diskIsOk && CheckDiskStatus(res.GetStatusFlags());
    }
    DiskIsFull = !diskIsOk;
    if (DiskIsFull) {
        LogAndCollectError(NKikimrServices::PERSQUEUE, "disk is full", ctx);
    }

    InitState = WaitMetaRead;
    RequestMetaRead(ctx, Tablet, Partition);
}

void TPartition::HandleMetaRead(const NKikimrClient::TResponse& response, const TActorContext& ctx)
{
    auto handleReadResult = [&](const NKikimrClient::TKeyValueResponse::TReadResult& response,
                                auto&& action) {
        switch (response.GetStatus()) {
        case NKikimrProto::OK:
            action(response);
            break;
        case NKikimrProto::NODATA:
            break;
        case NKikimrProto::ERROR:
            LOG_ERROR_S(
                    ctx, NKikimrServices::PERSQUEUE,
                    "read topic '" << TopicConverter->GetClientsideName() << "' partition " << Partition << " error"
            );
            ctx.Send(Tablet, new TEvents::TEvPoisonPill());
            break;
        default:
            Cerr << "ERROR " << response.GetStatus() << "\n";
            Y_FAIL("bad status");
        };
    };

    auto loadMeta = [&](const NKikimrClient::TKeyValueResponse::TReadResult& response) {
        NKikimrPQ::TPartitionMeta meta;
        bool res = meta.ParseFromString(response.GetValue());
        Y_VERIFY(res);
        /* Bring back later, when switch to 21-2 will be unable
           StartOffset = meta.GetStartOffset();
           EndOffset = meta.GetEndOffset();
           if (StartOffset == EndOffset) {
           NewHead.Offset = Head.Offset = EndOffset;
           }
           */
        if (CurrentStateFunc() == &TThis::StateInit) {
            SubDomainOutOfSpace = meta.GetSubDomainOutOfSpace();
        }
    };
    handleReadResult(response.GetReadResult(0), loadMeta);

    auto loadTxMeta = [this](const NKikimrClient::TKeyValueResponse::TReadResult& response) {
        NKikimrPQ::TPartitionTxMeta meta;
        bool res = meta.ParseFromString(response.GetValue());
        Y_VERIFY(res);

        if (meta.HasPlanStep()) {
            PlanStep = meta.GetPlanStep();
        }
        if (meta.HasTxId()) {
            TxId = meta.GetTxId();
        }
    };
    handleReadResult(response.GetReadResult(1), loadTxMeta);

    InitState = WaitInfoRange;
    RequestInfoRange(ctx, Tablet, Partition, "");
}

void TPartition::HandleInfoRangeRead(const NKikimrClient::TKeyValueResponse::TReadRangeResult& range, const TActorContext& ctx) {
    //megaqc check here all results
    Y_VERIFY(range.HasStatus());
    const TString *key = nullptr;
    switch (range.GetStatus()) {
        case NKikimrProto::OK:
        case NKikimrProto::OVERRUN:
            for (ui32 i = 0; i < range.PairSize(); ++i) {
                const auto& pair = range.GetPair(i);
                Y_VERIFY(pair.HasStatus());
                if (pair.GetStatus() != NKikimrProto::OK) {
                    LOG_ERROR_S(
                            ctx, NKikimrServices::PERSQUEUE,
                            "read range error topic '" << TopicConverter->GetClientsideName() << "' partition " << Partition
                                << " got status " << pair.GetStatus() << " for key " << (pair.HasKey() ? pair.GetKey() : "unknown")
                    );

                    ctx.Send(Tablet, new TEvents::TEvPoisonPill());
                    return;
                }
                Y_VERIFY(pair.HasKey());
                Y_VERIFY(pair.HasValue());

                key = &pair.GetKey();
                if ((*key)[TKeyPrefix::MarkPosition()] == TKeyPrefix::MarkSourceId) {
                    SourceIdStorage.LoadSourceIdInfo(*key, pair.GetValue(), ctx.Now());
                } else if ((*key)[TKeyPrefix::MarkPosition()] == TKeyPrefix::MarkProtoSourceId) {
                    SourceIdStorage.LoadSourceIdInfo(*key, pair.GetValue(), ctx.Now());
                } else if ((*key)[TKeyPrefix::MarkPosition()] == TKeyPrefix::MarkUser) {
                    UsersInfoStorage->Parse(*key, pair.GetValue(), ctx);
                } else if ((*key)[TKeyPrefix::MarkPosition()] == TKeyPrefix::MarkUserDeprecated) {
                    UsersInfoStorage->ParseDeprecated(*key, pair.GetValue(), ctx);
                }
            }
            //make next step
            if (range.GetStatus() == NKikimrProto::OVERRUN) {
                Y_VERIFY(key);
                RequestInfoRange(ctx, Tablet, Partition, *key);
            } else {
                InitState = WaitDataRange;
                RequestDataRange(ctx, Tablet, Partition, "");
            }
            break;
        case NKikimrProto::NODATA:
            InitState = WaitDataRange;
            RequestDataRange(ctx, Tablet, Partition, "");
            break;
        case NKikimrProto::ERROR:
            LOG_ERROR_S(
                    ctx, NKikimrServices::PERSQUEUE,
                    "read topic '" << TopicConverter->GetClientsideName() << "' partition " << Partition << " error"
            );
            ctx.Send(Tablet, new TEvents::TEvPoisonPill());
            break;
        default:
            Cerr << "ERROR " << range.GetStatus() << "\n";
            Y_FAIL("bad status");
    };
}

void TPartition::FillBlobsMetaData(const NKikimrClient::TKeyValueResponse::TReadRangeResult& range, const TActorContext& ctx) {
    for (ui32 i = 0; i < range.PairSize(); ++i) {
        auto pair = range.GetPair(i);
        Y_VERIFY(pair.GetStatus() == NKikimrProto::OK); //this is readrange without keys, only OK could be here
        TKey k(pair.GetKey());
        if (DataKeysBody.empty()) { //no data - this is first pair of first range
            Head.Offset = EndOffset = StartOffset = k.GetOffset();
            if (k.GetPartNo() > 0) ++StartOffset;
            Head.PartNo = 0;
        } else {
            Y_VERIFY(EndOffset <= k.GetOffset(), "%s", pair.GetKey().c_str());
            if (EndOffset < k.GetOffset()) {
                GapOffsets.push_back(std::make_pair(EndOffset, k.GetOffset()));
                GapSize += k.GetOffset() - EndOffset;
            }
        }
        Y_VERIFY(k.GetCount() + k.GetInternalPartsCount() > 0);
        Y_VERIFY(k.GetOffset() >= EndOffset);
        EndOffset = k.GetOffset() + k.GetCount();
        //at this point EndOffset > StartOffset
        if (!k.IsHead()) //head.Size will be filled after read or head blobs
            BodySize += pair.GetValueSize();

        LOG_DEBUG_S(
                ctx, NKikimrServices::PERSQUEUE,
                "Got data topic " << TopicConverter->GetClientsideName() << " partition " << k.GetPartition()
                    << " offset " << k.GetOffset() << " count " << k.GetCount() << " size " << pair.GetValueSize()
                    << " so " << StartOffset << " eo " << EndOffset << " " << pair.GetKey()
        );
        DataKeysBody.push_back({k, pair.GetValueSize(), TInstant::Seconds(pair.GetCreationUnixTime()), DataKeysBody.empty() ? 0 : DataKeysBody.back().CumulativeSize + DataKeysBody.back().Size});
    }

    Y_VERIFY(EndOffset >= StartOffset);
}

void TPartition::FormHeadAndProceed(const TActorContext& ctx) {
    Head.Offset = EndOffset;
    Head.PartNo = 0;
    TVector<TString> keys;
    while (DataKeysBody.size() > 0 && DataKeysBody.back().Key.IsHead()) {
        Y_VERIFY(DataKeysBody.back().Key.GetOffset() + DataKeysBody.back().Key.GetCount() == Head.Offset); //no gaps in head allowed
        HeadKeys.push_front(DataKeysBody.back());
        Head.Offset = DataKeysBody.back().Key.GetOffset();
        Head.PartNo = DataKeysBody.back().Key.GetPartNo();
        DataKeysBody.pop_back();
    }
    for (const auto& p : DataKeysBody) {
        Y_VERIFY(!p.Key.IsHead());
    }

    Y_VERIFY(HeadKeys.empty() || Head.Offset == HeadKeys.front().Key.GetOffset() && Head.PartNo == HeadKeys.front().Key.GetPartNo());
    Y_VERIFY(Head.Offset < EndOffset || Head.Offset == EndOffset && HeadKeys.empty());
    Y_VERIFY(Head.Offset >= StartOffset || Head.Offset == StartOffset - 1 && Head.PartNo > 0);

    //form head request
    for (auto& p : HeadKeys) {
        keys.push_back({p.Key.Data(), p.Key.Size()});
    }
    Y_VERIFY(keys.size() < TotalMaxCount);
    if (keys.empty()) {
        InitComplete(ctx);
        return;
    }
    InitState = WaitDataRead;
    RequestData(ctx, Tablet, keys);
}

void TPartition::HandleDataRangeRead(const NKikimrClient::TKeyValueResponse::TReadRangeResult& range, const TActorContext& ctx) {
    Y_VERIFY(range.HasStatus());
    switch(range.GetStatus()) {
        case NKikimrProto::OK:
        case NKikimrProto::OVERRUN:

            FillBlobsMetaData(range, ctx);

            if (range.GetStatus() == NKikimrProto::OVERRUN) { //request rest of range
                Y_VERIFY(range.PairSize());
                RequestDataRange(ctx, Tablet, Partition, range.GetPair(range.PairSize() - 1).GetKey());
                return;
            }
            FormHeadAndProceed(ctx);
            break;
        case NKikimrProto::NODATA:
            InitComplete(ctx);
            break;
        default:
            Cerr << "ERROR " << range.GetStatus() << "\n";
            Y_FAIL("bad status");
    };
}

void TPartition::HandleDataRead(const NKikimrClient::TResponse& response, const TActorContext& ctx) {
    Y_VERIFY(InitState == WaitDataRead);
    ui32 currentLevel = 0;
    Y_VERIFY(HeadKeys.size() == response.ReadResultSize());
    for (ui32 i = 0; i < response.ReadResultSize(); ++i) {
        auto& read = response.GetReadResult(i);
        Y_VERIFY(read.HasStatus());
        switch(read.GetStatus()) {
            case NKikimrProto::OK: {
                const TKey& key = HeadKeys[i].Key;
                ui32 size = HeadKeys[i].Size;
                Y_VERIFY(key.IsHead());
                ui64 offset = key.GetOffset();
                while (currentLevel + 1 < TotalLevels && size < CompactLevelBorder[currentLevel + 1])
                    ++currentLevel;
                Y_VERIFY(size < CompactLevelBorder[currentLevel]);

                DataKeysHead[currentLevel].AddKey(key, size);
                Y_VERIFY(DataKeysHead[currentLevel].KeysCount() < AppData(ctx)->PQConfig.GetMaxBlobsPerLevel());
                Y_VERIFY(!DataKeysHead[currentLevel].NeedCompaction());

                LOG_DEBUG_S(
                        ctx, NKikimrServices::PERSQUEUE,
                        "read res partition topic '" << TopicConverter->GetClientsideName()
                            << "' parititon " << key.GetPartition() << " offset " << offset << " endOffset " << EndOffset
                            << " key " << key.GetOffset() << "," << key.GetCount() << " valuesize " << read.GetValue().size()
                            << " expected " << size
                );

                Y_VERIFY(offset + 1 >= StartOffset);
                Y_VERIFY(offset < EndOffset);
                Y_VERIFY(size == read.GetValue().size());

                for (TBlobIterator it(key, read.GetValue()); it.IsValid(); it.Next()) {
                    Head.Batches.push_back(it.GetBatch());
                }
                Head.PackedSize += size;

                break;
                }
            case NKikimrProto::OVERRUN:
                Y_FAIL("implement overrun in readresult!!");
                return;
            case NKikimrProto::NODATA:
                Y_FAIL("NODATA can't be here");
                return;
            case NKikimrProto::ERROR:
                LOG_ERROR_S(
                        ctx, NKikimrServices::PERSQUEUE,
                        "tablet " << TabletID << " HandleOnInit topic '" << TopicConverter->GetClientsideName()
                            << "' partition " << Partition
                            << " ReadResult " << i << " status NKikimrProto::ERROR result message: \"" << read.GetMessage()
                            << " \" errorReason: \"" << response.GetErrorReason() << "\""
                );
                ctx.Send(Tablet, new TEvents::TEvPoisonPill());
                return;
            default:
                Cerr << "ERROR " << read.GetStatus() << " message: \"" << read.GetMessage() << "\"\n";
                Y_FAIL("bad status");

        };
    }

    Y_VERIFY(Head.PackedSize > 0);
    Y_VERIFY(Head.PackedSize < MaxBlobSize);
    Y_VERIFY(Head.GetNextOffset() == EndOffset);
    Y_VERIFY(std::accumulate(DataKeysHead.begin(), DataKeysHead.end(), 0u,
                             [](ui32 sum, const TKeyLevel& level){return sum + level.Sum();}) == Head.PackedSize);

    InitComplete(ctx);
}


void TPartition::HandleOnInit(TEvKeyValue::TEvResponse::TPtr& ev, const TActorContext& ctx) {

    auto& response = ev->Get()->Record;
    if (response.GetStatus() != NMsgBusProxy::MSTATUS_OK) {
        LOG_ERROR_S(
                ctx, NKikimrServices::PERSQUEUE,
                "commands for topic '" << TopicConverter->GetClientsideName() << " partition " << Partition
                << " are not processed at all, got KV error " << response.GetStatus()
        );
        ctx.Send(Tablet, new TEvents::TEvPoisonPill());
        return;
    }
    bool diskIsOk = true;
    for (ui32 i = 0; i < response.GetStatusResultSize(); ++i) {
        auto& res = response.GetGetStatusResult(i);
        if (res.GetStatus() != NKikimrProto::OK) {
            LOG_ERROR_S(
                    ctx, NKikimrServices::PERSQUEUE,
                    "commands for topic '" << TopicConverter->GetClientsideName() << "' partition " << Partition <<
                    " are not processed at all, got KV error in CmdGetStatus " << res.GetStatus()
            );
            ctx.Send(Tablet, new TEvents::TEvPoisonPill());
            return;
        }
        diskIsOk = diskIsOk && CheckDiskStatus(res.GetStatusFlags());
    }
    if (response.GetStatusResultSize())
        DiskIsFull = !diskIsOk;

    switch(InitState) {
        case WaitConfig:
            Y_VERIFY(response.ReadResultSize() == 1);
            HandleConfig(response, ctx);
            break;
        case WaitDiskStatus:
            Y_VERIFY(response.GetStatusResultSize());
            HandleGetDiskStatus(response, ctx);
            break;
        case WaitMetaRead:
            Y_VERIFY(response.ReadResultSize() == 2);
            HandleMetaRead(response, ctx);
            break;
        case WaitInfoRange:
            Y_VERIFY(response.ReadRangeResultSize() == 1);
            HandleInfoRangeRead(response.GetReadRangeResult(0), ctx);
            break;
        case WaitDataRange:
            Y_VERIFY(response.ReadRangeResultSize() == 1);
            HandleDataRangeRead(response.GetReadRangeResult(0), ctx);
            break;
        case WaitDataRead:
            Y_VERIFY(response.ReadResultSize());
            HandleDataRead(response, ctx);
            break;
        default:
            Y_FAIL("Unknown state");

    };
}

void TPartition::InitComplete(const TActorContext& ctx) {
    if (StartOffset == EndOffset && EndOffset == 0) {
        for (auto& [user, info] : UsersInfoStorage->GetAll()) {
            if (info.Offset > 0 && StartOffset < (ui64)info.Offset) {
                 Head.Offset = EndOffset = StartOffset = info.Offset;
            }
        }
    }

    LOG_INFO_S(
            ctx, NKikimrServices::PERSQUEUE,
            "init complete for topic '" << TopicConverter->GetClientsideName() << "' partition " << Partition << " " << ctx.SelfID
    );

    TStringBuilder ss;
    ss << "SYNC INIT topic " << TopicConverter->GetClientsideName() << " partitition " << Partition
       << " so " << StartOffset << " endOffset " << EndOffset << " Head " << Head << "\n";
    for (const auto& s : SourceIdStorage.GetInMemorySourceIds()) {
        ss << "SYNC INIT sourceId " << s.first << " seqNo " << s.second.SeqNo << " offset " << s.second.Offset << "\n";
    }
    for (const auto& h : DataKeysBody) {
        ss << "SYNC INIT DATA KEY: " << TString(h.Key.Data(), h.Key.Size()) << " size " << h.Size << "\n";
    }
    for (const auto& h : HeadKeys) {
        ss << "SYNC INIT HEAD KEY: " << TString(h.Key.Data(), h.Key.Size()) << " size " << h.Size << "\n";
    }
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, ss);

    CheckHeadConsistency();

    Become(&TThis::StateIdle);
    InitDuration = ctx.Now() - CreationTime;
    InitDone = true;
    TabletCounters.Percentile()[COUNTER_LATENCY_PQ_INIT].IncrementFor(InitDuration.MilliSeconds());

    FillReadFromTimestamps(Config, ctx);
    ResendPendingEvents(ctx);
    ProcessTxsAndUserActs(ctx);

    ctx.Send(ctx.SelfID, new TEvents::TEvWakeup());
    if (!NewPartition) {
        ctx.Send(Tablet, new TEvPQ::TEvInitComplete(Partition));
    }
    for (const auto& s : SourceIdStorage.GetInMemorySourceIds()) {
        LOG_DEBUG_S(
                ctx, NKikimrServices::PERSQUEUE,
                "Init complete for topic '" << TopicConverter->GetClientsideName() << "' Partition: " << Partition
                    << " SourceId: " << s.first << " SeqNo: " << s.second.SeqNo << " offset: " << s.second.Offset
                    << " MaxOffset: " << EndOffset
        );
    }
    ProcessHasDataRequests(ctx);

    InitUserInfoForImportantClients(ctx);

    for (auto& userInfoPair : UsersInfoStorage->GetAll()) {
        Y_VERIFY(userInfoPair.second.Offset >= 0);
        ReadTimestampForOffset(userInfoPair.first, userInfoPair.second, ctx);
    }
    if (PartitionCountersLabeled) {
        PartitionCountersLabeled->GetCounters()[METRIC_INIT_TIME] = InitDuration.MilliSeconds();
        PartitionCountersLabeled->GetCounters()[METRIC_LIFE_TIME] = CreationTime.MilliSeconds();
        PartitionCountersLabeled->GetCounters()[METRIC_PARTITIONS] = 1;
        PartitionCountersLabeled->GetCounters()[METRIC_PARTITIONS_TOTAL] = Config.PartitionIdsSize();
        ctx.Send(Tablet, new TEvPQ::TEvPartitionLabeledCounters(Partition, *PartitionCountersLabeled));
    }
    UpdateUserInfoEndOffset(ctx.Now());

    ScheduleUpdateAvailableSize(ctx);

    if (Config.GetPartitionConfig().HasMirrorFrom()) {
        CreateMirrorerActor();
    }

    ReportCounters(ctx);
}


void TPartition::UpdateUserInfoEndOffset(const TInstant& now) {
    for (auto& userInfo : UsersInfoStorage->GetAll()) {
        userInfo.second.EndOffset = (i64)EndOffset;
        userInfo.second.UpdateReadingTimeAndState(now);
    }

}

void TPartition::ProcessChangeOwnerRequest(TAutoPtr<TEvPQ::TEvChangeOwner> ev, const TActorContext& ctx) {

    auto &owner = ev->Owner;
    auto it = Owners.find(owner);
    if (it == Owners.end()) {
        Owners[owner];
        it = Owners.find(owner);
    }
    if (it->second.NeedResetOwner || ev->Force) { //change owner
        Y_VERIFY(ReservedSize >= it->second.ReservedSize);
        ReservedSize -= it->second.ReservedSize;

        it->second.GenerateCookie(owner, ev->PipeClient, ev->Sender, TopicConverter->GetClientsideName(), Partition, ctx);//will change OwnerCookie
        //cookie is generated. but answer will be sent when all inflight writes will be done - they in the same queue 'Requests'
        EmplaceRequest(TOwnershipMsg{ev->Cookie, it->second.OwnerCookie}, ctx);
        TabletCounters.Simple()[COUNTER_PQ_TABLET_RESERVED_BYTES_SIZE].Set(ReservedSize);
        UpdateWriteBufferIsFullState(ctx.Now());
        ProcessReserveRequests(ctx);
    } else {
        it->second.WaitToChangeOwner.push_back(THolder<TEvPQ::TEvChangeOwner>(ev.Release()));
    }
}


THashMap<TString, NKikimr::NPQ::TOwnerInfo>::iterator TPartition::DropOwner(THashMap<TString, NKikimr::NPQ::TOwnerInfo>::iterator& it, const TActorContext& ctx) {
    Y_VERIFY(ReservedSize >= it->second.ReservedSize);
    ReservedSize -= it->second.ReservedSize;
    UpdateWriteBufferIsFullState(ctx.Now());
    TabletCounters.Simple()[COUNTER_PQ_TABLET_RESERVED_BYTES_SIZE].Set(ReservedSize);
    for (auto& ev : it->second.WaitToChangeOwner) { //this request maybe could be done right now
        WaitToChangeOwner.push_back(THolder<TEvPQ::TEvChangeOwner>(ev.Release()));
    }
    auto jt = it;
    ++jt;
    Owners.erase(it);
    return jt;
}

void TPartition::InitUserInfoForImportantClients(const TActorContext& ctx) {
    TSet<TString> important;
    for (const auto& importantUser : Config.GetPartitionConfig().GetImportantClientId()) {
        important.insert(importantUser);
        TUserInfo* userInfo = UsersInfoStorage->GetIfExists(importantUser);
        if (userInfo && !userInfo->Important && userInfo->LabeledCounters) {
            ctx.Send(Tablet, new TEvPQ::TEvPartitionLabeledCountersDrop(Partition, userInfo->LabeledCounters->GetGroup()));
            userInfo->SetImportant(true);
            continue;
        }
        if (!userInfo) {
            userInfo = &UsersInfoStorage->Create(ctx, importantUser, 0, true, "", 0, 0, 0, 0, TInstant::Zero());
        }
        if (userInfo->Offset < (i64)StartOffset)
            userInfo->Offset = StartOffset;
        ReadTimestampForOffset(importantUser, *userInfo, ctx);
    }
    for (auto& userInfoPair : UsersInfoStorage->GetAll()) {
        if (!important.contains(userInfoPair.first) && userInfoPair.second.Important && userInfoPair.second.LabeledCounters) {
            ctx.Send(
                Tablet,
                new TEvPQ::TEvPartitionLabeledCountersDrop(Partition, userInfoPair.second.LabeledCounters->GetGroup())
            );
            userInfoPair.second.SetImportant(false);
        }
    }
}

void TPartition::Handle(TEvPQ::TEvChangePartitionConfig::TPtr& ev, const TActorContext& ctx) {
    PushBackDistrTx(ev->Release());

    ProcessTxsAndUserActs(ctx);
}


void TPartition::Handle(TEvPQ::TEvChangeOwner::TPtr& ev, const TActorContext& ctx) {
    bool res = OwnerPipes.insert(ev->Get()->PipeClient).second;
    Y_VERIFY(res);
    WaitToChangeOwner.push_back(ev->Release());
    ProcessChangeOwnerRequests(ctx);
}


void TPartition::Handle(TEvPQ::TEvPipeDisconnected::TPtr& ev, const TActorContext& ctx) {

    const TString& owner = ev->Get()->Owner;
    const TActorId& pipeClient = ev->Get()->PipeClient;

    OwnerPipes.erase(pipeClient);

    auto it = Owners.find(owner);
    if (it == Owners.end() || it->second.PipeClient != pipeClient) // owner session is already dead
        return;
    //TODO: Uncommet when writes will be done via new gRPC protocol
    // msgbus do not reserve bytes right now!!
    // DropOwner will drop reserved bytes and ownership
    if (owner != "default") { //default owner is for old LB protocol, pipe is dead right now after GetOwnership request, and no ReserveBytes done. So, ignore pipe disconnection
        DropOwner(it, ctx);
        ProcessChangeOwnerRequests(ctx);
    }
}


void TPartition::ProcessReserveRequests(const TActorContext& ctx) {
    const ui64 maxWriteInflightSize = Config.GetPartitionConfig().GetMaxWriteInflightSize();

    while (!ReserveRequests.empty()) {
        const TString& ownerCookie = ReserveRequests.front()->OwnerCookie;
        const TStringBuf owner = TOwnerInfo::GetOwnerFromOwnerCookie(ownerCookie);
        const ui64& size = ReserveRequests.front()->Size;
        const ui64& cookie = ReserveRequests.front()->Cookie;
        const bool& lastRequest = ReserveRequests.front()->LastRequest;

        auto it = Owners.find(owner);
        if (it == Owners.end() || it->second.OwnerCookie != ownerCookie) {
            ReplyError(ctx, cookie, NPersQueue::NErrorCode::BAD_REQUEST, "ReserveRequest from dead ownership session");
            ReserveRequests.pop_front();
            continue;
        }

        const ui64 currentSize = ReservedSize + WriteInflightSize + WriteCycleSize;
        if (currentSize != 0 && currentSize + size > maxWriteInflightSize) {
            LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "Reserve processing: maxWriteInflightSize riched");            
            break;
        }

        if (WaitingForSubDomainQuota(ctx, currentSize)) {
            LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "Reserve processing: SubDomainOutOfSpace");            
            break;
        }

        it->second.AddReserveRequest(size, lastRequest);
        ReservedSize += size;

        ReplyOk(ctx, cookie);

        ReserveRequests.pop_front();
    }
    UpdateWriteBufferIsFullState(ctx.Now());
    TabletCounters.Simple()[COUNTER_PQ_TABLET_RESERVED_BYTES_SIZE].Set(ReservedSize);
}

void TPartition::UpdateWriteBufferIsFullState(const TInstant& now) {
    WriteBufferIsFullCounter.UpdateWorkingTime(now);
    WriteBufferIsFullCounter.UpdateState(ReservedSize + WriteInflightSize + WriteCycleSize >= Config.GetPartitionConfig().GetBorderWriteInflightSize());
}



void TPartition::Handle(TEvPQ::TEvReserveBytes::TPtr& ev, const TActorContext& ctx) {
    const TString& ownerCookie = ev->Get()->OwnerCookie;
    TStringBuf owner = TOwnerInfo::GetOwnerFromOwnerCookie(ownerCookie);
    const ui64& messageNo = ev->Get()->MessageNo;

    auto it = Owners.find(owner);
    if (it == Owners.end() || it->second.OwnerCookie != ownerCookie) {
        ReplyError(ctx, ev->Get()->Cookie, NPersQueue::NErrorCode::BAD_REQUEST, "ReserveRequest from dead ownership session");
        return;
    }

    if (messageNo != it->second.NextMessageNo) {
        ReplyError(ctx, ev->Get()->Cookie, NPersQueue::NErrorCode::BAD_REQUEST,
            TStringBuilder() << "reorder in reserve requests, waiting " << it->second.NextMessageNo << ", but got " << messageNo);
        DropOwner(it, ctx);
        ProcessChangeOwnerRequests(ctx);
        return;
    }

    ++it->second.NextMessageNo;
    ReserveRequests.push_back(ev->Release());
    ProcessReserveRequests(ctx);
}




void TPartition::Handle(TEvPQ::TEvPartitionOffsets::TPtr& ev, const TActorContext& ctx) {
    NKikimrPQ::TOffsetsResponse::TPartResult result;
    result.SetPartition(Partition);
    result.SetStartOffset(StartOffset);
    result.SetEndOffset(EndOffset);
    result.SetErrorCode(NPersQueue::NErrorCode::OK);
    result.SetWriteTimestampEstimateMS(WriteTimestampEstimate.MilliSeconds());

    if (!ev->Get()->ClientId.empty()) {
        TUserInfo* userInfo = UsersInfoStorage->GetIfExists(ev->Get()->ClientId);
        if (userInfo) {
            i64 offset = Max<i64>(userInfo->Offset, 0);
            result.SetClientOffset(userInfo->Offset);
            TInstant tmp = userInfo->GetWriteTimestamp() ? userInfo->GetWriteTimestamp() : GetWriteTimeEstimate(offset);
            result.SetWriteTimestampMS(tmp.MilliSeconds());
            result.SetCreateTimestampMS(userInfo->GetCreateTimestamp().MilliSeconds());
            result.SetClientReadOffset(userInfo->GetReadOffset());
            tmp = userInfo->GetReadWriteTimestamp() ? userInfo->GetReadWriteTimestamp() : GetWriteTimeEstimate(userInfo->GetReadOffset());
            result.SetReadWriteTimestampMS(tmp.MilliSeconds());
            result.SetReadCreateTimestampMS(userInfo->GetReadCreateTimestamp().MilliSeconds());
        }
    }
    ctx.Send(ev->Get()->Sender, new TEvPQ::TEvPartitionOffsetsResponse(result));
}

void TPartition::HandleOnInit(TEvPQ::TEvPartitionOffsets::TPtr& ev, const TActorContext& ctx) {
    NKikimrPQ::TOffsetsResponse::TPartResult result;
    result.SetPartition(Partition);
    result.SetErrorCode(NPersQueue::NErrorCode::INITIALIZING);
    result.SetErrorReason("partition is not ready yet");
    ctx.Send(ev->Get()->Sender, new TEvPQ::TEvPartitionOffsetsResponse(result));
}

void TPartition::Handle(TEvPQ::TEvPartitionStatus::TPtr& ev, const TActorContext& ctx) {
    NKikimrPQ::TStatusResponse::TPartResult result;
    result.SetPartition(Partition);
    if (DiskIsFull || WaitingForSubDomainQuota(ctx)) {
        result.SetStatus(NKikimrPQ::TStatusResponse::STATUS_DISK_IS_FULL);
    } else if (EndOffset - StartOffset >= static_cast<ui64>(Config.GetPartitionConfig().GetMaxCountInPartition()) ||
               Size() >= static_cast<ui64>(Config.GetPartitionConfig().GetMaxSizeInPartition())) {
        result.SetStatus(NKikimrPQ::TStatusResponse::STATUS_PARTITION_IS_FULL);
    } else {
        result.SetStatus(NKikimrPQ::TStatusResponse::STATUS_OK);
    }
    result.SetLastInitDurationSeconds(InitDuration.Seconds());
    result.SetCreationTimestamp(CreationTime.Seconds());
    ui64 headGapSize = DataKeysBody.empty() ? 0 : (Head.Offset - (DataKeysBody.back().Key.GetOffset() + DataKeysBody.back().Key.GetCount()));
    ui32 gapsCount = GapOffsets.size() + (headGapSize ? 1 : 0);
    result.SetGapCount(gapsCount);
    result.SetGapSize(headGapSize + GapSize);

    Y_VERIFY(AvgWriteBytes.size() == 4);
    result.SetAvgWriteSpeedPerSec(AvgWriteBytes[0].GetValue());
    result.SetAvgWriteSpeedPerMin(AvgWriteBytes[1].GetValue());
    result.SetAvgWriteSpeedPerHour(AvgWriteBytes[2].GetValue());
    result.SetAvgWriteSpeedPerDay(AvgWriteBytes[3].GetValue());

    Y_VERIFY(AvgQuotaBytes.size() == 4);
    result.SetAvgQuotaSpeedPerSec(AvgQuotaBytes[0].GetValue());
    result.SetAvgQuotaSpeedPerMin(AvgQuotaBytes[1].GetValue());
    result.SetAvgQuotaSpeedPerHour(AvgQuotaBytes[2].GetValue());
    result.SetAvgQuotaSpeedPerDay(AvgQuotaBytes[3].GetValue());

    result.SetSourceIdCount(SourceIdStorage.GetInMemorySourceIds().size());
    result.SetSourceIdRetentionPeriodSec((ctx.Now() - SourceIdStorage.MinAvailableTimestamp(ctx.Now())).Seconds());

    result.SetWriteBytesQuota(WriteQuota->GetTotalSpeed());

    TVector<ui64> resSpeed;
    resSpeed.resize(4);
    ui64 maxQuota = 0;
    for (auto& userInfoPair : UsersInfoStorage->GetAll()) {
        auto& userInfo = userInfoPair.second;
        if (ev->Get()->ClientId.empty() || ev->Get()->ClientId == userInfo.User) {
            Y_VERIFY(userInfo.AvgReadBytes.size() == 4);
            for (ui32 i = 0; i < 4; ++i) {
                resSpeed[i] += userInfo.AvgReadBytes[i].GetValue();
            }
            maxQuota += userInfo.ReadQuota.GetTotalSpeed();
        }
        if (ev->Get()->ClientId == userInfo.User) { //fill lags
            NKikimrPQ::TClientInfo* clientInfo = result.MutableLagsInfo();
            clientInfo->SetClientId(userInfo.User);
            auto write = clientInfo->MutableWritePosition();
            write->SetOffset(userInfo.Offset);
            userInfo.EndOffset = EndOffset;
            write->SetWriteTimestamp((userInfo.GetWriteTimestamp() ? userInfo.GetWriteTimestamp() : GetWriteTimeEstimate(userInfo.Offset)).MilliSeconds());
            write->SetCreateTimestamp(userInfo.GetCreateTimestamp().MilliSeconds());
            auto read = clientInfo->MutableReadPosition();
            read->SetOffset(userInfo.GetReadOffset());
            read->SetWriteTimestamp((userInfo.GetReadWriteTimestamp() ? userInfo.GetReadWriteTimestamp() : GetWriteTimeEstimate(userInfo.GetReadOffset())).MilliSeconds());
            read->SetCreateTimestamp(userInfo.GetReadCreateTimestamp().MilliSeconds());
            write->SetSize(GetSizeLag(userInfo.Offset));
            read->SetSize(GetSizeLag(userInfo.GetReadOffset()));

            clientInfo->SetReadLagMs(userInfo.GetReadOffset() < (i64)EndOffset
                                        ? (userInfo.GetReadTimestamp() - TInstant::MilliSeconds(read->GetWriteTimestamp())).MilliSeconds()
                                        : 0);
            clientInfo->SetLastReadTimestampMs(userInfo.GetReadTimestamp().MilliSeconds());
            clientInfo->SetWriteLagMs(userInfo.GetWriteLagMs());
            ui64 totalLag = clientInfo->GetReadLagMs() + userInfo.GetWriteLagMs() + (ctx.Now() - userInfo.GetReadTimestamp()).MilliSeconds();
            clientInfo->SetTotalLagMs(totalLag);
        }

        if (ev->Get()->GetStatForAllConsumers) { //fill lags
            auto* clientInfo = result.AddConsumerResult();
            clientInfo->SetConsumer(userInfo.User);
            auto readTimestamp = (userInfo.GetReadWriteTimestamp() ? userInfo.GetReadWriteTimestamp() : GetWriteTimeEstimate(userInfo.GetReadOffset())).MilliSeconds();
            clientInfo->SetReadLagMs(userInfo.GetReadOffset() < (i64)EndOffset
                                        ? (userInfo.GetReadTimestamp() - TInstant::MilliSeconds(readTimestamp)).MilliSeconds()
                                        : 0);
            clientInfo->SetLastReadTimestampMs(userInfo.GetReadTimestamp().MilliSeconds());
            clientInfo->SetWriteLagMs(userInfo.GetWriteLagMs());

            clientInfo->SetAvgReadSpeedPerMin(userInfo.AvgReadBytes[1].GetValue());
            clientInfo->SetAvgReadSpeedPerHour(userInfo.AvgReadBytes[2].GetValue());
            clientInfo->SetAvgReadSpeedPerDay(userInfo.AvgReadBytes[3].GetValue());
        }

    }
    result.SetAvgReadSpeedPerSec(resSpeed[0]);
    result.SetAvgReadSpeedPerMin(resSpeed[1]);
    result.SetAvgReadSpeedPerHour(resSpeed[2]);
    result.SetAvgReadSpeedPerDay(resSpeed[3]);

    result.SetReadBytesQuota(maxQuota);

    result.SetPartitionSize(MeteringDataSize(ctx));
    result.SetUsedReserveSize(UsedReserveSize(ctx));
    result.SetStartOffset(StartOffset);
    result.SetEndOffset(EndOffset);

    result.SetLastWriteTimestampMs(WriteTimestamp.MilliSeconds());
    result.SetWriteLagMs(WriteLagMs.GetValue());

    *result.MutableErrors() = {Errors.begin(), Errors.end()};

    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE,
                "Topic PartitionStatus PartitionSize: " << result.GetPartitionSize()
                << " UsedReserveSize: " << result.GetUsedReserveSize()
                << " ReserveSize: " << ReserveSize()
                << " PartitionConfig" << Config.GetPartitionConfig();
    );

    UpdateCounters(ctx);
    if (PartitionCountersLabeled) {
        auto* ac = result.MutableAggregatedCounters();
        for (ui32 i = 0; i < PartitionCountersLabeled->GetCounters().Size(); ++i) {
            ac->AddValues(PartitionCountersLabeled->GetCounters()[i].Get());
        }
        for (auto& userInfoPair : UsersInfoStorage->GetAll()) {
            auto& userInfo = userInfoPair.second;
            if (!userInfo.LabeledCounters)
                continue;
            if (!userInfo.HasReadRule && !userInfo.Important)
                continue;
            auto* cac = ac->AddConsumerAggregatedCounters();
            cac->SetConsumer(userInfo.User);
            for (ui32 i = 0; i < userInfo.LabeledCounters->GetCounters().Size(); ++i) {
                cac->AddValues(userInfo.LabeledCounters->GetCounters()[i].Get());
            }
        }
    }
    ctx.Send(ev->Get()->Sender, new TEvPQ::TEvPartitionStatusResponse(result));
}

void TPartition::HandleOnInit(TEvPQ::TEvPartitionStatus::TPtr& ev, const TActorContext& ctx) {
    NKikimrPQ::TStatusResponse::TPartResult result;
    result.SetPartition(Partition);
    result.SetStatus(NKikimrPQ::TStatusResponse::STATUS_INITIALIZING);
    result.SetLastInitDurationSeconds((ctx.Now() - CreationTime).Seconds());
    result.SetCreationTimestamp(CreationTime.Seconds());
    ctx.Send(ev->Get()->Sender, new TEvPQ::TEvPartitionStatusResponse(result));
}


void TPartition::Handle(TEvPQ::TEvGetPartitionClientInfo::TPtr& ev, const TActorContext& ctx) {
    THolder<TEvPersQueue::TEvPartitionClientInfoResponse> response = MakeHolder<TEvPersQueue::TEvPartitionClientInfoResponse>();
    NKikimrPQ::TClientInfoResponse& result(response->Record);
    result.SetPartition(Partition);
    result.SetStartOffset(StartOffset);
    result.SetEndOffset(EndOffset);
    result.SetResponseTimestamp(ctx.Now().MilliSeconds());
    for (auto& pr : UsersInfoStorage->GetAll()) {
        TUserInfo& userInfo(pr.second);
        NKikimrPQ::TClientInfo& clientInfo = *result.AddClientInfo();
        clientInfo.SetClientId(pr.first);
        auto& write = *clientInfo.MutableWritePosition();
        write.SetOffset(userInfo.Offset);
        userInfo.EndOffset = EndOffset;
        write.SetWriteTimestamp((userInfo.GetWriteTimestamp() ? userInfo.GetWriteTimestamp() : GetWriteTimeEstimate(userInfo.Offset)).MilliSeconds());
        write.SetCreateTimestamp(userInfo.GetCreateTimestamp().MilliSeconds());
        auto& read = *clientInfo.MutableReadPosition();
        read.SetOffset(userInfo.GetReadOffset());
        read.SetWriteTimestamp((userInfo.GetReadWriteTimestamp() ? userInfo.GetReadWriteTimestamp() : GetWriteTimeEstimate(userInfo.GetReadOffset())).MilliSeconds());
        read.SetCreateTimestamp(userInfo.GetReadCreateTimestamp().MilliSeconds());
        write.SetSize(GetSizeLag(userInfo.Offset));
        read.SetSize(GetSizeLag(userInfo.GetReadOffset()));
    }
    ctx.Send(ev->Get()->Sender, response.Release(), 0, ev->Cookie);
}

void TPartition::Handle(TEvPersQueue::TEvReportPartitionError::TPtr& ev, const TActorContext& ctx) {
    LogAndCollectError(ev->Get()->Record, ctx);
}

void TPartition::LogAndCollectError(const NKikimrPQ::TStatusResponse::TErrorMessage& error, const TActorContext& ctx) {
    if (Errors.size() == MAX_ERRORS_COUNT_TO_STORE) {
        Errors.pop_front();
    }
    Errors.push_back(error);
    LOG_ERROR_S(ctx, error.GetService(), error.GetMessage());
}

void TPartition::LogAndCollectError(NKikimrServices::EServiceKikimr service, const TString& msg, const TActorContext& ctx) {
    NKikimrPQ::TStatusResponse::TErrorMessage error;
    error.SetTimestamp(ctx.Now().Seconds());
    error.SetService(service);
    error.SetMessage(TStringBuilder() << "topic '" << TopicConverter->GetClientsideName() << "' partition " << Partition << " got error: " << msg);
    LogAndCollectError(error, ctx);
}

std::pair<TInstant, TInstant> TPartition::GetTime(const TUserInfo& userInfo, ui64 offset) const {
    TInstant wtime = userInfo.WriteTimestamp > TInstant::Zero() ? userInfo.WriteTimestamp : GetWriteTimeEstimate(offset);
    return std::make_pair(wtime, userInfo.CreateTimestamp);
}

//zero means no such record
TInstant TPartition::GetWriteTimeEstimate(ui64 offset) const {
    if (offset < StartOffset) offset = StartOffset;
    if (offset >= EndOffset)
        return TInstant::Zero();
    const std::deque<TDataKey>& container =
        (offset < Head.Offset || offset == Head.Offset && Head.PartNo > 0) ? DataKeysBody : HeadKeys;
    Y_VERIFY(!container.empty());
    auto it = std::upper_bound(container.begin(), container.end(), offset,
                    [](const ui64 offset, const TDataKey& p) {
                        return offset < p.Key.GetOffset() ||
                                        offset == p.Key.GetOffset() && p.Key.GetPartNo() > 0;
                    });
    // Always greater
    Y_VERIFY(it != container.begin(),
             "Tablet %lu StartOffset %lu, HeadOffset %lu, offset %lu, containter size %lu, first-elem: %s",
             TabletID, StartOffset, Head.Offset, offset, container.size(),
             container.front().Key.ToString().c_str());
    Y_VERIFY(it == container.end() ||
             it->Key.GetOffset() > offset ||
             it->Key.GetOffset() == offset && it->Key.GetPartNo() > 0);
    --it;
    if (it != container.begin())
        --it;
    return it->Timestamp;
}


void TPartition::Handle(TEvPQ::TEvGetClientOffset::TPtr& ev, const TActorContext& ctx) {
    auto& userInfo = UsersInfoStorage->GetOrCreate(ev->Get()->ClientId, ctx);
    Y_VERIFY(userInfo.Offset >= -1, "Unexpected Offset: %" PRIi64, userInfo.Offset);
    ui64 offset = Max<i64>(userInfo.Offset, 0);
    auto ts = GetTime(userInfo, offset);
    TabletCounters.Cumulative()[COUNTER_PQ_GET_CLIENT_OFFSET_OK].Increment(1);
    ReplyGetClientOffsetOk(ctx, ev->Get()->Cookie, userInfo.Offset, ts.first, ts.second);
}

void TPartition::Handle(TEvPQ::TEvUpdateWriteTimestamp::TPtr& ev, const TActorContext& ctx) {
    TInstant timestamp = TInstant::MilliSeconds(ev->Get()->WriteTimestamp);
    if (WriteTimestampEstimate > timestamp) {
        ReplyError(ctx, ev->Get()->Cookie, NPersQueue::NErrorCode::BAD_REQUEST,
            TStringBuilder() << "too big timestamp: " << timestamp << " known " << WriteTimestampEstimate);
        return;
    }
    WriteTimestampEstimate = timestamp;
    ReplyOk(ctx, ev->Get()->Cookie);
}

void TPartition::Handle(TEvPersQueue::TEvProposeTransaction::TPtr& ev, const TActorContext& ctx)
{
    const NKikimrPQ::TEvProposeTransaction& event = ev->Get()->Record;
    Y_VERIFY(event.GetTxBodyCase() == NKikimrPQ::TEvProposeTransaction::kData);
    Y_VERIFY(event.HasData());
    const NKikimrPQ::TDataTransaction& txBody = event.GetData();

    if (!txBody.GetImmediate()) {
        ReplyPropose(ctx,
                     event,
                     NKikimrPQ::TEvProposeTransactionResult::ABORTED);
        return;
    }

    if (ImmediateTxs.size() > MAX_TXS) {
        ReplyPropose(ctx,
                     event,
                     NKikimrPQ::TEvProposeTransactionResult::OVERLOADED);
        return;
    }

    AddImmediateTx(ev->Release());

    ProcessTxsAndUserActs(ctx);
}

void TPartition::Handle(TEvPQ::TEvProposePartitionConfig::TPtr& ev, const TActorContext& ctx)
{
    PushBackDistrTx(ev->Release());

    ProcessTxsAndUserActs(ctx);
}

void TPartition::HandleOnInit(TEvPQ::TEvTxCalcPredicate::TPtr& ev, const TActorContext&)
{
    PendingEvents.emplace_back(ev->ReleaseBase().Release());
}

void TPartition::HandleOnInit(TEvPQ::TEvTxCommit::TPtr& ev, const TActorContext&)
{
    PendingEvents.emplace_back(ev->ReleaseBase().Release());
}

void TPartition::HandleOnInit(TEvPQ::TEvTxRollback::TPtr& ev, const TActorContext&)
{
    PendingEvents.emplace_back(ev->ReleaseBase().Release());
}

void TPartition::HandleOnInit(TEvPQ::TEvProposePartitionConfig::TPtr& ev, const TActorContext&)
{
    PendingEvents.emplace_back(ev->ReleaseBase().Release());
}

void TPartition::Handle(TEvPQ::TEvTxCalcPredicate::TPtr& ev, const TActorContext& ctx)
{
    PushBackDistrTx(ev->Release());

    ProcessTxsAndUserActs(ctx);
}

void TPartition::Handle(TEvPQ::TEvTxCommit::TPtr& ev, const TActorContext& ctx)
{
    EndTransaction(*ev->Get(), ctx);

    TxInProgress = false;

    ContinueProcessTxsAndUserActs(ctx);
}

void TPartition::Handle(TEvPQ::TEvTxRollback::TPtr& ev, const TActorContext& ctx)
{
    EndTransaction(*ev->Get(), ctx);

    TxInProgress = false;

    ContinueProcessTxsAndUserActs(ctx);
}

void TPartition::Handle(TEvPQ::TEvSetClientInfo::TPtr& ev, const TActorContext& ctx) {
    if (size_t count = GetUserActCount(ev->Get()->ClientId); count > MAX_USER_ACTS) {
        TabletCounters.Cumulative()[COUNTER_PQ_SET_CLIENT_OFFSET_ERROR].Increment(1);
        ReplyError(ctx, ev->Get()->Cookie, NPersQueue::NErrorCode::OVERLOAD,
            TStringBuilder() << "too big inflight: " << count);
        return;
    }

    const ui64& offset = ev->Get()->Offset;
    Y_VERIFY(offset <= (ui64)Max<i64>(), "Unexpected Offset: %" PRIu64, offset);

    AddUserAct(ev->Release());

    ProcessTxsAndUserActs(ctx);
}

void TPartition::Handle(TEvPQ::TEvGetMaxSeqNoRequest::TPtr& ev, const TActorContext& ctx) {
    auto response = MakeHolder<TEvPQ::TEvProxyResponse>(ev->Get()->Cookie);
    NKikimrClient::TResponse& resp = response->Response;

    resp.SetStatus(NMsgBusProxy::MSTATUS_OK);
    resp.SetErrorCode(NPersQueue::NErrorCode::OK);

    auto& result = *resp.MutablePartitionResponse()->MutableCmdGetMaxSeqNoResult();
    for (const auto& sourceId : ev->Get()->SourceIds) {
        auto& protoInfo = *result.AddSourceIdInfo();
        protoInfo.SetSourceId(sourceId);

        auto it = SourceIdStorage.GetInMemorySourceIds().find(sourceId);
        if (it == SourceIdStorage.GetInMemorySourceIds().end()) {
            continue;
        }

        const auto& memInfo = it->second;
        Y_VERIFY(memInfo.Offset <= (ui64)Max<i64>(), "Offset is too big: %" PRIu64, memInfo.Offset);
        Y_VERIFY(memInfo.SeqNo <= (ui64)Max<i64>(), "SeqNo is too big: %" PRIu64, memInfo.SeqNo);

        protoInfo.SetSeqNo(memInfo.SeqNo);
        protoInfo.SetOffset(memInfo.Offset);
        protoInfo.SetWriteTimestampMS(memInfo.WriteTimestamp.MilliSeconds());
        protoInfo.SetExplicit(memInfo.Explicit);
        protoInfo.SetState(TSourceIdInfo::ConvertState(memInfo.State));
    }

    ctx.Send(Tablet, response.Release());
}


void TPartition::Handle(TEvPQ::TEvBlobResponse::TPtr& ev, const TActorContext& ctx) {
    const ui64 cookie = ev->Get()->GetCookie();
    Y_VERIFY(ReadInfo.contains(cookie));

    auto it = ReadInfo.find(cookie);
    Y_VERIFY(it != ReadInfo.end());
    TReadInfo info = std::move(it->second);
    ReadInfo.erase(it);

    //make readinfo class
    TReadAnswer answer(info.FormAnswer(
        ctx, *ev->Get(), EndOffset, Partition, &UsersInfoStorage->GetOrCreate(info.User, ctx),
        info.Destination, GetSizeLag(info.Offset), Tablet, Config.GetMeteringMode()
    ));

    if (HasError(*ev->Get())) {
        if (info.IsSubscription) {
            TabletCounters.Cumulative()[COUNTER_PQ_READ_SUBSCRIPTION_ERROR].Increment(1);
        }
        TabletCounters.Cumulative()[COUNTER_PQ_READ_ERROR].Increment(1);
        TabletCounters.Percentile()[COUNTER_LATENCY_PQ_READ_ERROR].IncrementFor((ctx.Now() - info.Timestamp).MilliSeconds());
    } else {
        if (info.IsSubscription) {
            TabletCounters.Cumulative()[COUNTER_PQ_READ_SUBSCRIPTION_OK].Increment(1);
        }
        const auto& resp = dynamic_cast<TEvPQ::TEvProxyResponse*>(answer.Event.Get())->Response;
        TabletCounters.Cumulative()[COUNTER_PQ_READ_OK].Increment(1);
        TabletCounters.Percentile()[COUNTER_LATENCY_PQ_READ_OK].IncrementFor((ctx.Now() - info.Timestamp).MilliSeconds());
        TabletCounters.Cumulative()[COUNTER_PQ_READ_BYTES].Increment(resp.ByteSize());
    }
    ctx.Send(info.Destination != 0 ? Tablet : ctx.SelfID, answer.Event.Release());
    OnReadRequestFinished(std::move(info), answer.Size);
}


template <typename T> // TCmdReadResult
static void AddResultBlob(T* read, const TClientBlob& blob, ui64 offset) {
    auto cc = read->AddResult();
    cc->SetOffset(offset);
    cc->SetData(blob.Data);
    cc->SetSourceId(blob.SourceId);
    cc->SetSeqNo(blob.SeqNo);
    cc->SetWriteTimestampMS(blob.WriteTimestamp.MilliSeconds());
    cc->SetCreateTimestampMS(blob.CreateTimestamp.MilliSeconds());
    cc->SetUncompressedSize(blob.UncompressedSize);
    cc->SetPartitionKey(blob.PartitionKey);
    cc->SetExplicitHash(blob.ExplicitHashKey);

    if (blob.PartData) {
        cc->SetPartNo(blob.PartData->PartNo);
        cc->SetTotalParts(blob.PartData->TotalParts);
        if (blob.PartData->PartNo == 0)
            cc->SetTotalSize(blob.PartData->TotalSize);
    }
}

template <typename T>
static void AddResultDebugInfo(const TEvPQ::TEvBlobResponse* response, T* readResult) {
    ui64 cachedSize = 0;
    ui32 cachedBlobs = 0;
    ui32 diskBlobs = 0;
    for (auto blob : response->GetBlobs()) {
        if (blob.Cached) {
            ++cachedBlobs;
            cachedSize += blob.Size;
        } else
            ++diskBlobs;
    }
    if (cachedSize)
        readResult->SetBlobsCachedSize(cachedSize);
    if (cachedBlobs)
        readResult->SetBlobsFromCache(cachedBlobs);
    if (diskBlobs)
        readResult->SetBlobsFromDisk(diskBlobs);
}

TReadAnswer TReadInfo::FormAnswer(
    const TActorContext& ctx,
    const TEvPQ::TEvBlobResponse& blobResponse,
    const ui64 endOffset,
    const ui32 partition,
    TUserInfo* userInfo,
    const ui64 cookie,
    const ui64 sizeLag,
    const TActorId& tablet,
    const NKikimrPQ::TPQTabletConfig::EMeteringMode meteringMode
) {
    Y_UNUSED(meteringMode);
    Y_UNUSED(partition);
    THolder<TEvPQ::TEvProxyResponse> answer = MakeHolder<TEvPQ::TEvProxyResponse>(cookie);
    NKikimrClient::TResponse& res = answer->Response;
    const TEvPQ::TEvBlobResponse* response = &blobResponse;

    if (HasError(blobResponse)) {
        return TReadAnswer{
            blobResponse.Error.ErrorStr.size(),
            MakeHolder<TEvPQ::TEvError>(blobResponse.Error.ErrorCode, blobResponse.Error.ErrorStr, cookie)
        };
    }

    res.SetStatus(NMsgBusProxy::MSTATUS_OK);
    res.SetErrorCode(NPersQueue::NErrorCode::OK);
    auto readResult = res.MutablePartitionResponse()->MutableCmdReadResult();
    readResult->SetWaitQuotaTimeMs(WaitQuotaTime.MilliSeconds());
    readResult->SetMaxOffset(endOffset);
    readResult->SetRealReadOffset(Offset);
    readResult->SetReadFromTimestampMs(ReadTimestampMs);
    Y_VERIFY(endOffset <= (ui64)Max<i64>(), "Max offset is too big: %" PRIu64, endOffset);

    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "FormAnswer " << Blobs.size());

    AddResultDebugInfo(response, readResult);

    ui32 cnt = 0;
    ui32 size = 0;

    ui32 lastBlobSize = 0;
    const TVector<TRequestedBlob>& blobs = response->GetBlobs();

    auto updateUsage = [&](const TClientBlob& blob) {
        size += blob.GetBlobSize();
        lastBlobSize += blob.GetBlobSize();
        if (blob.IsLastPart()) {
            bool messageSkippingBehaviour = AppData()->PQConfig.GetTopicsAreFirstClassCitizen() &&
                    ReadTimestampMs > blob.WriteTimestamp.MilliSeconds();
            ++cnt;
            if (messageSkippingBehaviour) {
                --cnt;
                size -= lastBlobSize;
            }
            lastBlobSize = 0;
            return (size >= Size || cnt >= Count);
        }
        return !AppData()->PQConfig.GetTopicsAreFirstClassCitizen() && (size >= Size || cnt >= Count);
    };

    Y_VERIFY(blobs.size() == Blobs.size());
    response->Check();
    bool needStop = false;
    for (ui32 pos = 0; pos < blobs.size() && !needStop; ++pos) {
        Y_VERIFY(Blobs[pos].Offset == blobs[pos].Offset, "Mismatch %" PRIu64 " vs %" PRIu64, Blobs[pos].Offset, blobs[pos].Offset);
        Y_VERIFY(Blobs[pos].Count == blobs[pos].Count, "Mismatch %" PRIu32 " vs %" PRIu32, Blobs[pos].Count, blobs[pos].Count);

        ui64 offset = blobs[pos].Offset;
        ui32 count = blobs[pos].Count;
        ui16 partNo = blobs[pos].PartNo;
        ui16 internalPartsCount = blobs[pos].InternalPartsCount;
        const TString& blobValue = blobs[pos].Value;

        if (blobValue.empty()) { // this is ok. Means that someone requested too much data or retention race
            LOG_DEBUG(ctx, NKikimrServices::PERSQUEUE, "Not full answer here!");
            ui64 answerSize = answer->Response.ByteSize();
            if (userInfo && Destination != 0) {
                userInfo->ReadDone(ctx, ctx.Now(), answerSize, cnt, ClientDC,
                        tablet);
            }
            readResult->SetSizeLag(sizeLag - size);
            return {answerSize, std::move(answer)};
        }
        Y_VERIFY(blobValue.size() == blobs[pos].Size, "value for offset %" PRIu64 " count %u size must be %u, but got %u",
                                                        offset, count, blobs[pos].Size, (ui32)blobValue.size());

        if (offset > Offset || (offset == Offset && partNo > PartNo)) { // got gap
            Offset = offset;
            PartNo = partNo;
        }
        Y_VERIFY(offset <= Offset);
        Y_VERIFY(offset < Offset || partNo <= PartNo);
        TKey key(TKeyPrefix::TypeData, 0, offset, partNo, count, internalPartsCount, false);
        for (TBlobIterator it(key, blobValue); it.IsValid() && !needStop; it.Next()) {
            TBatch batch = it.GetBatch();
            auto& header = batch.Header;
            batch.Unpack();

            ui32 pos = 0;
            if (header.GetOffset() > Offset || header.GetOffset() == Offset && header.GetPartNo() >= PartNo) {
                pos = 0;
            } else {
                pos = batch.FindPos(Offset, PartNo);
            }
            offset += header.GetCount();

            if (pos == Max<ui32>()) // this batch does not contain data to read, skip it
                continue;


            LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "FormAnswer processing batch offset "
                << (offset - header.GetCount()) <<  " totakecount " << count << " count " << header.GetCount() << " size " << header.GetPayloadSize() << " from pos " << pos << " cbcount " << batch.Blobs.size());

            ui32 i = 0;
            for (i = pos; i < batch.Blobs.size(); ++i) {
                TClientBlob &res = batch.Blobs[i];
                VERIFY_RESULT_BLOB(res, i);

                Y_VERIFY(PartNo == res.GetPartNo(), "pos %" PRIu32 " i %" PRIu32 " Offset %" PRIu64 " PartNo %" PRIu16 " offset %" PRIu64 " partNo %" PRIu16,
                         pos, i, Offset, PartNo, offset, res.GetPartNo());

                if (userInfo) {
                    userInfo->AddTimestampToCache(
                                                  Offset, res.WriteTimestamp, res.CreateTimestamp,
                                                  Destination != 0, ctx.Now()
                                              );
                }

                AddResultBlob(readResult, res, Offset);

                if (res.IsLastPart()) {
                    PartNo = 0;
                    ++Offset;
                } else {
                    ++PartNo;
                }
                if (updateUsage(res)) {
                    break;
                }
            }

            if (i != batch.Blobs.size()) {//not fully processed batch - next definetely will not be processed
                needStop = true;
            }
        }
    }

    if (!needStop && cnt < Count && size < Size) { // body blobs are fully processed and need to take more data
        if (CachedOffset > Offset) {
            lastBlobSize = 0;
            Offset = CachedOffset;
        }

        for (const auto& writeBlob : Cached) {
            VERIFY_RESULT_BLOB(writeBlob, 0u);

            readResult->SetBlobsCachedSize(readResult->GetBlobsCachedSize() + writeBlob.GetBlobSize());

            if (userInfo) {
                userInfo->AddTimestampToCache(
                    Offset, writeBlob.WriteTimestamp, writeBlob.CreateTimestamp,
                    Destination != 0, ctx.Now()
                );
            }
            AddResultBlob(readResult, writeBlob, Offset);

            if (writeBlob.IsLastPart()) {
                ++Offset;
            }
            if (updateUsage(writeBlob)) {
                break;
            }
        }
    }
    Y_VERIFY(Offset <= (ui64)Max<i64>(), "Offset is too big: %" PRIu64, Offset);
    ui64 answerSize = answer->Response.ByteSize();
    if (userInfo && Destination != 0) {
        userInfo->ReadDone(ctx, ctx.Now(), answerSize, cnt, ClientDC,
                        tablet);

    }
    readResult->SetSizeLag(sizeLag - size);
    return {answerSize, std::move(answer)};
}


void TPartition::HandleOnIdle(TEvPQ::TEvWrite::TPtr& ev, const TActorContext& ctx) {
    HandleOnWrite(ev, ctx);
    HandleWrites(ctx);
}


void TPartition::Handle(TEvPQ::TEvReadTimeout::TPtr& ev, const TActorContext& ctx) {
    auto res = Subscriber.OnTimeout(ev);
    if (!res)
        return;
    TReadAnswer answer(res->FormAnswer(ctx, res->Offset, Partition, nullptr, res->Destination, 0, Tablet, Config.GetMeteringMode()));
    ctx.Send(Tablet, answer.Event.Release());
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, " waiting read cookie " << ev->Get()->Cookie
        << " partition " << Partition << " read timeout for " << res->User << " offset " << res->Offset);
    auto& userInfo = UsersInfoStorage->GetOrCreate(res->User, ctx);

    userInfo.ForgetSubscription(ctx.Now());
    OnReadRequestFinished(std::move(res.GetRef()), answer.Size);
}


TVector<TRequestedBlob> TPartition::GetReadRequestFromBody(const ui64 startOffset, const ui16 partNo, const ui32 maxCount, const ui32 maxSize, ui32* rcount, ui32* rsize) {
    Y_VERIFY(rcount && rsize);
    ui32& count = *rcount;
    ui32& size = *rsize;
    count = size = 0;
    TVector<TRequestedBlob> blobs;
    if (!DataKeysBody.empty() && (Head.Offset > startOffset || Head.Offset == startOffset && Head.PartNo > partNo)) { //will read smth from body
        auto it = std::upper_bound(DataKeysBody.begin(), DataKeysBody.end(), std::make_pair(startOffset, partNo),
            [](const std::pair<ui64, ui16>& offsetAndPartNo, const TDataKey& p) { return offsetAndPartNo.first < p.Key.GetOffset() || offsetAndPartNo.first == p.Key.GetOffset() && offsetAndPartNo.second < p.Key.GetPartNo();});
        if (it == DataKeysBody.begin()) //could be true if data is deleted or gaps are created
            return blobs;
        Y_VERIFY(it != DataKeysBody.begin()); //always greater, startoffset can't be less that StartOffset
        Y_VERIFY(it == DataKeysBody.end() || it->Key.GetOffset() > startOffset || it->Key.GetOffset() == startOffset && it->Key.GetPartNo() > partNo);
        --it;
        Y_VERIFY(it->Key.GetOffset() < startOffset || (it->Key.GetOffset() == startOffset && it->Key.GetPartNo() <= partNo));
        ui32 cnt = 0;
        ui32 sz = 0;
        if (startOffset > it->Key.GetOffset() + it->Key.GetCount()) { //there is a gap
            ++it;
            if (it != DataKeysBody.end()) {
                cnt = it->Key.GetCount();
                sz = it->Size;
            }
        } else {
            Y_VERIFY(it->Key.GetCount() >= (startOffset - it->Key.GetOffset()));
            cnt = it->Key.GetCount() - (startOffset - it->Key.GetOffset()); //don't count all elements from first blob
            sz = (cnt == it->Key.GetCount() ? it->Size : 0); //not readed client blobs can be of ~8Mb, so don't count this size at all
        }
        while (it != DataKeysBody.end() && (size < maxSize && count < maxCount || count == 0)) { //count== 0 grants that blob with offset from ReadFromTimestamp will be readed
            size += sz;
            count += cnt;
            TRequestedBlob reqBlob(it->Key.GetOffset(), it->Key.GetPartNo(), it->Key.GetCount(),
                                   it->Key.GetInternalPartsCount(), it->Size, TString(), it->Key);
            blobs.push_back(reqBlob);

            ++it;
            if (it == DataKeysBody.end())
                break;
            sz = it->Size;
            cnt = it->Key.GetCount();
        }
    }
    return blobs;
}



TVector<TClientBlob> TPartition::GetReadRequestFromHead(const ui64 startOffset, const ui16 partNo, const ui32 maxCount, const ui32 maxSize, const ui64 readTimestampMs, ui32* rcount, ui32* rsize, ui64* insideHeadOffset) {
    Y_UNUSED(readTimestampMs);
    ui32& count = *rcount;
    ui32& size = *rsize;
    TVector<TClientBlob> res;
    std::optional<ui64> firstAddedBlobOffset{};
    ui32 pos = 0;
    if (startOffset > Head.Offset || startOffset == Head.Offset && partNo > Head.PartNo) {
        pos = Head.FindPos(startOffset, partNo);
        Y_VERIFY(pos != Max<ui32>());
    }
    ui32 lastBlobSize = 0;
    for (;pos < Head.Batches.size(); ++pos) {

        TVector<TClientBlob> blobs;
        Head.Batches[pos].UnpackTo(&blobs);
        ui32 i = 0;
        ui64 offset = Head.Batches[pos].GetOffset();
        ui16 pno = Head.Batches[pos].GetPartNo();
        for (; i < blobs.size(); ++i) {

            ui64 curOffset = offset;

            Y_VERIFY(pno == blobs[i].GetPartNo());
            bool skip = offset < startOffset || offset == startOffset &&
                blobs[i].GetPartNo() < partNo;
            if (blobs[i].IsLastPart()) {
                ++offset;
                pno = 0;
            } else {
                ++pno;
            }
            if (skip) continue;
            if (blobs[i].IsLastPart()) {
                bool messageSkippingBehaviour = AppData()->PQConfig.GetTopicsAreFirstClassCitizen() &&
                        readTimestampMs > blobs[i].WriteTimestamp.MilliSeconds();
                ++count;
                if (messageSkippingBehaviour) { //do not count in limits; message will be skippend in proxy
                    --count;
                    size -= lastBlobSize;
                }
                lastBlobSize = 0;

                if (count > maxCount) // blob is counted already
                    break;
                if (size > maxSize)
                    break;
            }
            size += blobs[i].GetBlobSize();
            lastBlobSize += blobs[i].GetBlobSize();
            res.push_back(blobs[i]);

            if (!firstAddedBlobOffset)
                firstAddedBlobOffset = curOffset;

        }
        if (i < blobs.size()) // already got limit
            break;
    }
    *insideHeadOffset = firstAddedBlobOffset.value_or(*insideHeadOffset);
    return res;
}

void TPartition::Handle(TEvPQ::TEvRead::TPtr& ev, const TActorContext& ctx) {
    auto read = ev->Get();

    if (read->Count == 0) {
        TabletCounters.Cumulative()[COUNTER_PQ_READ_ERROR].Increment(1);
        TabletCounters.Percentile()[COUNTER_LATENCY_PQ_READ_ERROR].IncrementFor(0);
        ReplyError(ctx, read->Cookie,  NPersQueue::NErrorCode::BAD_REQUEST, "no infinite flows allowed - count is not set or 0");
        return;
    }
    if (read->Offset < StartOffset) {
        TabletCounters.Cumulative()[COUNTER_PQ_READ_ERROR_SMALL_OFFSET].Increment(1);
        read->Offset = StartOffset;
        if (read->PartNo > 0) {
            LOG_ERROR_S(ctx, NKikimrServices::PERSQUEUE,
                        "I was right, there could be rewinds and deletions at once! Topic " << TopicConverter->GetClientsideName() <<
                        " partition " << Partition <<
                        " readOffset " << read->Offset <<
                        " readPartNo " << read->PartNo <<
                        " startOffset " << StartOffset);
            ReplyError(ctx, read->Cookie,  NPersQueue::NErrorCode::READ_ERROR_TOO_SMALL_OFFSET,
                       "client requested not from first part, and this part is lost");
            return;
        }
    }
    if (read->Offset > EndOffset || read->Offset == EndOffset && read->PartNo > 0) {
        TabletCounters.Cumulative()[COUNTER_PQ_READ_ERROR_BIG_OFFSET].Increment(1);
        TabletCounters.Percentile()[COUNTER_LATENCY_PQ_READ_ERROR].IncrementFor(0);
        LOG_ERROR_S(ctx, NKikimrServices::PERSQUEUE,
                    "reading from too big offset - topic " << TopicConverter->GetClientsideName() <<
                    " partition " << Partition <<
                    " client " << read->ClientId <<
                    " EndOffset " << EndOffset <<
                    " offset " << read->Offset);
        ReplyError(ctx, read->Cookie, NPersQueue::NErrorCode::READ_ERROR_TOO_BIG_OFFSET,
                                      TStringBuilder() << "trying to read from future. ReadOffset " <<
                                      read->Offset << ", " << read->PartNo << " EndOffset " << EndOffset);
        return;
    }

    const TString& user = read->ClientId;

    Y_VERIFY(read->Offset <= EndOffset);

    auto& userInfo = UsersInfoStorage->GetOrCreate(user, ctx);

    if (!read->SessionId.empty()) {
        if (userInfo.Session != read->SessionId) {
            TabletCounters.Cumulative()[COUNTER_PQ_READ_ERROR_NO_SESSION].Increment(1);
            TabletCounters.Percentile()[COUNTER_LATENCY_PQ_READ_ERROR].IncrementFor(0);
            ReplyError(ctx, read->Cookie, NPersQueue::NErrorCode::READ_ERROR_NO_SESSION,
                TStringBuilder() << "no such session '" << read->SessionId << "'");
            return;
        }
    }

    if (userInfo.ReadSpeedLimiter) {
        Send(userInfo.ReadSpeedLimiter->Actor, new NReadSpeedLimiterEvents::TEvRequest(ev.Release()));
    } else {
        DoRead(ev.Release(), TDuration::Zero(), ctx);
    }
}

void TPartition::Handle(NReadSpeedLimiterEvents::TEvResponse::TPtr& ev, const TActorContext& ctx) {
    DoRead(ev->Get()->ReadRequest.Release(), ev->Get()->WaitTime, ctx);
}

void TPartition::DoRead(TEvPQ::TEvRead::TPtr ev, TDuration waitQuotaTime, const TActorContext& ctx) {
    auto read = ev->Get();
    const TString& user = read->ClientId;
    auto& userInfo = UsersInfoStorage->GetOrCreate(user, ctx);

    ui64 offset = read->Offset;
    if (read->PartNo == 0 && (read->MaxTimeLagMs > 0 || read->ReadTimestampMs > 0 || userInfo.ReadFromTimestamp > TInstant::MilliSeconds(1))) {
        TInstant timestamp = read->MaxTimeLagMs > 0 ? ctx.Now() - TDuration::MilliSeconds(read->MaxTimeLagMs) : TInstant::Zero();
        timestamp = Max(timestamp, TInstant::MilliSeconds(read->ReadTimestampMs));
        timestamp = Max(timestamp, userInfo.ReadFromTimestamp);
        offset = Max(GetOffsetEstimate(DataKeysBody, timestamp, Min(Head.Offset, EndOffset - 1)), offset);
        userInfo.ReadOffsetRewindSum += offset - read->Offset;
    }

    TReadInfo info(user, read->ClientDC, offset, read->PartNo, read->Count, read->Size, read->Cookie, read->ReadTimestampMs, waitQuotaTime);

    ui64 cookie = Cookie++;

    LOG_DEBUG_S(
            ctx, NKikimrServices::PERSQUEUE,
            "read cookie " << cookie << " Topic '" << TopicConverter->GetClientsideName() << "' partition " << Partition
                << " user " << user
                << " offset " << read->Offset << " count " << read->Count << " size " << read->Size << " endOffset " << EndOffset
                << " max time lag " << read->MaxTimeLagMs << "ms effective offset " << offset
    );


    if (offset == EndOffset) {
        if (read->Timeout > 30000) {
            LOG_DEBUG_S(
                    ctx, NKikimrServices::PERSQUEUE,
                    "too big read timeout " << " Topic '" << TopicConverter->GetClientsideName() << "' partition " << Partition
                        << " user " << read->ClientId << " offset " << read->Offset << " count " << read->Count
                        << " size " << read->Size << " endOffset " << EndOffset << " max time lag " << read->MaxTimeLagMs
                        << "ms effective offset " << offset
            );
            read->Timeout = 30000;
        }
        Subscriber.AddSubscription(std::move(info), read->Timeout, cookie, ctx);
        ++userInfo.Subscriptions;
        userInfo.UpdateReadOffset((i64)offset - 1, userInfo.WriteTimestamp, userInfo.CreateTimestamp, ctx.Now());

        return;
    }

    Y_VERIFY(offset < EndOffset);

    ProcessRead(ctx, std::move(info), cookie, false);
}

void TPartition::OnReadRequestFinished(TReadInfo&& info, ui64 answerSize) {
    auto userInfo = UsersInfoStorage->GetIfExists(info.User);
    Y_VERIFY(userInfo);

    if (Config.GetMeteringMode() == NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS) {
        return;
    }

    if (userInfo->ReadSpeedLimiter) {
        Send(
            userInfo->ReadSpeedLimiter->Actor,
            new NReadSpeedLimiterEvents::TEvConsumed(answerSize, info.Destination)
        );
    }
}

void TPartition::AnswerCurrentWrites(const TActorContext& ctx) {
    ui64 offset = EndOffset;
    while (!Responses.empty()) {
        const auto& response = Responses.front();

        const TDuration quotedTime = response.QuotedTime;
        const TDuration queueTime = response.QueueTime;
        const TDuration writeTime = ctx.Now() - response.WriteTimeBaseline;

        if (response.IsWrite()) {
            const auto& writeResponse = response.GetWrite();
            const TString& s = writeResponse.Msg.SourceId;
            const ui64& seqNo = writeResponse.Msg.SeqNo;
            const ui16& partNo = writeResponse.Msg.PartNo;
            const ui16& totalParts = writeResponse.Msg.TotalParts;
            const TMaybe<ui64>& wrOffset = writeResponse.Offset;

            bool already = false;

            auto it = SourceIdStorage.GetInMemorySourceIds().find(s);

            ui64 maxSeqNo = 0;
            ui64 maxOffset = 0;

            if (it != SourceIdStorage.GetInMemorySourceIds().end()) {
                maxSeqNo = it->second.SeqNo;
                maxOffset = it->second.Offset;
                if (it->second.SeqNo >= seqNo && !writeResponse.Msg.DisableDeduplication) {
                    already = true;
                }
            }

            if (!already) {
                if (wrOffset) {
                    Y_VERIFY(*wrOffset >= offset);
                    offset = *wrOffset;
                }
            }
            if (!already && partNo + 1 == totalParts) {
                if (it == SourceIdStorage.GetInMemorySourceIds().end()) {
                    TabletCounters.Cumulative()[COUNTER_PQ_SID_CREATED].Increment(1);
                    SourceIdStorage.RegisterSourceId(s, writeResponse.Msg.SeqNo, offset, CurrentTimestamp);
                } else {
                    SourceIdStorage.RegisterSourceId(s, it->second.Updated(writeResponse.Msg.SeqNo, offset, CurrentTimestamp));
                }

                TabletCounters.Cumulative()[COUNTER_PQ_WRITE_OK].Increment(1);
            }
            ReplyWrite(
                ctx, writeResponse.Cookie, s, seqNo, partNo, totalParts,
                already ? maxOffset : offset, CurrentTimestamp, already, maxSeqNo,
                quotedTime, TopicQuotaWaitTimeForCurrentBlob, queueTime, writeTime
            );
            LOG_DEBUG_S(
                ctx,
                NKikimrServices::PERSQUEUE,
                "Answering for message sourceid: '" << EscapeC(s) <<
                "', Topic: '" << TopicConverter->GetClientsideName() <<
                "', Partition: " << Partition <<
                ", SeqNo: " << seqNo << ", partNo: " << partNo <<
                ", Offset: " << offset << " is " << (already ? "already written" : "stored on disk")
            );
            if (PartitionWriteQuotaWaitCounter) {
                PartitionWriteQuotaWaitCounter->IncFor(quotedTime.MilliSeconds());
            }

            if (!already && partNo + 1 == totalParts)
                ++offset;
        } else if (response.IsOwnership()) {
            const TString& ownerCookie = response.GetOwnership().OwnerCookie;
            auto it = Owners.find(TOwnerInfo::GetOwnerFromOwnerCookie(ownerCookie));
            if (it != Owners.end() && it->second.OwnerCookie == ownerCookie) {
                ReplyOwnerOk(ctx, response.GetCookie(), ownerCookie);
            } else {
                ReplyError(ctx, response.GetCookie(), NPersQueue::NErrorCode::WRONG_COOKIE, "new GetOwnership request is dropped already");
            }
        } else if (response.IsRegisterMessageGroup()) {
            const auto& body = response.GetRegisterMessageGroup().Body;

            TMaybe<TPartitionKeyRange> keyRange;
            if (body.KeyRange) {
                keyRange = TPartitionKeyRange::Parse(*body.KeyRange);
            }

            Y_VERIFY(body.AssignedOffset);
            SourceIdStorage.RegisterSourceId(body.SourceId, body.SeqNo, *body.AssignedOffset, CurrentTimestamp, std::move(keyRange));
            ReplyOk(ctx, response.GetCookie());
        } else if (response.IsDeregisterMessageGroup()) {
            const auto& body = response.GetDeregisterMessageGroup().Body;

            SourceIdStorage.DeregisterSourceId(body.SourceId);
            ReplyOk(ctx, response.GetCookie());
        } else if (response.IsSplitMessageGroup()) {
            const auto& split = response.GetSplitMessageGroup();

            for (const auto& body : split.Deregistrations) {
                SourceIdStorage.DeregisterSourceId(body.SourceId);
            }

            for (const auto& body : split.Registrations) {
                TMaybe<TPartitionKeyRange> keyRange;
                if (body.KeyRange) {
                    keyRange = TPartitionKeyRange::Parse(*body.KeyRange);
                }

                Y_VERIFY(body.AssignedOffset);
                SourceIdStorage.RegisterSourceId(body.SourceId, body.SeqNo, *body.AssignedOffset, CurrentTimestamp, std::move(keyRange), true);
            }

            ReplyOk(ctx, response.GetCookie());
        } else {
            Y_FAIL("Unexpected message");
        }
        Responses.pop_front();
    }
    TopicQuotaWaitTimeForCurrentBlob = TDuration::Zero();
}


void TPartition::ReadTimestampForOffset(const TString& user, TUserInfo& userInfo, const TActorContext& ctx) {
    if (userInfo.ReadScheduled)
        return;
    userInfo.ReadScheduled = true;
    LOG_DEBUG_S(
            ctx, NKikimrServices::PERSQUEUE,
            "Topic '" << TopicConverter->GetClientsideName() << "' partition " << Partition <<
            " user " << user << " readTimeStamp for offset " << userInfo.Offset << " initiated " <<
            " queuesize " << UpdateUserInfoTimestamp.size() << " startOffset " << StartOffset <<
            " ReadingTimestamp " << ReadingTimestamp << " rrg " << userInfo.ReadRuleGeneration
    );

    if (ReadingTimestamp) {
        UpdateUserInfoTimestamp.push_back(std::make_pair(user, userInfo.ReadRuleGeneration));
        return;
    }
    if (userInfo.Offset < (i64)StartOffset) {
        userInfo.ReadScheduled = false;
        auto now = ctx.Now();
        userInfo.CreateTimestamp = now - TDuration::Seconds(Max(86400, Config.GetPartitionConfig().GetLifetimeSeconds()));
        userInfo.WriteTimestamp = now - TDuration::Seconds(Max(86400, Config.GetPartitionConfig().GetLifetimeSeconds()));
        userInfo.ActualTimestamps = true;
        if (userInfo.ReadOffset + 1 < userInfo.Offset) {
            userInfo.ReadOffset = userInfo.Offset - 1;
            userInfo.ReadCreateTimestamp = userInfo.CreateTimestamp;
            userInfo.ReadWriteTimestamp = userInfo.WriteTimestamp;
        }

        TabletCounters.Cumulative()[COUNTER_PQ_WRITE_TIMESTAMP_OFFSET_IS_LOST].Increment(1);
        return;
    }

    if (userInfo.Offset >= (i64)EndOffset || StartOffset == EndOffset) {
        userInfo.ReadScheduled = false;
        return;
    }

    Y_VERIFY(!ReadingTimestamp);

    ReadingTimestamp = true;
    ReadingForUser = user;
    ReadingForOffset = userInfo.Offset;
    ReadingForUserReadRuleGeneration = userInfo.ReadRuleGeneration;

    for (const auto& user : UpdateUserInfoTimestamp) {
        Y_VERIFY(user.first != ReadingForUser || user.second != ReadingForUserReadRuleGeneration);
    }

    LOG_DEBUG_S(
            ctx, NKikimrServices::PERSQUEUE,
            "Topic '" << TopicConverter->GetClientsideName() << "' partition " << Partition
            << " user " << user << " send read request for offset " << userInfo.Offset << " initiated "
            << " queuesize " << UpdateUserInfoTimestamp.size() << " startOffset " << StartOffset
            << " ReadingTimestamp " << ReadingTimestamp << " rrg " << ReadingForUserReadRuleGeneration
    );


    THolder<TEvPQ::TEvRead> event = MakeHolder<TEvPQ::TEvRead>(0, userInfo.Offset, 0, 1, "",
                                                               user, 0, MAX_BLOB_PART_SIZE * 2, 0, 0, "",
                                                               false);

    ctx.Send(ctx.SelfID, event.Release());
    TabletCounters.Cumulative()[COUNTER_PQ_WRITE_TIMESTAMP_CACHE_MISS].Increment(1);
}

void TPartition::ProcessTimestampsForNewData(const ui64 prevEndOffset, const TActorContext& ctx) {
    for (auto& userInfoPair : UsersInfoStorage->GetAll()) {
        if (userInfoPair.second.Offset >= (i64)prevEndOffset && userInfoPair.second.Offset < (i64)EndOffset) {
            ReadTimestampForOffset(userInfoPair.first, userInfoPair.second, ctx);
        }
    }
}

void TPartition::Handle(TEvPQ::TEvProxyResponse::TPtr& ev, const TActorContext& ctx) {
    ReadingTimestamp = false;
    auto userInfo = UsersInfoStorage->GetIfExists(ReadingForUser);
    if (!userInfo || userInfo->ReadRuleGeneration != ReadingForUserReadRuleGeneration) {
        LOG_INFO_S(
            ctx, NKikimrServices::PERSQUEUE,
            "Topic '" << TopicConverter->GetClientsideName() << "'" <<
            " partition " << Partition <<
            " user " << ReadingForUser <<
            " readTimeStamp for other generation or no client info at all"
        );

        ProcessTimestampRead(ctx);
        return;
    }

    LOG_DEBUG_S(
            ctx, NKikimrServices::PERSQUEUE,
            "Topic '" << TopicConverter->GetClientsideName() << "'" <<
            " partition " << Partition <<
            " user " << ReadingForUser <<
            " readTimeStamp done, result " << userInfo->WriteTimestamp.MilliSeconds() <<
            " queuesize " << UpdateUserInfoTimestamp.size() <<
            " startOffset " << StartOffset
    );
    Y_VERIFY(userInfo->ReadScheduled);
    userInfo->ReadScheduled = false;
    Y_VERIFY(ReadingForUser != "");

    if (!userInfo->ActualTimestamps) {
        LOG_INFO_S(
            ctx,
            NKikimrServices::PERSQUEUE,
            "Reading Timestamp failed for offset " << ReadingForOffset << " ( "<< userInfo->Offset << " ) " << ev->Get()->Response.DebugString()
        );
        if (ev->Get()->Response.GetStatus() == NMsgBusProxy::MSTATUS_OK &&
            ev->Get()->Response.GetErrorCode() == NPersQueue::NErrorCode::OK &&
            ev->Get()->Response.GetPartitionResponse().HasCmdReadResult() &&
            ev->Get()->Response.GetPartitionResponse().GetCmdReadResult().ResultSize() > 0 &&
            (i64)ev->Get()->Response.GetPartitionResponse().GetCmdReadResult().GetResult(0).GetOffset() >= userInfo->Offset) {
                //offsets is inside gap - return timestamp of first record after gap
            const auto& res = ev->Get()->Response.GetPartitionResponse().GetCmdReadResult().GetResult(0);
            userInfo->WriteTimestamp = TInstant::MilliSeconds(res.GetWriteTimestampMS());
            userInfo->CreateTimestamp = TInstant::MilliSeconds(res.GetCreateTimestampMS());
            userInfo->ActualTimestamps = true;
            if (userInfo->ReadOffset + 1 < userInfo->Offset) {
                userInfo->ReadOffset = userInfo->Offset - 1;
                userInfo->ReadWriteTimestamp = userInfo->WriteTimestamp;
                userInfo->ReadCreateTimestamp = userInfo->CreateTimestamp;
            }
        } else {
            UpdateUserInfoTimestamp.push_back(std::make_pair(ReadingForUser, ReadingForUserReadRuleGeneration));
            userInfo->ReadScheduled = true;
        }
        TabletCounters.Cumulative()[COUNTER_PQ_WRITE_TIMESTAMP_ERROR].Increment(1);
    }
    ProcessTimestampRead(ctx);
}


void TPartition::ProcessTimestampRead(const TActorContext& ctx) {
    ReadingForUser = "";
    ReadingForOffset = 0;
    ReadingForUserReadRuleGeneration = 0;
    while (!ReadingTimestamp && !UpdateUserInfoTimestamp.empty()) {
        TString user = UpdateUserInfoTimestamp.front().first;
        ui64 readRuleGeneration = UpdateUserInfoTimestamp.front().second;
        UpdateUserInfoTimestamp.pop_front();
        auto userInfo = UsersInfoStorage->GetIfExists(user);
        if (!userInfo || !userInfo->ReadScheduled || userInfo->ReadRuleGeneration != readRuleGeneration)
            continue;
        userInfo->ReadScheduled = false;
        if (userInfo->Offset == (i64)EndOffset)
            continue;
        ReadTimestampForOffset(user, *userInfo, ctx);
    }
    Y_VERIFY(ReadingTimestamp || UpdateUserInfoTimestamp.empty());
}


void TPartition::Handle(TEvPQ::TEvError::TPtr& ev, const TActorContext& ctx) {
    ReadingTimestamp = false;
    auto userInfo = UsersInfoStorage->GetIfExists(ReadingForUser);
    if (!userInfo || userInfo->ReadRuleGeneration != ReadingForUserReadRuleGeneration) {
        ProcessTimestampRead(ctx);
        return;
    }
    Y_VERIFY(userInfo->ReadScheduled);
    Y_VERIFY(ReadingForUser != "");

    LOG_ERROR_S(
            ctx, NKikimrServices::PERSQUEUE,
            "Topic '" << TopicConverter->GetClientsideName() << "' partition " << Partition
                << " user " << ReadingForUser << " readTimeStamp error: " << ev->Get()->Error
    );

    UpdateUserInfoTimestamp.push_back(std::make_pair(ReadingForUser, ReadingForUserReadRuleGeneration));

    ProcessTimestampRead(ctx);
}


void TPartition::CheckHeadConsistency() const {
    ui32 p = 0;
    for (ui32 j = 0; j < DataKeysHead.size(); ++j) {
        ui32 s = 0;
        for (ui32 k = 0; k < DataKeysHead[j].KeysCount(); ++k) {
            Y_VERIFY(p < HeadKeys.size());
            Y_VERIFY(DataKeysHead[j].GetKey(k) == HeadKeys[p].Key);
            Y_VERIFY(DataKeysHead[j].GetSize(k) == HeadKeys[p].Size);
            s += DataKeysHead[j].GetSize(k);
            Y_VERIFY(j + 1 == TotalLevels || DataKeysHead[j].GetSize(k) >= CompactLevelBorder[j + 1]);
            ++p;
        }
        Y_VERIFY(s < DataKeysHead[j].Border());
    }
    Y_VERIFY(DataKeysBody.empty() ||
             Head.Offset >= DataKeysBody.back().Key.GetOffset() + DataKeysBody.back().Key.GetCount());
    Y_VERIFY(p == HeadKeys.size());
    if (!HeadKeys.empty()) {
        Y_VERIFY(HeadKeys.size() <= TotalMaxCount);
        Y_VERIFY(HeadKeys.front().Key.GetOffset() == Head.Offset);
        Y_VERIFY(HeadKeys.front().Key.GetPartNo() == Head.PartNo);
        for (p = 1; p < HeadKeys.size(); ++p) {
            Y_VERIFY(HeadKeys[p].Key.GetOffset() == HeadKeys[p-1].Key.GetOffset() + HeadKeys[p-1].Key.GetCount());
            Y_VERIFY(HeadKeys[p].Key.ToString() > HeadKeys[p-1].Key.ToString());
        }
    }
}


void TPartition::SyncMemoryStateWithKVState(const TActorContext& ctx) {
    if (!CompactedKeys.empty())
        HeadKeys.clear();

    if (NewHeadKey.Size > 0) {
        while (!HeadKeys.empty() &&
            (HeadKeys.back().Key.GetOffset() > NewHeadKey.Key.GetOffset() || HeadKeys.back().Key.GetOffset() == NewHeadKey.Key.GetOffset()
                                                                       && HeadKeys.back().Key.GetPartNo() >= NewHeadKey.Key.GetPartNo())) {
                HeadKeys.pop_back();
        }
        HeadKeys.push_back(NewHeadKey);
        NewHeadKey = TDataKey{TKey{}, 0, TInstant::Zero(), 0};
    }

    if (CompactedKeys.empty() && NewHead.PackedSize == 0) { //Nothing writed at all
        return;
    }

    Y_VERIFY(EndOffset == Head.GetNextOffset());

    if (!CompactedKeys.empty() || Head.PackedSize == 0) { //has compactedkeys or head is already empty
        Head.PackedSize = 0;
        Head.Offset = NewHead.Offset;
        Head.PartNo = NewHead.PartNo; //no partNo at this point
        Head.Batches.clear();
    }

    while (!CompactedKeys.empty()) {
        const auto& ck = CompactedKeys.front();
        BodySize += ck.second;
        Y_VERIFY(!ck.first.IsHead());
        ui64 lastOffset = DataKeysBody.empty() ? 0 : (DataKeysBody.back().Key.GetOffset() + DataKeysBody.back().Key.GetCount());
        Y_VERIFY(lastOffset <= ck.first.GetOffset());
        if (DataKeysBody.empty()) {
            StartOffset = ck.first.GetOffset() + (ck.first.GetPartNo() > 0 ? 1 : 0);
        } else {
            if (lastOffset < ck.first.GetOffset()) {
                GapOffsets.push_back(std::make_pair(lastOffset, ck.first.GetOffset()));
                GapSize += ck.first.GetOffset() - lastOffset;
            }
        }
        DataKeysBody.push_back({ck.first, ck.second, ctx.Now(), DataKeysBody.empty() ? 0 : DataKeysBody.back().CumulativeSize + DataKeysBody.back().Size});

        CompactedKeys.pop_front();
    } // head cleared, all data moved to body

    //append Head with newHead
    while (!NewHead.Batches.empty()) {
        Head.Batches.push_back(NewHead.Batches.front());
        NewHead.Batches.pop_front();
    }
    Head.PackedSize += NewHead.PackedSize;

    if (Head.PackedSize > 0 && DataKeysBody.empty()) {
        StartOffset = Head.Offset + (Head.PartNo > 0 ? 1 : 0);
    }

    EndOffset = Head.GetNextOffset();
    NewHead.Clear();
    NewHead.Offset = EndOffset;

    CheckHeadConsistency();

    UpdateUserInfoEndOffset(ctx.Now());
}


ui64 TPartition::GetSizeLag(i64 offset) {
    ui64 sizeLag = 0;
    if (!DataKeysBody.empty() && (offset < (i64)Head.Offset || offset == (i64)Head.Offset && Head.PartNo > 0)) { //there will be something in body
        auto it = std::upper_bound(DataKeysBody.begin(), DataKeysBody.end(), std::make_pair(offset, 0),
                [](const std::pair<ui64, ui16>& offsetAndPartNo, const TDataKey& p) { return offsetAndPartNo.first < p.Key.GetOffset() || offsetAndPartNo.first == p.Key.GetOffset() && offsetAndPartNo.second < p.Key.GetPartNo();});
        if (it != DataKeysBody.begin())
            --it; //point to blob with this offset
        Y_VERIFY(it != DataKeysBody.end());
        sizeLag = it->Size + DataKeysBody.back().CumulativeSize - it->CumulativeSize;
        Y_VERIFY(BodySize == DataKeysBody.back().CumulativeSize + DataKeysBody.back().Size - DataKeysBody.front().CumulativeSize);
    }
    for (auto& b : HeadKeys) {
        if ((i64)b.Key.GetOffset() >= offset)
            sizeLag += b.Size;
    }
    return sizeLag;
}


bool TPartition::UpdateCounters(const TActorContext& ctx) {
    if (!PartitionCountersLabeled) {
        return false;
    }
    // per client counters
    const auto now = ctx.Now();
    for (auto& userInfoPair : UsersInfoStorage->GetAll()) {
        auto& userInfo = userInfoPair.second;
        if (!userInfo.LabeledCounters)
            continue;
        if (!userInfo.HasReadRule && !userInfo.Important)
            continue;
        bool haveChanges = false;
        userInfo.EndOffset = EndOffset;
        userInfo.UpdateReadingTimeAndState(now);
        ui64 ts = userInfo.GetWriteTimestamp().MilliSeconds();
        if (ts < MIN_TIMESTAMP_MS) ts = Max<i64>();
        if (userInfo.LabeledCounters->GetCounters()[METRIC_COMMIT_WRITE_TIME].Get() != ts) {
            haveChanges = true;
            userInfo.LabeledCounters->GetCounters()[METRIC_COMMIT_WRITE_TIME].Set(ts);
        }
        ts = userInfo.GetCreateTimestamp().MilliSeconds();
        if (ts < MIN_TIMESTAMP_MS) ts = Max<i64>();
        if (userInfo.LabeledCounters->GetCounters()[METRIC_COMMIT_CREATE_TIME].Get() != ts) {
            haveChanges = true;
            userInfo.LabeledCounters->GetCounters()[METRIC_COMMIT_CREATE_TIME].Set(ts);
        }
        ts = userInfo.GetReadWriteTimestamp().MilliSeconds();
        if (userInfo.LabeledCounters->GetCounters()[METRIC_READ_WRITE_TIME].Get() != ts) {
            haveChanges = true;
            userInfo.LabeledCounters->GetCounters()[METRIC_READ_WRITE_TIME].Set(ts);
        }

        i64 off = userInfo.GetReadOffset(); //we want to track first not-readed offset
        TInstant wts = userInfo.GetReadWriteTimestamp() ? userInfo.GetReadWriteTimestamp() : GetWriteTimeEstimate(userInfo.GetReadOffset());
        TInstant readTimestamp = userInfo.GetReadTimestamp();
        ui64 readTimeLag = off >= (i64)EndOffset ? 0 : (readTimestamp - wts).MilliSeconds();
        ui64 totalLag = userInfo.GetWriteLagMs() + readTimeLag + (now - readTimestamp).MilliSeconds();

        if (userInfo.LabeledCounters->GetCounters()[METRIC_READ_TOTAL_TIME].Get() != totalLag) {
            haveChanges = true;
            userInfo.LabeledCounters->GetCounters()[METRIC_READ_TOTAL_TIME].Set(totalLag);
        }

        ts = readTimestamp.MilliSeconds();
        if (userInfo.LabeledCounters->GetCounters()[METRIC_LAST_READ_TIME].Get() != ts) {
            haveChanges = true;
            userInfo.LabeledCounters->GetCounters()[METRIC_LAST_READ_TIME].Set(ts);
        }

        ui64 timeLag = userInfo.GetWriteLagMs();
        if (userInfo.LabeledCounters->GetCounters()[METRIC_WRITE_TIME_LAG].Get() != timeLag) {
            haveChanges = true;
            userInfo.LabeledCounters->GetCounters()[METRIC_WRITE_TIME_LAG].Set(timeLag);
        }

        if (userInfo.LabeledCounters->GetCounters()[METRIC_READ_TIME_LAG].Get() != readTimeLag) {
            haveChanges = true;
            userInfo.LabeledCounters->GetCounters()[METRIC_READ_TIME_LAG].Set(readTimeLag);
        }

        if (userInfo.LabeledCounters->GetCounters()[METRIC_COMMIT_MESSAGE_LAG].Get() != EndOffset - userInfo.Offset) {
            haveChanges = true;
            userInfo.LabeledCounters->GetCounters()[METRIC_COMMIT_MESSAGE_LAG].Set(EndOffset - userInfo.Offset);
        }

        if (userInfo.LabeledCounters->GetCounters()[METRIC_READ_MESSAGE_LAG].Get() != EndOffset - off) {
            haveChanges = true;
            userInfo.LabeledCounters->GetCounters()[METRIC_READ_MESSAGE_LAG].Set(EndOffset - off);
            userInfo.LabeledCounters->GetCounters()[METRIC_READ_TOTAL_MESSAGE_LAG].Set(EndOffset - off);
        }

        ui64 sizeLag = GetSizeLag(userInfo.Offset);
        if (userInfo.LabeledCounters->GetCounters()[METRIC_COMMIT_SIZE_LAG].Get() != sizeLag) {
            haveChanges = true;
            userInfo.LabeledCounters->GetCounters()[METRIC_COMMIT_SIZE_LAG].Set(sizeLag);
        }

        ui64 sizeLagRead = GetSizeLag(userInfo.ReadOffset);
        if (userInfo.LabeledCounters->GetCounters()[METRIC_READ_SIZE_LAG].Get() != sizeLagRead) {
            haveChanges = true;
            userInfo.LabeledCounters->GetCounters()[METRIC_READ_SIZE_LAG].Set(sizeLagRead);
            userInfo.LabeledCounters->GetCounters()[METRIC_READ_TOTAL_SIZE_LAG].Set(sizeLag);
        }

        if (userInfo.LabeledCounters->GetCounters()[METRIC_USER_PARTITIONS].Get() == 0) {
            haveChanges = true;
            userInfo.LabeledCounters->GetCounters()[METRIC_USER_PARTITIONS].Set(1);
        }

        ui64 speed = userInfo.ReadQuota.GetTotalSpeed();
        if (speed != userInfo.LabeledCounters->GetCounters()[METRIC_READ_QUOTA_BYTES].Get()) {
            haveChanges = true;
            userInfo.LabeledCounters->GetCounters()[METRIC_READ_QUOTA_BYTES].Set(speed);
        }

        ui64 readOffsetRewindSum = userInfo.ReadOffsetRewindSum;
        if (readOffsetRewindSum != userInfo.LabeledCounters->GetCounters()[METRIC_READ_OFFSET_REWIND_SUM].Get()) {
            haveChanges = true;
            userInfo.LabeledCounters->GetCounters()[METRIC_READ_OFFSET_REWIND_SUM].Set(readOffsetRewindSum);
        }

        ui32 id = METRIC_TOTAL_READ_SPEED_1;
        for (ui32 i = 0; i < userInfo.AvgReadBytes.size(); ++i) {
            ui64 avg = userInfo.AvgReadBytes[i].GetValue();
            if (avg != userInfo.LabeledCounters->GetCounters()[id].Get()) {
                haveChanges = true;
                userInfo.LabeledCounters->GetCounters()[id].Set(avg); //total
                userInfo.LabeledCounters->GetCounters()[id + 1].Set(avg); //max
            }
            id += 2;
        }
        Y_VERIFY(id == METRIC_MAX_READ_SPEED_4 + 1);
        if (userInfo.ReadQuota.GetTotalSpeed()) {
            ui64 quotaUsage = ui64(userInfo.AvgReadBytes[1].GetValue()) * 1000000 / userInfo.ReadQuota.GetTotalSpeed() / 60;
            if (quotaUsage != userInfo.LabeledCounters->GetCounters()[METRIC_READ_QUOTA_USAGE].Get()) {
                haveChanges = true;
                userInfo.LabeledCounters->GetCounters()[METRIC_READ_QUOTA_USAGE].Set(quotaUsage);
            }
        }
        if (haveChanges) {
            ctx.Send(Tablet, new TEvPQ::TEvPartitionLabeledCounters(Partition, *userInfo.LabeledCounters));
        }
    }
    bool haveChanges = false;
    if (SourceIdStorage.GetInMemorySourceIds().size() != PartitionCountersLabeled->GetCounters()[METRIC_MAX_NUM_SIDS].Get()) {
        haveChanges = true;
        PartitionCountersLabeled->GetCounters()[METRIC_MAX_NUM_SIDS].Set(SourceIdStorage.GetInMemorySourceIds().size());
        PartitionCountersLabeled->GetCounters()[METRIC_NUM_SIDS].Set(SourceIdStorage.GetInMemorySourceIds().size());
    }

    TDuration lifetimeNow = ctx.Now() - SourceIdStorage.MinAvailableTimestamp(ctx.Now());
    if (lifetimeNow.MilliSeconds() != PartitionCountersLabeled->GetCounters()[METRIC_MIN_SID_LIFETIME].Get()) {
        haveChanges = true;
        PartitionCountersLabeled->GetCounters()[METRIC_MIN_SID_LIFETIME].Set(lifetimeNow.MilliSeconds());
    }

    const ui64 headGapSize = DataKeysBody.empty() ? 0 : (Head.Offset - (DataKeysBody.back().Key.GetOffset() + DataKeysBody.back().Key.GetCount()));
    const ui64 gapSize = GapSize + headGapSize;
    if (gapSize != PartitionCountersLabeled->GetCounters()[METRIC_GAPS_SIZE].Get()) {
        haveChanges = true;
        PartitionCountersLabeled->GetCounters()[METRIC_MAX_GAPS_SIZE].Set(gapSize);
        PartitionCountersLabeled->GetCounters()[METRIC_GAPS_SIZE].Set(gapSize);
    }

    const ui32 gapsCount = GapOffsets.size() + (headGapSize ? 1 : 0);
    if (gapsCount != PartitionCountersLabeled->GetCounters()[METRIC_GAPS_COUNT].Get()) {
        haveChanges = true;
        PartitionCountersLabeled->GetCounters()[METRIC_MAX_GAPS_COUNT].Set(gapsCount);
        PartitionCountersLabeled->GetCounters()[METRIC_GAPS_COUNT].Set(gapsCount);
    }

    ui64 speed = WriteQuota->GetTotalSpeed();
    if (speed != PartitionCountersLabeled->GetCounters()[METRIC_WRITE_QUOTA_BYTES].Get()) {
        haveChanges = true;
        PartitionCountersLabeled->GetCounters()[METRIC_WRITE_QUOTA_BYTES].Set(speed);
    }

    ui32 id = METRIC_TOTAL_WRITE_SPEED_1;
    for (ui32 i = 0; i < AvgWriteBytes.size(); ++i) {
        ui64 avg = AvgWriteBytes[i].GetValue();
        if (avg != PartitionCountersLabeled->GetCounters()[id].Get()) {
            haveChanges = true;
            PartitionCountersLabeled->GetCounters()[id].Set(avg); //total
            PartitionCountersLabeled->GetCounters()[id + 1].Set(avg); //max
        }
        id += 2;
    }
    Y_VERIFY(id == METRIC_MAX_WRITE_SPEED_4 + 1);


    id = METRIC_TOTAL_QUOTA_SPEED_1;
    for (ui32 i = 0; i < AvgQuotaBytes.size(); ++i) {
        ui64 avg = AvgQuotaBytes[i].GetValue();
        if (avg != PartitionCountersLabeled->GetCounters()[id].Get()) {
            haveChanges = true;
            PartitionCountersLabeled->GetCounters()[id].Set(avg); //total
            PartitionCountersLabeled->GetCounters()[id + 1].Set(avg); //max
        }
        id += 2;
    }
    Y_VERIFY(id == METRIC_MAX_QUOTA_SPEED_4 + 1);

    if (WriteQuota->GetTotalSpeed()) {
        ui64 quotaUsage = ui64(AvgQuotaBytes[1].GetValue()) * 1000000 / WriteQuota->GetTotalSpeed() / 60;
        if (quotaUsage != PartitionCountersLabeled->GetCounters()[METRIC_WRITE_QUOTA_USAGE].Get()) {
            haveChanges = true;
            PartitionCountersLabeled->GetCounters()[METRIC_WRITE_QUOTA_USAGE].Set(quotaUsage);
        }
    }

    ui64 partSize = Size();
    if (partSize != PartitionCountersLabeled->GetCounters()[METRIC_TOTAL_PART_SIZE].Get()) {
        haveChanges = true;
        PartitionCountersLabeled->GetCounters()[METRIC_MAX_PART_SIZE].Set(partSize);
        PartitionCountersLabeled->GetCounters()[METRIC_TOTAL_PART_SIZE].Set(partSize);
    }

    ui64 ts = (WriteTimestamp.MilliSeconds() < MIN_TIMESTAMP_MS) ? Max<i64>() : WriteTimestamp.MilliSeconds();
    if (PartitionCountersLabeled->GetCounters()[METRIC_LAST_WRITE_TIME].Get() != ts) {
        haveChanges = true;
        PartitionCountersLabeled->GetCounters()[METRIC_LAST_WRITE_TIME].Set(ts);
    }

    ui64 timeLag = WriteLagMs.GetValue();
    if (PartitionCountersLabeled->GetCounters()[METRIC_WRITE_TIME_LAG_MS].Get() != timeLag) {
        haveChanges = true;
        PartitionCountersLabeled->GetCounters()[METRIC_WRITE_TIME_LAG_MS].Set(timeLag);
    }
    return haveChanges;
}

void TPartition::ReportCounters(const TActorContext& ctx) {
    if (UpdateCounters(ctx)) {
        ctx.Send(Tablet, new TEvPQ::TEvPartitionLabeledCounters(Partition, *PartitionCountersLabeled));
    }
}


void TPartition::Handle(TEvKeyValue::TEvResponse::TPtr& ev, const TActorContext& ctx) {
    auto& response = ev->Get()->Record;

    //check correctness of response
    if (response.GetStatus() != NMsgBusProxy::MSTATUS_OK) {
        LOG_ERROR_S(
                ctx, NKikimrServices::PERSQUEUE,
                "OnWrite topic '" << TopicConverter->GetClientsideName() << "' partition " << Partition
                    << " commands are not processed at all, reason: " << response.DebugString()
        );
        ctx.Send(Tablet, new TEvents::TEvPoisonPill());
        //TODO: if status is DISK IS FULL, is global status MSTATUS_OK? it will be good if it is true
        return;
    }
    if (response.DeleteRangeResultSize()) {
        for (ui32 i = 0; i < response.DeleteRangeResultSize(); ++i) {
            if (response.GetDeleteRangeResult(i).GetStatus() != NKikimrProto::OK) {
                LOG_ERROR_S(
                        ctx, NKikimrServices::PERSQUEUE,
                        "OnWrite topic '" << TopicConverter->GetClientsideName() << "' partition " << Partition
                            << " delete range error"
                );
                //TODO: if disk is full, could this be ok? delete must be ok, of course
                ctx.Send(Tablet, new TEvents::TEvPoisonPill());
                return;
            }
        }
    }

    if (response.WriteResultSize()) {
        bool diskIsOk = true;
        for (ui32 i = 0; i < response.WriteResultSize(); ++i) {
            if (response.GetWriteResult(i).GetStatus() != NKikimrProto::OK) {
                LOG_ERROR_S(
                        ctx, NKikimrServices::PERSQUEUE,
                        "OnWrite  topic '" << TopicConverter->GetClientsideName() << "' partition " << Partition
                            << " write error"
                );
                ctx.Send(Tablet, new TEvents::TEvPoisonPill());
                return;
            }
            diskIsOk = diskIsOk && CheckDiskStatus(response.GetWriteResult(i).GetStatusFlags());
        }
        DiskIsFull = !diskIsOk;
    }
    bool diskIsOk = true;
    for (ui32 i = 0; i < response.GetStatusResultSize(); ++i) {
        auto& res = response.GetGetStatusResult(i);
        if (res.GetStatus() != NKikimrProto::OK) {
            LOG_ERROR_S(
                    ctx, NKikimrServices::PERSQUEUE,
                    "OnWrite  topic '" << TopicConverter->GetClientsideName() << "' partition " << Partition
                        << " are not processed at all, got KV error in CmdGetStatus " << res.GetStatus()
            );
            ctx.Send(Tablet, new TEvents::TEvPoisonPill());
            return;
        }
        diskIsOk = diskIsOk && CheckDiskStatus(res.GetStatusFlags());
    }
    if (response.GetStatusResultSize())
        DiskIsFull = !diskIsOk;

    if (response.HasCookie()) {
        HandleSetOffsetResponse(response.GetCookie(), ctx);
    } else {
        if (ctx.Now() - WriteStartTime > TDuration::MilliSeconds(AppData(ctx)->PQConfig.GetMinWriteLatencyMs())) {
            HandleWriteResponse(ctx);
        } else {
            ctx.Schedule(TDuration::MilliSeconds(AppData(ctx)->PQConfig.GetMinWriteLatencyMs()) - (ctx.Now() - WriteStartTime), new TEvPQ::TEvHandleWriteResponse());
        }
    }
}

void TPartition::Handle(TEvPQ::TEvHandleWriteResponse::TPtr&, const TActorContext& ctx) {
    HandleWriteResponse(ctx);
}

void TPartition::HandleSetOffsetResponse(ui64 cookie, const TActorContext& ctx) {
    Y_VERIFY(cookie == SET_OFFSET_COOKIE);


    if (ChangeConfig) {
        EndChangePartitionConfig(ChangeConfig->Config,
                                 ChangeConfig->TopicConverter,
                                 ctx);
    }

    for (auto& user : AffectedUsers) {
        if (auto* actual = GetPendingUserIfExists(user)) {
            TUserInfo& userInfo = UsersInfoStorage->GetOrCreate(user, ctx);
            bool offsetHasChanged = (userInfo.Offset != actual->Offset);

            userInfo.Session = actual->Session;
            userInfo.Generation = actual->Generation;
            userInfo.Step = actual->Step;
            userInfo.Offset = actual->Offset;
            userInfo.ReadRuleGeneration = actual->ReadRuleGeneration;
            userInfo.ReadFromTimestamp = actual->ReadFromTimestamp;
            userInfo.HasReadRule = true;

            if (userInfo.Important != actual->Important) {
                if (userInfo.LabeledCounters) {
                    ScheduleDropPartitionLabeledCounters(userInfo.LabeledCounters->GetGroup());
                }
                userInfo.SetImportant(actual->Important);
            }
            if (userInfo.Important && userInfo.Offset < (i64)StartOffset) {
                userInfo.Offset = StartOffset;
            }

            if (offsetHasChanged && !userInfo.UpdateTimestampFromCache()) {
                userInfo.ActualTimestamps = false;
                ReadTimestampForOffset(user, userInfo, ctx);
            } else {
                TabletCounters.Cumulative()[COUNTER_PQ_WRITE_TIMESTAMP_CACHE_HIT].Increment(1);
            }
        } else {
            auto ui = UsersInfoStorage->GetIfExists(user);
            if (ui && ui->LabeledCounters) {
                ScheduleDropPartitionLabeledCounters(ui->LabeledCounters->GetGroup());
            }

            UsersInfoStorage->Remove(user, ctx);
        }
    }

    for (auto& [actor, reply] : Replies) {
        ctx.Send(actor, reply.release());
    }

    PendingUsersInfo.clear();
    Replies.clear();
    AffectedUsers.clear();

    UsersInfoWriteInProgress = false;

    TxIdHasChanged = false;

    if (ChangeConfig) {
        ReportCounters(ctx);
        ChangeConfig = nullptr;
    }


    ProcessTxsAndUserActs(ctx);
}

void TPartition::PushBackDistrTx(TSimpleSharedPtr<TEvPQ::TEvTxCalcPredicate> event)
{
    DistrTxs.emplace_back(std::move(event));
}

void TPartition::PushBackDistrTx(TSimpleSharedPtr<TEvPQ::TEvChangePartitionConfig> event)
{
    DistrTxs.emplace_back(std::move(event), true);
}

void TPartition::PushFrontDistrTx(TSimpleSharedPtr<TEvPQ::TEvChangePartitionConfig> event)
{
    DistrTxs.emplace_front(std::move(event), false);
}

void TPartition::PushBackDistrTx(TSimpleSharedPtr<TEvPQ::TEvProposePartitionConfig> event)
{
    DistrTxs.emplace_back(std::move(event));
}

void TPartition::AddImmediateTx(TSimpleSharedPtr<TEvPersQueue::TEvProposeTransaction> tx)
{
    ImmediateTxs.push_back(std::move(tx));
}

void TPartition::AddUserAct(TSimpleSharedPtr<TEvPQ::TEvSetClientInfo> act)
{
    UserActs.push_back(std::move(act));
    ++UserActCount[UserActs.back()->ClientId];
}

void TPartition::RemoveImmediateTx()
{
    Y_VERIFY(!ImmediateTxs.empty());

    ImmediateTxs.pop_front();
}

void TPartition::RemoveUserAct()
{
    Y_VERIFY(!UserActs.empty());

    auto p = UserActCount.find(UserActs.front()->ClientId);
    Y_VERIFY(p != UserActCount.end());

    Y_VERIFY(p->second > 0);
    if (!--p->second) {
        UserActCount.erase(p);
    }

    UserActs.pop_front();
}

size_t TPartition::GetUserActCount(const TString& consumer) const
{
    if (auto i = UserActCount.find(consumer); i != UserActCount.end()) {
        return i->second;
    } else {
        return 0;
    }
}

void TPartition::ProcessTxsAndUserActs(const TActorContext& ctx)
{
    if (UsersInfoWriteInProgress || (ImmediateTxs.empty() && UserActs.empty() && DistrTxs.empty()) || TxInProgress) {
        return;
    }

    Y_VERIFY(PendingUsersInfo.empty());
    Y_VERIFY(Replies.empty());
    Y_VERIFY(AffectedUsers.empty());

    ContinueProcessTxsAndUserActs(ctx);
}

void TPartition::ContinueProcessTxsAndUserActs(const TActorContext& ctx)
{
    if (!DistrTxs.empty()) {
        ProcessDistrTxs(ctx);

        if (TxInProgress) {
            return;
        }
    }

    ProcessUserActs(ctx);
    ProcessImmediateTxs(ctx);

    THolder<TEvKeyValue::TEvRequest> request(new TEvKeyValue::TEvRequest);
    request->Record.SetCookie(SET_OFFSET_COOKIE);

    if (TxIdHasChanged) {
        AddCmdWriteTxMeta(request->Record,
                          *PlanStep, *TxId);
    }
    AddCmdWriteUserInfos(request->Record);
    AddCmdWriteConfig(request->Record);

    ctx.Send(Tablet, request.Release());
    UsersInfoWriteInProgress = true;
}

void TPartition::RemoveDistrTx()
{
    Y_VERIFY(!DistrTxs.empty());

    DistrTxs.pop_front();
}

void TPartition::ProcessDistrTxs(const TActorContext& ctx)
{
    Y_VERIFY(!TxInProgress);

    while (!TxInProgress && !DistrTxs.empty()) {
        ProcessDistrTx(ctx);
    }
}

bool TPartition::BeginTransaction(const TEvPQ::TEvTxCalcPredicate& tx,
                                  const TActorContext& ctx)
{
    Y_UNUSED(ctx);
    bool predicate = true;

    for (auto& operation : tx.Operations) {
        const TString& consumer = operation.GetConsumer();

        if (AffectedUsers.contains(consumer) && !GetPendingUserIfExists(consumer)) {
            predicate = false;
            break;
        }

        if (!UsersInfoStorage->GetIfExists(consumer)) {
            predicate = false;
            break;
        }

        bool isAffectedConsumer = AffectedUsers.contains(consumer);
        TUserInfoBase& userInfo = GetOrCreatePendingUser(consumer);

        if (operation.GetBegin() > operation.GetEnd()) {
            // BAD_REQUEST
            predicate = false;
        } else if (userInfo.Offset != (i64)operation.GetBegin()) {
            // ABORTED
            predicate = false;
        } else if (operation.GetEnd() > EndOffset) {
            // BAD_REQUEST
            predicate = false;
        }

        if (!predicate) {
            if (!isAffectedConsumer) {
                AffectedUsers.erase(consumer);
            }
            break;
        }
    }

    return predicate;
}

bool TPartition::BeginTransaction(const TEvPQ::TEvProposePartitionConfig& event)
{
    ChangeConfig =
        MakeSimpleShared<TEvPQ::TEvChangePartitionConfig>(TopicConverter,
                                                          event.Config);
    SendChangeConfigReply = false;
    return true;
}

void TPartition::EndTransaction(const TEvPQ::TEvTxCommit& event,
                                const TActorContext& ctx)
{
    if (PlanStep.Defined() && TxId.Defined()) {
        if (GetStepAndTxId(event) <= GetStepAndTxId(*PlanStep, *TxId)) {
            ctx.Send(Tablet, MakeCommitDone(event.Step, event.TxId).Release());
            return;
        }
    }

    Y_VERIFY(TxInProgress);

    Y_VERIFY(!DistrTxs.empty());
    TTransaction& t = DistrTxs.front();

    if (t.Tx) {
        Y_VERIFY(GetStepAndTxId(event) == GetStepAndTxId(*t.Tx));
        Y_VERIFY(t.Predicate.Defined() && *t.Predicate);

        for (auto& operation : t.Tx->Operations) {
            TUserInfoBase& userInfo = GetOrCreatePendingUser(operation.GetConsumer());

            Y_VERIFY(userInfo.Offset == (i64)operation.GetBegin());

            userInfo.Offset = operation.GetEnd();
            userInfo.Session = "";
        }

        ChangePlanStepAndTxId(t.Tx->Step, t.Tx->TxId);

        ScheduleReplyCommitDone(t.Tx->Step, t.Tx->TxId);
    } else if (t.ProposeConfig) {
        Y_VERIFY(GetStepAndTxId(event) == GetStepAndTxId(*t.ProposeConfig));
        Y_VERIFY(t.Predicate.Defined() && *t.Predicate);

        BeginChangePartitionConfig(t.ProposeConfig->Config, ctx);

        ChangePlanStepAndTxId(t.ProposeConfig->Step, t.ProposeConfig->TxId);

        ScheduleReplyCommitDone(t.ProposeConfig->Step, t.ProposeConfig->TxId);
    } else {
        Y_VERIFY(t.ChangeConfig);
    }

    RemoveDistrTx();
}

void TPartition::EndTransaction(const TEvPQ::TEvTxRollback& event,
                                const TActorContext& ctx)
{
    Y_UNUSED(ctx);

    if (PlanStep.Defined() && TxId.Defined()) {
        if (GetStepAndTxId(event) <= GetStepAndTxId(*PlanStep, *TxId)) {
            return;
        }
    }

    Y_VERIFY(TxInProgress);

    Y_VERIFY(!DistrTxs.empty());
    TTransaction& t = DistrTxs.front();

    if (t.Tx) {
        Y_VERIFY(GetStepAndTxId(event) == GetStepAndTxId(*t.Tx));
        Y_VERIFY(t.Predicate.Defined());

        ChangePlanStepAndTxId(t.Tx->Step, t.Tx->TxId);
    } else if (t.ProposeConfig) {
        Y_VERIFY(GetStepAndTxId(event) == GetStepAndTxId(*t.ProposeConfig));
        Y_VERIFY(t.Predicate.Defined());

        ChangePlanStepAndTxId(t.ProposeConfig->Step, t.ProposeConfig->TxId);
    } else {
        Y_VERIFY(t.ChangeConfig);
    }


    RemoveDistrTx();
}

void TPartition::BeginChangePartitionConfig(const NKikimrPQ::TPQTabletConfig& config,
                                            const TActorContext& ctx)
{
    TSet<TString> hasReadRule;

    for (auto& [consumer, info] : UsersInfoStorage->GetAll()) {
        hasReadRule.insert(consumer);
    }

    TSet<TString> important;
    for (const auto& importantUser : config.GetPartitionConfig().GetImportantClientId()) {
        important.insert(importantUser);
    }

    for (ui32 i = 0; i < config.ReadRulesSize(); ++i) {
        const auto& consumer = config.GetReadRules(i);
        auto& userInfo = GetOrCreatePendingUser(consumer, 0);

        TInstant ts = i < config.ReadFromTimestampsMsSize() ? TInstant::MilliSeconds(config.GetReadFromTimestampsMs(i)) : TInstant::Zero();
        if (!ts) {
            ts += TDuration::MilliSeconds(1);
        }
        userInfo.ReadFromTimestamp = ts;
        userInfo.Important = important.contains(consumer);

        ui64 rrGen = i < config.ReadRuleGenerationsSize() ? config.GetReadRuleGenerations(i) : 0;
        if (userInfo.ReadRuleGeneration != rrGen) {
            TEvPQ::TEvSetClientInfo act(0, consumer, 0, "", 0, 0,
                                        TEvPQ::TEvSetClientInfo::ESCI_INIT_READ_RULE, rrGen);

            ProcessUserAct(act, ctx);
        }
        hasReadRule.erase(consumer);
    }

    for (auto& consumer : hasReadRule) {
        GetOrCreatePendingUser(consumer);
        TEvPQ::TEvSetClientInfo act(0, consumer,
                                    0, "", 0, 0, TEvPQ::TEvSetClientInfo::ESCI_DROP_READ_RULE, 0);

        ProcessUserAct(act, ctx);
    }
}

void TPartition::EndChangePartitionConfig(const NKikimrPQ::TPQTabletConfig& config,
                                          NPersQueue::TTopicConverterPtr topicConverter,
                                          const TActorContext& ctx)
{
    Config = config;
    TopicConverter = topicConverter;

    Y_VERIFY(Config.GetPartitionConfig().GetTotalPartitions() > 0);

    UsersInfoStorage->UpdateConfig(Config);

    WriteQuota->UpdateConfig(Config.GetPartitionConfig().GetBurstSize(), Config.GetPartitionConfig().GetWriteSpeedInBytesPerSecond());
    if (AppData(ctx)->PQConfig.GetQuotingConfig().GetPartitionReadQuotaIsTwiceWriteQuota()) {
        for (auto& userInfo : UsersInfoStorage->GetAll()) {
            userInfo.second.ReadQuota.UpdateConfig(Config.GetPartitionConfig().GetBurstSize() * 2, Config.GetPartitionConfig().GetWriteSpeedInBytesPerSecond() * 2);
        }
    }

    for (const auto& readQuota : Config.GetPartitionConfig().GetReadQuota()) {
        auto& userInfo = UsersInfoStorage->GetOrCreate(readQuota.GetClientId(), ctx);
        userInfo.ReadQuota.UpdateConfig(readQuota.GetBurstSize(), readQuota.GetSpeedInBytesPerSecond());
    }

    if (Config.GetPartitionConfig().HasMirrorFrom()) {
        if (Mirrorer) {
            ctx.Send(Mirrorer->Actor, new TEvPQ::TEvChangePartitionConfig(TopicConverter,
                                                                          Config));
        } else {
            CreateMirrorerActor();
        }
    } else {
        if (Mirrorer) {
            ctx.Send(Mirrorer->Actor, new TEvents::TEvPoisonPill());
            Mirrorer.Reset();
        }
    }

    if (SendChangeConfigReply) {
        SchedulePartitionConfigChanged();
    }
}

TString TPartition::GetKeyConfig() const
{
    return Sprintf("_config_%u", Partition);
}

void TPartition::ChangePlanStepAndTxId(ui64 step, ui64 txId)
{
    PlanStep = step;
    TxId = txId;
    TxIdHasChanged = true;
}

void TPartition::ResendPendingEvents(const TActorContext& ctx)
{
    while (!PendingEvents.empty()) {
        ctx.Schedule(TDuration::Zero(), PendingEvents.front().release());
        PendingEvents.pop_front();
    }
}

void TPartition::ProcessDistrTx(const TActorContext& ctx)
{
    Y_VERIFY(!TxInProgress);

    Y_VERIFY(!DistrTxs.empty());
    TTransaction& t = DistrTxs.front();

    if (t.Tx) {
        t.Predicate = BeginTransaction(*t.Tx, ctx);

        ctx.Send(Tablet,
                 MakeHolder<TEvPQ::TEvTxCalcPredicateResult>(t.Tx->Step,
                                                             t.Tx->TxId,
                                                             Partition,
                                                             *t.Predicate).Release());

        TxInProgress = true;
    } else if (t.ProposeConfig) {
        t.Predicate = BeginTransaction(*t.ProposeConfig);

        ctx.Send(Tablet,
                 MakeHolder<TEvPQ::TEvProposePartitionConfigResult>(t.ProposeConfig->Step,
                                                                    t.ProposeConfig->TxId,
                                                                    Partition).Release());

        TxInProgress = true;
    } else {
        Y_VERIFY(!ChangeConfig);

        ChangeConfig = t.ChangeConfig;
        SendChangeConfigReply = t.SendReply;
        BeginChangePartitionConfig(ChangeConfig->Config, ctx);

        RemoveDistrTx();
    }
}

void TPartition::ProcessImmediateTxs(const TActorContext& ctx)
{
    Y_VERIFY(!UsersInfoWriteInProgress);

    while (!ImmediateTxs.empty() && (AffectedUsers.size() < MAX_USERS)) {
        ProcessImmediateTx(ImmediateTxs.front()->Record, ctx);

        RemoveImmediateTx();
    }
}

void TPartition::ProcessImmediateTx(const NKikimrPQ::TEvProposeTransaction& tx,
                                    const TActorContext& ctx)
{
    Y_UNUSED(ctx);

    Y_VERIFY(tx.GetTxBodyCase() == NKikimrPQ::TEvProposeTransaction::kData);
    Y_VERIFY(tx.HasData());

    for (auto& operation : tx.GetData().GetOperations()) {
        Y_VERIFY(operation.HasBegin() && operation.HasEnd() && operation.HasConsumer());

        Y_VERIFY(operation.GetBegin() <= (ui64)Max<i64>(), "Unexpected begin offset: %" PRIu64, operation.GetBegin());
        Y_VERIFY(operation.GetEnd() <= (ui64)Max<i64>(), "Unexpected end offset: %" PRIu64, operation.GetEnd());

        const TString& user = operation.GetConsumer();

        if (!PendingUsersInfo.contains(user) && AffectedUsers.contains(user)) {
            ScheduleReplyPropose(tx,
                                 NKikimrPQ::TEvProposeTransactionResult::ABORTED);
            return;
        }

        TUserInfoBase& userInfo = GetOrCreatePendingUser(user);

        if (operation.GetBegin() > operation.GetEnd()) {
            ScheduleReplyPropose(tx,
                                 NKikimrPQ::TEvProposeTransactionResult::BAD_REQUEST);
            return;
        }

        if (userInfo.Offset != (i64)operation.GetBegin()) {
            ScheduleReplyPropose(tx,
                                 NKikimrPQ::TEvProposeTransactionResult::ABORTED);
            return;
        }

        if (operation.GetEnd() > EndOffset) {
            ScheduleReplyPropose(tx,
                                 NKikimrPQ::TEvProposeTransactionResult::BAD_REQUEST);
            return;
        }

        userInfo.Offset = operation.GetEnd();
        userInfo.Session = "";
    }

    ScheduleReplyPropose(tx,
                         NKikimrPQ::TEvProposeTransactionResult::COMPLETE);
}

void TPartition::ProcessUserActs(const TActorContext& ctx)
{
    Y_VERIFY(!UsersInfoWriteInProgress);

    while (!UserActs.empty() && (AffectedUsers.size() < MAX_USERS)) {
        ProcessUserAct(*UserActs.front(), ctx);

        RemoveUserAct();
    }
}

void TPartition::ProcessUserAct(TEvPQ::TEvSetClientInfo& act,
                                const TActorContext& ctx)
{
    Y_VERIFY(!UsersInfoWriteInProgress);

    const TString& user = act.ClientId;
    const bool strictCommitOffset = (act.Type == TEvPQ::TEvSetClientInfo::ESCI_OFFSET && act.Strict);

    if (!PendingUsersInfo.contains(user) && AffectedUsers.contains(user)) {
        switch (act.Type) {
        case TEvPQ::TEvSetClientInfo::ESCI_INIT_READ_RULE:
            break;
        case TEvPQ::TEvSetClientInfo::ESCI_DROP_READ_RULE:
            return;
        default:
            ScheduleReplyError(act.Cookie,
                               NPersQueue::NErrorCode::WRONG_COOKIE,
                               "request to deleted read rule");
            return;
        }
    }

    TUserInfoBase& userInfo = GetOrCreatePendingUser(user);

    if (act.Type == TEvPQ::TEvSetClientInfo::ESCI_DROP_READ_RULE) {
        LOG_DEBUG_S(
                ctx, NKikimrServices::PERSQUEUE,
                "Topic '" << TopicConverter->GetClientsideName() << "' partition " << Partition
                    << " user " << user << " drop request"
        );

        EmulatePostProcessUserAct(act, userInfo, ctx);

        return;
    }

    if (act.Type == TEvPQ::TEvSetClientInfo::ESCI_CREATE_SESSION && act.SessionId == userInfo.Session) { //this is retry of current request, answer ok
        auto *ui = UsersInfoStorage->GetIfExists(userInfo.User);
        auto ts = ui ? GetTime(*ui, userInfo.Offset) : std::make_pair<TInstant, TInstant>(TInstant::Zero(), TInstant::Zero());

        ScheduleReplyGetClientOffsetOk(act.Cookie,
                                       userInfo.Offset,
                                       ts.first, ts.second);

        return;
    }

    if (act.Type != TEvPQ::TEvSetClientInfo::ESCI_CREATE_SESSION && act.Type != TEvPQ::TEvSetClientInfo::ESCI_INIT_READ_RULE
            && !act.SessionId.empty() && userInfo.Session != act.SessionId //request to wrong session
            && (act.Type != TEvPQ::TEvSetClientInfo::ESCI_DROP_SESSION || !userInfo.Session.empty()) //but allow DropSession request when session is already dropped - for idempotence
            || (act.Type == TEvPQ::TEvSetClientInfo::ESCI_CREATE_SESSION && !userInfo.Session.empty()
                 && (act.Generation < userInfo.Generation || act.Generation == userInfo.Generation && act.Step <= userInfo.Step))) { //old generation request
        TabletCounters.Cumulative()[COUNTER_PQ_SET_CLIENT_OFFSET_ERROR].Increment(1);

        ScheduleReplyError(act.Cookie,
                           NPersQueue::NErrorCode::WRONG_COOKIE,
                           TStringBuilder() << "set offset in already dead session " << act.SessionId << " actual is " << userInfo.Session);

        return;
    }

    if (!act.SessionId.empty() && act.Type == TEvPQ::TEvSetClientInfo::ESCI_OFFSET && (i64)act.Offset <= userInfo.Offset) { //this is stale request, answer ok for it
        ScheduleReplyOk(act.Cookie);

        return;
    }

    if (strictCommitOffset && act.Offset < StartOffset) {
        // strict commit to past, reply error
        TabletCounters.Cumulative()[COUNTER_PQ_SET_CLIENT_OFFSET_ERROR].Increment(1);
        ScheduleReplyError(act.Cookie,
                           NPersQueue::NErrorCode::SET_OFFSET_ERROR_COMMIT_TO_PAST,
                           TStringBuilder() << "set offset " <<  act.Offset << " to past for consumer " << act.ClientId << " actual start offset is " << StartOffset);

        return;
    }

    //request in correct session - make it

    ui64 offset = (act.Type == TEvPQ::TEvSetClientInfo::ESCI_OFFSET ? act.Offset : userInfo.Offset);
    ui64 readRuleGeneration = userInfo.ReadRuleGeneration;

    if (act.Type == TEvPQ::TEvSetClientInfo::ESCI_INIT_READ_RULE) {
        readRuleGeneration = act.ReadRuleGeneration;
        offset = 0;
        LOG_DEBUG_S(
                ctx, NKikimrServices::PERSQUEUE,
                "Topic '" << TopicConverter->GetClientsideName() << "' partition " << Partition
                    << " user " << act.ClientId << " reinit request with generation " << readRuleGeneration
        );
    }

    Y_VERIFY(offset <= (ui64)Max<i64>(), "Offset is too big: %" PRIu64, offset);

    if (offset > EndOffset) {
        if (strictCommitOffset) {
            TabletCounters.Cumulative()[COUNTER_PQ_SET_CLIENT_OFFSET_ERROR].Increment(1);
            ScheduleReplyError(act.Cookie,
                            NPersQueue::NErrorCode::SET_OFFSET_ERROR_COMMIT_TO_FUTURE,
                            TStringBuilder() << "strict commit can't set offset " <<  act.Offset << " to future, consumer " << act.ClientId << ", actual end offset is " << EndOffset);

            return;
        }
        LOG_WARN_S(
                ctx, NKikimrServices::PERSQUEUE,
                "commit to future - topic " << TopicConverter->GetClientsideName() << " partition " << Partition
                    << " client " << act.ClientId << " EndOffset " << EndOffset << " offset " << offset
        );
        act.Offset = EndOffset;
/*
        TODO:
        TabletCounters.Cumulative()[COUNTER_PQ_SET_CLIENT_OFFSET_ERROR].Increment(1);
        ReplyError(ctx, ev->Cookie, NPersQueue::NErrorCode::SET_OFFSET_ERROR_COMMIT_TO_FUTURE,
            TStringBuilder() << "can't commit to future. Offset " << offset << " EndOffset " << EndOffset);
        userInfo.UserActrs.pop_front();
        continue;
*/
    }

    EmulatePostProcessUserAct(act, userInfo, ctx);
}

void TPartition::EmulatePostProcessUserAct(const TEvPQ::TEvSetClientInfo& act,
                                           TUserInfoBase& userInfo,
                                           const TActorContext& ctx)
{
    const TString& user = act.ClientId;
    ui64 offset = act.Offset;
    const TString& session = act.SessionId;
    ui32 generation = act.Generation;
    ui32 step = act.Step;
    const ui64 readRuleGeneration = act.ReadRuleGeneration;

    bool setSession = act.Type == TEvPQ::TEvSetClientInfo::ESCI_CREATE_SESSION;
    bool dropSession = act.Type == TEvPQ::TEvSetClientInfo::ESCI_DROP_SESSION;
    bool strictCommitOffset = (act.Type == TEvPQ::TEvSetClientInfo::ESCI_OFFSET && act.SessionId.empty());

    if (act.Type == TEvPQ::TEvSetClientInfo::ESCI_DROP_READ_RULE) {
        userInfo.ReadRuleGeneration = 0;
        userInfo.Session = "";
        userInfo.Generation = userInfo.Step = 0;
        userInfo.Offset = 0;

        LOG_DEBUG_S(
                ctx, NKikimrServices::PERSQUEUE,
                "Topic '" << TopicConverter->GetClientsideName() << "' partition " << Partition << " user " << user
                    << " drop done"
        );
        PendingUsersInfo.erase(user);
    } else if (act.Type == TEvPQ::TEvSetClientInfo::ESCI_INIT_READ_RULE) {
        LOG_DEBUG_S(
                ctx, NKikimrServices::PERSQUEUE,
                "Topic '" << TopicConverter->GetClientsideName() << "' partition " << Partition << " user " << user
                    << " reinit with generation " << readRuleGeneration << " done"
        );

        userInfo.ReadRuleGeneration = readRuleGeneration;
        userInfo.Session = "";
        userInfo.Generation = userInfo.Step = 0;
        userInfo.Offset = 0;

        if (userInfo.Important) {
            userInfo.Offset = StartOffset;
        }
    } else {
        if (setSession || dropSession) {
            offset = userInfo.Offset;
            auto *ui = UsersInfoStorage->GetIfExists(userInfo.User);
            auto ts = ui ? GetTime(*ui, userInfo.Offset) : std::make_pair<TInstant, TInstant>(TInstant::Zero(), TInstant::Zero());

            ScheduleReplyGetClientOffsetOk(act.Cookie,
                                           offset,
                                           ts.first, ts.second);
        } else {
            ScheduleReplyOk(act.Cookie);
        }

        if (setSession) {
            userInfo.Session = session;
            userInfo.Generation = generation;
            userInfo.Step = step;
        } else if (dropSession || strictCommitOffset) {
            userInfo.Session = "";
            userInfo.Generation = 0;
            userInfo.Step = 0;
        }

        Y_VERIFY(offset <= (ui64)Max<i64>(), "Unexpected Offset: %" PRIu64, offset);
        LOG_DEBUG_S(
                ctx, NKikimrServices::PERSQUEUE,
                "Topic '" << TopicConverter->GetClientsideName() << "' partition " << Partition << " user " << user
                    << (setSession || dropSession ? " session" : " offset")
                    << " is set to " << offset << " (startOffset " << StartOffset << ") session " << session
        );

        userInfo.Offset = offset;

        auto counter = setSession ? COUNTER_PQ_CREATE_SESSION_OK : (dropSession ? COUNTER_PQ_DELETE_SESSION_OK : COUNTER_PQ_SET_CLIENT_OFFSET_OK);
        TabletCounters.Cumulative()[counter].Increment(1);
    }
}

void TPartition::ScheduleReplyOk(const ui64 dst)
{
    Replies.emplace_back(Tablet,
                         MakeReplyOk(dst).Release());
}

void TPartition::ScheduleReplyGetClientOffsetOk(const ui64 dst,
                                                const i64 offset,
                                                const TInstant writeTimestamp, const TInstant createTimestamp)
{
    Replies.emplace_back(Tablet,
                         MakeReplyGetClientOffsetOk(dst,
                                                    offset,
                                                    writeTimestamp, createTimestamp).Release());

}

void TPartition::ScheduleReplyError(const ui64 dst,
                                    NPersQueue::NErrorCode::EErrorCode errorCode,
                                    const TString& error)
{
    Replies.emplace_back(Tablet,
                         MakeReplyError(dst,
                                        errorCode,
                                        error).Release());
}

void TPartition::ScheduleReplyPropose(const NKikimrPQ::TEvProposeTransaction& event,
                                      NKikimrPQ::TEvProposeTransactionResult::EStatus statusCode)
{
    Replies.emplace_back(ActorIdFromProto(event.GetSource()),
                         MakeReplyPropose(event,
                                          statusCode).Release());
}

void TPartition::ScheduleReplyCommitDone(ui64 step, ui64 txId)
{
    Replies.emplace_back(Tablet,
                         MakeCommitDone(step, txId).Release());
}

void TPartition::ScheduleDropPartitionLabeledCounters(const TString& group)
{
    Replies.emplace_back(Tablet,
                         MakeHolder<TEvPQ::TEvPartitionLabeledCountersDrop>(Partition, group).Release());
}

void TPartition::SchedulePartitionConfigChanged()
{
    Replies.emplace_back(Tablet,
                         MakeHolder<TEvPQ::TEvPartitionConfigChanged>(Partition).Release());
}

void TPartition::AddCmdDeleteRange(NKikimrClient::TKeyValueRequest& request,
                                   const TKeyPrefix& ikey, const TKeyPrefix& ikeyDeprecated)
{
    auto del = request.AddCmdDeleteRange();
    auto range = del->MutableRange();
    range->SetFrom(ikey.Data(), ikey.Size());
    range->SetTo(ikey.Data(), ikey.Size());
    range->SetIncludeFrom(true);
    range->SetIncludeTo(true);

    del = request.AddCmdDeleteRange();
    range = del->MutableRange();
    range->SetFrom(ikeyDeprecated.Data(), ikeyDeprecated.Size());
    range->SetTo(ikeyDeprecated.Data(), ikeyDeprecated.Size());
    range->SetIncludeFrom(true);
    range->SetIncludeTo(true);
}

void TPartition::AddCmdWrite(NKikimrClient::TKeyValueRequest& request,
                             const TKeyPrefix& ikey, const TKeyPrefix& ikeyDeprecated,
                             ui64 offset, ui32 gen, ui32 step, const TString& session,
                             ui64 readOffsetRewindSum,
                             ui64 readRuleGeneration)
{
    TBuffer idata;
    {
        NKikimrPQ::TUserInfo userData;
        userData.SetOffset(offset);
        userData.SetGeneration(gen);
        userData.SetStep(step);
        userData.SetSession(session);
        userData.SetOffsetRewindSum(readOffsetRewindSum);
        userData.SetReadRuleGeneration(readRuleGeneration);

        TString out;
        Y_PROTOBUF_SUPPRESS_NODISCARD userData.SerializeToString(&out);

        idata.Append(out.c_str(), out.size());
    }

    auto write = request.AddCmdWrite();
    write->SetKey(ikey.Data(), ikey.Size());
    write->SetValue(idata.Data(), idata.Size());
    write->SetStorageChannel(NKikimrClient::TKeyValueRequest::INLINE);

    TBuffer idataDeprecated = NDeprecatedUserData::Serialize(offset, gen, step, session);

    write = request.AddCmdWrite();
    write->SetKey(ikeyDeprecated.Data(), ikeyDeprecated.Size());
    write->SetValue(idataDeprecated.Data(), idataDeprecated.Size());
    write->SetStorageChannel(NKikimrClient::TKeyValueRequest::INLINE);
}

void TPartition::AddCmdWriteTxMeta(NKikimrClient::TKeyValueRequest& request,
                                   ui64 step, ui64 txId)
{
    TKeyPrefix ikey(TKeyPrefix::TypeTxMeta, Partition);

    NKikimrPQ::TPartitionTxMeta meta;
    meta.SetPlanStep(step);
    meta.SetTxId(txId);

    TString out;
    Y_PROTOBUF_SUPPRESS_NODISCARD meta.SerializeToString(&out);

    auto write = request.AddCmdWrite();
    write->SetKey(ikey.Data(), ikey.Size());
    write->SetValue(out.c_str(), out.size());
    write->SetStorageChannel(NKikimrClient::TKeyValueRequest::INLINE);
}

void TPartition::AddCmdWriteUserInfos(NKikimrClient::TKeyValueRequest& request)
{
    for (auto& user : AffectedUsers) {
        TKeyPrefix ikey(TKeyPrefix::TypeInfo, Partition, TKeyPrefix::MarkUser);
        ikey.Append(user.c_str(), user.size());
        TKeyPrefix ikeyDeprecated(TKeyPrefix::TypeInfo, Partition, TKeyPrefix::MarkUserDeprecated);
        ikeyDeprecated.Append(user.c_str(), user.size());

        if (TUserInfoBase* userInfo = GetPendingUserIfExists(user)) {
            auto *ui = UsersInfoStorage->GetIfExists(user);
            AddCmdWrite(request,
                        ikey, ikeyDeprecated,
                        userInfo->Offset, userInfo->Generation, userInfo->Step, userInfo->Session,
                        ui ? ui->ReadOffsetRewindSum : 0,
                        userInfo->ReadRuleGeneration);
        } else {
            AddCmdDeleteRange(request,
                              ikey, ikeyDeprecated);
        }
    }
}

void TPartition::AddCmdWriteConfig(NKikimrClient::TKeyValueRequest& request)
{
    if (!ChangeConfig) {
        return;
    }

    TString key = GetKeyConfig();

    TString data;
    Y_VERIFY(ChangeConfig->Config.SerializeToString(&data));

    auto write = request.AddCmdWrite();
    write->SetKey(key.Data(), key.Size());
    write->SetValue(data.Data(), data.Size());
    write->SetStorageChannel(NKikimrClient::TKeyValueRequest::INLINE);
}

TUserInfoBase& TPartition::GetOrCreatePendingUser(const TString& user,
                                                  TMaybe<ui64> readRuleGeneration)
{
    TUserInfoBase* userInfo = nullptr;

    auto i = PendingUsersInfo.find(user);
    if (i == PendingUsersInfo.end()) {
        auto ui = UsersInfoStorage->GetIfExists(user);
        auto [p, _] = PendingUsersInfo.emplace(user, UsersInfoStorage->CreateUserInfo(user,
                                                                                      readRuleGeneration));

        if (ui) {
            p->second.Session = ui->Session;
            p->second.Generation = ui->Generation;
            p->second.Step = ui->Step;
            p->second.Offset = ui->Offset;
            p->second.ReadRuleGeneration = ui->ReadRuleGeneration;
            p->second.Important = ui->Important;
            p->second.ReadFromTimestamp = ui->ReadFromTimestamp;
        }

        userInfo = &p->second;
    } else {
        userInfo = &i->second;
    }

    AffectedUsers.insert(user);

    return *userInfo;
}

TUserInfoBase* TPartition::GetPendingUserIfExists(const TString& user)
{
    if (auto i = PendingUsersInfo.find(user); i != PendingUsersInfo.end()) {
        return &i->second;
    }

    return nullptr;
}

THolder<TEvPQ::TEvProxyResponse> TPartition::MakeReplyOk(const ui64 dst)
{
    auto response = MakeHolder<TEvPQ::TEvProxyResponse>(dst);
    NKikimrClient::TResponse& resp = response->Response;

    resp.SetStatus(NMsgBusProxy::MSTATUS_OK);
    resp.SetErrorCode(NPersQueue::NErrorCode::OK);

    return response;
}

THolder<TEvPQ::TEvProxyResponse> TPartition::MakeReplyGetClientOffsetOk(const ui64 dst,
                                                                        const i64 offset,
                                                                        const TInstant writeTimestamp, const TInstant createTimestamp)
{
    auto response = MakeHolder<TEvPQ::TEvProxyResponse>(dst);
    NKikimrClient::TResponse& resp = response->Response;

    resp.SetStatus(NMsgBusProxy::MSTATUS_OK);
    resp.SetErrorCode(NPersQueue::NErrorCode::OK);

    auto user = resp.MutablePartitionResponse()->MutableCmdGetClientOffsetResult();
    if (offset > -1)
        user->SetOffset(offset);
    if (writeTimestamp)
        user->SetWriteTimestampMS(writeTimestamp.MilliSeconds());
    if (createTimestamp) {
        Y_VERIFY(writeTimestamp);
        user->SetCreateTimestampMS(createTimestamp.MilliSeconds());
    }
    user->SetEndOffset(EndOffset);
    user->SetSizeLag(GetSizeLag(offset));
    user->SetWriteTimestampEstimateMS(WriteTimestampEstimate.MilliSeconds());

    return response;
}

THolder<TEvPQ::TEvError> TPartition::MakeReplyError(const ui64 dst,
                                                    NPersQueue::NErrorCode::EErrorCode errorCode,
                                                    const TString& error)
{
    //
    // FIXME(abcdef): в ReplyPersQueueError есть дополнительные действия
    //
    return MakeHolder<TEvPQ::TEvError>(errorCode, error, dst);
}

THolder<TEvPersQueue::TEvProposeTransactionResult> TPartition::MakeReplyPropose(const NKikimrPQ::TEvProposeTransaction& event,
                                                                                NKikimrPQ::TEvProposeTransactionResult::EStatus statusCode)
{
    auto response = MakeHolder<TEvPersQueue::TEvProposeTransactionResult>();

    response->Record.SetOrigin(TabletID);
    response->Record.SetStatus(statusCode);
    response->Record.SetTxId(event.GetTxId());

    return response;
}

THolder<TEvPQ::TEvTxCommitDone> TPartition::MakeCommitDone(ui64 step, ui64 txId)
{
    return MakeHolder<TEvPQ::TEvTxCommitDone>(step, txId, Partition);
}

void TPartition::ScheduleUpdateAvailableSize(const TActorContext& ctx) {
    ctx.Schedule(UPDATE_AVAIL_SIZE_INTERVAL, new TEvPQ::TEvUpdateAvailableSize());
}

void TPartition::HandleWriteResponse(const TActorContext& ctx) {

    Y_VERIFY(CurrentStateFunc() == &TThis::StateWrite);
    ui64 prevEndOffset = EndOffset;

    ui32 totalLatencyMs = (ctx.Now() - WriteCycleStartTime).MilliSeconds();
    ui32 writeLatencyMs = (ctx.Now() - WriteStartTime).MilliSeconds();

    WriteLatency.IncFor(writeLatencyMs, 1);
    if (writeLatencyMs >= AppData(ctx)->PQConfig.GetWriteLatencyBigMs()) {
        SLIBigLatency.Inc();
    }

    TabletCounters.Percentile()[COUNTER_LATENCY_PQ_WRITE_CYCLE].IncrementFor(totalLatencyMs);
    TabletCounters.Cumulative()[COUNTER_PQ_WRITE_CYCLE_BYTES_TOTAL].Increment(WriteCycleSize);
    TabletCounters.Cumulative()[COUNTER_PQ_WRITE_BYTES_OK].Increment(WriteNewSize);
    TabletCounters.Percentile()[COUNTER_PQ_WRITE_CYCLE_BYTES].IncrementFor(WriteCycleSize);
    TabletCounters.Percentile()[COUNTER_PQ_WRITE_NEW_BYTES].IncrementFor(WriteNewSize);
    if (BytesWrittenGrpc)
        BytesWrittenGrpc.Inc(WriteNewSizeInternal);
    if (BytesWrittenTotal)
        BytesWrittenTotal.Inc(WriteNewSize);

    if (BytesWrittenUncompressed)
        BytesWrittenUncompressed.Inc(WriteNewSizeUncompressed);
    if (BytesWrittenComp)
        BytesWrittenComp.Inc(WriteCycleSize);
    if (MsgsWrittenGrpc)
        MsgsWrittenGrpc.Inc(WriteNewMessagesInternal);
    if (MsgsWrittenTotal)
        MsgsWrittenTotal.Inc(WriteNewMessages);

    //All ok
    auto now = ctx.Now();
    const auto& quotingConfig = AppData()->PQConfig.GetQuotingConfig();
    if (quotingConfig.GetTopicWriteQuotaEntityToLimit() == NKikimrPQ::TPQConfig::TQuotingConfig::USER_PAYLOAD_SIZE) {
        WriteQuota->Exaust(WriteNewSize, now);
    } else {
        WriteQuota->Exaust(WriteCycleSize, now);
    }
    for (auto& avg : AvgWriteBytes) {
        avg.Update(WriteNewSize, now);
    }
    for (auto& avg : AvgQuotaBytes) {
        avg.Update(WriteNewSize, now);
    }

    WriteCycleSize = 0;
    WriteNewSize = 0;
    WriteNewSizeInternal = 0;
    WriteNewSizeUncompressed = 0;
    WriteNewMessages = 0;
    WriteNewMessagesInternal = 0;
    UpdateWriteBufferIsFullState(now);

    AnswerCurrentWrites(ctx);
    SyncMemoryStateWithKVState(ctx);

    //if EndOffset changed there could be subscriptions witch could be completed
    TVector<std::pair<TReadInfo, ui64>> reads = Subscriber.GetReads(EndOffset);
    for (auto& read : reads) {
        Y_VERIFY(EndOffset > read.first.Offset);
        ProcessRead(ctx, std::move(read.first), read.second, true);
    }
    //same for read requests
    ProcessHasDataRequests(ctx);

    ProcessTimestampsForNewData(prevEndOffset, ctx);

    HandleWrites(ctx);
}

void TPartition::HandleOnWrite(TEvPQ::TEvWrite::TPtr& ev, const TActorContext& ctx) {
    ui32 sz = std::accumulate(ev->Get()->Msgs.begin(), ev->Get()->Msgs.end(), 0u, [](ui32 sum, const TEvPQ::TEvWrite::TMsg& msg){
                            return sum + msg.Data.size();
                        });

    bool mirroredPartition = Config.GetPartitionConfig().HasMirrorFrom();

    if (mirroredPartition && !ev->Get()->OwnerCookie.empty()) {
        ReplyError(ctx, ev->Get()->Cookie, NPersQueue::NErrorCode::BAD_REQUEST,
            TStringBuilder() << "Write to mirrored topic is forbiden ");
        return;
    }

    ui64 decReservedSize = 0;
    TStringBuf owner;

    if (!mirroredPartition && !ev->Get()->IsDirectWrite) {
        owner = TOwnerInfo::GetOwnerFromOwnerCookie(ev->Get()->OwnerCookie);
        auto it = Owners.find(owner);

        if (it == Owners.end() || it->second.NeedResetOwner) {
            ReplyError(ctx, ev->Get()->Cookie, NPersQueue::NErrorCode::WRONG_COOKIE,
                TStringBuilder() << "new GetOwnership request needed for owner " << owner);
            return;
        }

        if (it->second.SourceIdDeleted) {
            ReplyError(ctx, ev->Get()->Cookie, NPersQueue::NErrorCode::SOURCEID_DELETED,
                TStringBuilder() << "Yours maximum written sequence number for session was deleted, need to recreate session. "
                    << "Current count of sourceIds is " << SourceIdStorage.GetInMemorySourceIds().size() << " and limit is " << Config.GetPartitionConfig().GetSourceIdMaxCounts()
                    << ", current minimum sourceid timestamp(Ms) is " << SourceIdStorage.MinAvailableTimestamp(ctx.Now()).MilliSeconds()
                    << " and border timestamp(Ms) is " << ((ctx.Now() - TInstant::Seconds(Config.GetPartitionConfig().GetSourceIdLifetimeSeconds())).MilliSeconds()));
            return;
        }

        if (it->second.OwnerCookie != ev->Get()->OwnerCookie) {
            ReplyError(ctx, ev->Get()->Cookie, NPersQueue::NErrorCode::WRONG_COOKIE,
                        TStringBuilder() << "incorrect ownerCookie " << ev->Get()->OwnerCookie << ", must be " << it->second.OwnerCookie);
            return;
        }

        if (ev->Get()->MessageNo != it->second.NextMessageNo) {
            ReplyError(ctx, ev->Get()->Cookie, NPersQueue::NErrorCode::BAD_REQUEST,
                TStringBuilder() << "reorder in requests, waiting " << it->second.NextMessageNo << ", but got " << ev->Get()->MessageNo);
            DropOwner(it, ctx);
            return;
        }

        ++it->second.NextMessageNo;
        decReservedSize = it->second.DecReservedSize();
    }

    TMaybe<ui64> offset = ev->Get()->Offset;

    if (WriteInflightSize > Config.GetPartitionConfig().GetMaxWriteInflightSize()) {
        TabletCounters.Cumulative()[COUNTER_PQ_WRITE_ERROR].Increment(ev->Get()->Msgs.size());
        TabletCounters.Cumulative()[COUNTER_PQ_WRITE_BYTES_ERROR].Increment(sz);

        ReplyError(ctx, ev->Get()->Cookie, NPersQueue::NErrorCode::OVERLOAD,
            TStringBuilder() << "try later. Write inflight limit reached. "
                << WriteInflightSize << " vs. maximum " <<  Config.GetPartitionConfig().GetMaxWriteInflightSize());
        return;
    }
    for (const auto& msg: ev->Get()->Msgs) {
        //this is checked in pq_impl when forming EvWrite request
        Y_VERIFY(!msg.SourceId.empty() || ev->Get()->IsDirectWrite);
        Y_VERIFY(!msg.Data.empty());

        if (msg.SeqNo > (ui64)Max<i64>()) {
            LOG_ERROR_S(ctx, NKikimrServices::PERSQUEUE, "Request to write wrong SeqNo. Partition "
                << Partition << " sourceId '" << EscapeC(msg.SourceId) << "' seqno " << msg.SeqNo);

            ReplyError(ctx, ev->Get()->Cookie, NPersQueue::NErrorCode::BAD_REQUEST,
                TStringBuilder() << "wrong SeqNo " << msg.SeqNo);
            return;
        }

        ui32 sz = msg.Data.size() + msg.SourceId.size() + TClientBlob::OVERHEAD;

        if (sz > MAX_BLOB_PART_SIZE) {
            ReplyError(ctx, ev->Get()->Cookie, NPersQueue::NErrorCode::BAD_REQUEST,
                TStringBuilder() << "too big message " << sz << " vs. maximum " << MAX_BLOB_PART_SIZE);
            return;
        }

        if (!mirroredPartition) {
            SourceIdStorage.RegisterSourceIdOwner(msg.SourceId, owner);
        }
    }

    const ui64 maxSize = Config.GetPartitionConfig().GetMaxSizeInPartition();
    const ui64 maxCount = Config.GetPartitionConfig().GetMaxCountInPartition();
    if (EndOffset - StartOffset >= maxCount || Size() >= maxSize) {
        TabletCounters.Cumulative()[COUNTER_PQ_WRITE_ERROR].Increment(ev->Get()->Msgs.size());
        TabletCounters.Cumulative()[COUNTER_PQ_WRITE_BYTES_ERROR].Increment(sz);

        ReplyError(ctx, ev->Get()->Cookie, NPersQueue::NErrorCode::WRITE_ERROR_PARTITION_IS_FULL,
                   Sprintf("try later, partition is full - already have %" PRIu64" from %" PRIu64 " count, %" PRIu64 " from %" PRIu64 " size", EndOffset - StartOffset, maxCount, Size(), maxSize));
        return;
    }
    ui64 size = 0;
    for (auto& msg: ev->Get()->Msgs) {
        size += msg.Data.size();
        bool needToChangeOffset = msg.PartNo + 1 == msg.TotalParts;
        EmplaceRequest(TWriteMsg{ev->Get()->Cookie, offset, std::move(msg)}, ctx);
        if (offset && needToChangeOffset)
            ++*offset;
    }
    WriteInflightSize += size;

    ReservedSize -= decReservedSize;
    // TODO: remove decReservedSize == 0
    Y_VERIFY(size <= decReservedSize || decReservedSize == 0);
    TabletCounters.Simple()[COUNTER_PQ_TABLET_RESERVED_BYTES_SIZE].Set(ReservedSize);
    UpdateWriteBufferIsFullState(ctx.Now());
}

void TPartition::HandleOnIdle(TEvPQ::TEvRegisterMessageGroup::TPtr& ev, const TActorContext& ctx) {
    HandleOnWrite(ev, ctx);
    HandleWrites(ctx);
}

void TPartition::HandleOnWrite(TEvPQ::TEvRegisterMessageGroup::TPtr& ev, const TActorContext& ctx) {
    const auto& body = ev->Get()->Body;

    auto it = SourceIdStorage.GetInMemorySourceIds().find(body.SourceId);
    if (it != SourceIdStorage.GetInMemorySourceIds().end()) {
        if (!it->second.Explicit) {
            return ReplyError(ctx, ev->Get()->Cookie, NPersQueue::NErrorCode::BAD_REQUEST,
                "Trying to register implicitly registered SourceId");
        }

        switch (it->second.State) {
        case TSourceIdInfo::EState::Registered:
            return ReplyOk(ctx, ev->Get()->Cookie);
        case TSourceIdInfo::EState::PendingRegistration:
            if (!body.AfterSplit) {
                return ReplyError(ctx, ev->Get()->Cookie, NPersQueue::NErrorCode::BAD_REQUEST,
                    "AfterSplit must be set");
            }
            break;
        default:
            return ReplyError(ctx, ev->Get()->Cookie, NPersQueue::NErrorCode::ERROR,
                TStringBuilder() << "Unknown state: " << static_cast<ui32>(it->second.State));
        }
    } else if (body.AfterSplit) {
        return ReplyError(ctx, ev->Get()->Cookie, NPersQueue::NErrorCode::BAD_REQUEST,
            "SourceId not found, registration cannot be completed");
    }

    EmplaceRequest(TRegisterMessageGroupMsg(*ev->Get()), ctx);
}

void TPartition::HandleOnIdle(TEvPQ::TEvDeregisterMessageGroup::TPtr& ev, const TActorContext& ctx) {
    HandleOnWrite(ev, ctx);
    HandleWrites(ctx);
}

void TPartition::HandleOnWrite(TEvPQ::TEvDeregisterMessageGroup::TPtr& ev, const TActorContext& ctx) {
    const auto& body = ev->Get()->Body;

    auto it = SourceIdStorage.GetInMemorySourceIds().find(body.SourceId);
    if (it == SourceIdStorage.GetInMemorySourceIds().end()) {
        return ReplyError(ctx, ev->Get()->Cookie, NPersQueue::NErrorCode::SOURCEID_DELETED,
            "SourceId doesn't exist");
    }
    
    EmplaceRequest(TDeregisterMessageGroupMsg(*ev->Get()), ctx);
}

void TPartition::HandleOnIdle(TEvPQ::TEvSplitMessageGroup::TPtr& ev, const TActorContext& ctx) {
    HandleOnWrite(ev, ctx);
    HandleWrites(ctx);
}

void TPartition::HandleOnWrite(TEvPQ::TEvSplitMessageGroup::TPtr& ev, const TActorContext& ctx) {
    if (ev->Get()->Deregistrations.size() > 1) {
        return ReplyError(ctx, ev->Get()->Cookie, NPersQueue::NErrorCode::BAD_REQUEST,
            TStringBuilder() << "Currently, single deregistrations are supported");
    }

    TSplitMessageGroupMsg msg(ev->Get()->Cookie);

    for (auto& body : ev->Get()->Deregistrations) {
        auto it = SourceIdStorage.GetInMemorySourceIds().find(body.SourceId);
        if (it != SourceIdStorage.GetInMemorySourceIds().end()) {
            msg.Deregistrations.push_back(std::move(body));
        } else {
            return ReplyError(ctx, ev->Get()->Cookie, NPersQueue::NErrorCode::SOURCEID_DELETED,
                "SourceId doesn't exist");
        }
    }

    for (auto& body : ev->Get()->Registrations) {
        auto it = SourceIdStorage.GetInMemorySourceIds().find(body.SourceId);
        if (it == SourceIdStorage.GetInMemorySourceIds().end()) {
            msg.Registrations.push_back(std::move(body));
        } else {
            if (!it->second.Explicit) {
                return ReplyError(ctx, ev->Get()->Cookie, NPersQueue::NErrorCode::BAD_REQUEST,
                    "Trying to register implicitly registered SourceId");
            }
        }
    }

    EmplaceRequest(std::move(msg), ctx);
}

std::pair<TKey, ui32> TPartition::Compact(const TKey& key, const ui32 size, bool headCleared) {
    std::pair<TKey, ui32> res({key, size});
    ui32 x = headCleared ? 0 : Head.PackedSize;
    Y_VERIFY(std::accumulate(DataKeysHead.begin(), DataKeysHead.end(), 0u, [](ui32 sum, const TKeyLevel& level){return sum + level.Sum();}) == NewHead.PackedSize + x);
    for (auto it = DataKeysHead.rbegin(); it != DataKeysHead.rend(); ++it) {
        auto jt = it; ++jt;
        if (it->NeedCompaction()) {
            res = it->Compact();
            if (jt != DataKeysHead.rend()) {
                jt->AddKey(res.first, res.second);
            }
        } else {
            Y_VERIFY(jt == DataKeysHead.rend() || !jt->NeedCompaction()); //compact must start from last level, not internal
        }
        Y_VERIFY(!it->NeedCompaction());
    }
    Y_VERIFY(res.second >= size);
    Y_VERIFY(res.first.GetOffset() < key.GetOffset() || res.first.GetOffset() == key.GetOffset() && res.first.GetPartNo() <= key.GetPartNo());
    return res;
}


void TPartition::ProcessChangeOwnerRequests(const TActorContext& ctx) {
    while (!WaitToChangeOwner.empty()) {
        auto &ev = WaitToChangeOwner.front();
        if (OwnerPipes.find(ev->PipeClient) != OwnerPipes.end()) { //this is not request from dead pipe
            ProcessChangeOwnerRequest(ev.Release(), ctx);
        } else {
            ReplyError(ctx, ev->Cookie, NPersQueue::NErrorCode::ERROR, "Pipe for GetOwnershipRequest is already dead");
        }
        WaitToChangeOwner.pop_front();
    }
    if (CurrentStateFunc() == &TThis::StateIdle) {
        HandleWrites(ctx);
    }
}


void TPartition::BecomeIdle(const TActorContext&) {
    Become(&TThis::StateIdle);
}

void TPartition::ClearOldHead(const ui64 offset, const ui16 partNo, TEvKeyValue::TEvRequest* request) {
    for (auto it = HeadKeys.rbegin(); it != HeadKeys.rend(); ++it) {
        if (it->Key.GetOffset() > offset || it->Key.GetOffset() == offset && it->Key.GetPartNo() >= partNo) {
            auto del = request->Record.AddCmdDeleteRange();
            auto range = del->MutableRange();
            range->SetFrom(it->Key.Data(), it->Key.Size());
            range->SetIncludeFrom(true);
            range->SetTo(it->Key.Data(), it->Key.Size());
            range->SetIncludeTo(true);
        } else {
            break;
        }
    }
}


void TPartition::CancelAllWritesOnWrite(const TActorContext& ctx, TEvKeyValue::TEvRequest* request, const TString& errorStr, const TWriteMsg& p, TSourceIdWriter& sourceIdWriter, NPersQueue::NErrorCode::EErrorCode errorCode = NPersQueue::NErrorCode::BAD_REQUEST) {
    ReplyError(ctx, p.Cookie, errorCode, errorStr);
    TabletCounters.Cumulative()[COUNTER_PQ_WRITE_ERROR].Increment(1);
    TabletCounters.Cumulative()[COUNTER_PQ_WRITE_BYTES_ERROR].Increment(p.Msg.Data.size() + p.Msg.SourceId.size());
    FailBadClient(ctx);
    NewHead.Clear();
    NewHead.Offset = EndOffset;
    sourceIdWriter.Clear();
    request->Record.Clear();
    PartitionedBlob = TPartitionedBlob(Partition, 0, "", 0, 0, 0, Head, NewHead, true, false, MaxBlobSize);
    CompactedKeys.clear();
}


bool TPartition::AppendHeadWithNewWrites(TEvKeyValue::TEvRequest* request, const TActorContext& ctx,
                                         TSourceIdWriter& sourceIdWriter) {

    ui64 curOffset = PartitionedBlob.IsInited() ? PartitionedBlob.GetOffset() : EndOffset;

    WriteCycleSize = 0;
    WriteNewSize = 0;
    WriteNewSizeUncompressed = 0;
    WriteNewMessages = 0;
    UpdateWriteBufferIsFullState(ctx.Now());
    CurrentTimestamp = ctx.Now();

    NewHead.Offset = EndOffset;
    NewHead.PartNo = 0;
    NewHead.PackedSize = 0;

    Y_VERIFY(NewHead.Batches.empty());

    bool oldPartsCleared = false;
    bool headCleared = (Head.PackedSize == 0);


    //TODO: Process here not TClientBlobs, but also TBatches from LB(LB got them from pushclient too)
    //Process is following: if batch contains already written messages or only one client message part -> unpack it and process as several TClientBlobs
    //otherwise write this batch as is to head;

    while (!Requests.empty() && WriteCycleSize < MAX_WRITE_CYCLE_SIZE) { //head is not too big
        auto pp = Requests.front();
        Requests.pop_front();

        if (!pp.IsWrite()) {
            if (pp.IsRegisterMessageGroup()) {
                auto& body = pp.GetRegisterMessageGroup().Body;

                TMaybe<TPartitionKeyRange> keyRange;
                if (body.KeyRange) {
                    keyRange = TPartitionKeyRange::Parse(*body.KeyRange);
                }

                body.AssignedOffset = curOffset;
                sourceIdWriter.RegisterSourceId(body.SourceId, body.SeqNo, curOffset, CurrentTimestamp, std::move(keyRange));
            } else if (pp.IsDeregisterMessageGroup()) {
                sourceIdWriter.DeregisterSourceId(pp.GetDeregisterMessageGroup().Body.SourceId);
            } else if (pp.IsSplitMessageGroup()) {
                for (auto& body : pp.GetSplitMessageGroup().Deregistrations) {
                    sourceIdWriter.DeregisterSourceId(body.SourceId);
                }

                for (auto& body : pp.GetSplitMessageGroup().Registrations) {
                    TMaybe<TPartitionKeyRange> keyRange;
                    if (body.KeyRange) {
                        keyRange = TPartitionKeyRange::Parse(*body.KeyRange);
                    }

                    body.AssignedOffset = curOffset;
                    sourceIdWriter.RegisterSourceId(body.SourceId, body.SeqNo, curOffset, CurrentTimestamp, std::move(keyRange), true);
                }
            } else {
                Y_VERIFY(pp.IsOwnership());
            }

            EmplaceResponse(std::move(pp), ctx);
            continue;
        }

        Y_VERIFY(pp.IsWrite());
        auto& p = pp.GetWrite();

        WriteInflightSize -= p.Msg.Data.size();

        TabletCounters.Percentile()[COUNTER_LATENCY_PQ_RECEIVE_QUEUE].IncrementFor(ctx.Now().MilliSeconds() - p.Msg.ReceiveTimestamp);
        //check already written

        ui64 poffset = p.Offset ? *p.Offset : curOffset;

        auto it_inMemory = SourceIdStorage.GetInMemorySourceIds().find(p.Msg.SourceId);
        auto it_toWrite = sourceIdWriter.GetSourceIdsToWrite().find(p.Msg.SourceId);
        if (!p.Msg.DisableDeduplication && (it_inMemory != SourceIdStorage.GetInMemorySourceIds().end() && it_inMemory->second.SeqNo >= p.Msg.SeqNo || (it_toWrite != sourceIdWriter.GetSourceIdsToWrite().end() && it_toWrite->second.SeqNo >= p.Msg.SeqNo))) {
            bool isWriting = (it_toWrite != sourceIdWriter.GetSourceIdsToWrite().end());
            bool isCommitted = (it_inMemory != SourceIdStorage.GetInMemorySourceIds().end());

            if (poffset >= curOffset) {
                LOG_DEBUG_S(
                        ctx, NKikimrServices::PERSQUEUE,
                        "Already written message. Topic: '" << TopicConverter->GetClientsideName()
                            << "' Partition: " << Partition << " SourceId: '" << EscapeC(p.Msg.SourceId)
                            << "'. Message seqNo = " << p.Msg.SeqNo
                            << ". Committed seqNo = " << (isCommitted ? it_inMemory->second.SeqNo : 0)
                            << (isWriting ? ". Writing seqNo: " : ". ") << (isWriting ? it_toWrite->second.SeqNo : 0)
                            << " EndOffset " << EndOffset << " CurOffset " << curOffset << " offset " << poffset
                );

                TabletCounters.Cumulative()[COUNTER_PQ_WRITE_ALREADY].Increment(1);
                TabletCounters.Cumulative()[COUNTER_PQ_WRITE_BYTES_ALREADY].Increment(p.Msg.Data.size());
            } else {
                TabletCounters.Cumulative()[COUNTER_PQ_WRITE_SMALL_OFFSET].Increment(1);
                TabletCounters.Cumulative()[COUNTER_PQ_WRITE_BYTES_SMALL_OFFSET].Increment(p.Msg.Data.size());
            }

            TString().swap(p.Msg.Data);
            EmplaceResponse(std::move(pp), ctx);
            continue;
        }

        if (poffset < curOffset) { //too small offset
            CancelAllWritesOnWrite(ctx, request,
                                    TStringBuilder() << "write message sourceId: " << EscapeC(p.Msg.SourceId) << " seqNo: " << p.Msg.SeqNo
                                        << " partNo: " << p.Msg.PartNo << " has incorrect offset " << poffset << ", must be at least " << curOffset,
                                        p, sourceIdWriter, NPersQueue::NErrorCode::EErrorCode::WRITE_ERROR_BAD_OFFSET);
            return false;
        }

        Y_VERIFY(poffset >= curOffset);

        bool needCompactHead = poffset > curOffset;
        if (needCompactHead) { //got gap
            if (p.Msg.PartNo != 0) { //gap can't be inside of partitioned message
                CancelAllWritesOnWrite(ctx, request,
                                        TStringBuilder() << "write message sourceId: " << EscapeC(p.Msg.SourceId) << " seqNo: " << p.Msg.SeqNo
                                            << " partNo: " << p.Msg.PartNo << " has gap inside partitioned message, incorrect offset "
                                            << poffset << ", must be " << curOffset,
                                            p, sourceIdWriter);
                return false;
            }
            curOffset = poffset;
        }

        if (p.Msg.PartNo == 0) { //create new PartitionedBlob
            //there could be parts from previous owner, clear them
            if (!oldPartsCleared) {
                oldPartsCleared = true;
                auto del = request->Record.AddCmdDeleteRange();
                auto range = del->MutableRange();
                TKeyPrefix from(TKeyPrefix::TypeTmpData, Partition);
                range->SetFrom(from.Data(), from.Size());
                TKeyPrefix to(TKeyPrefix::TypeTmpData, Partition + 1);
                range->SetTo(to.Data(), to.Size());
            }

            if (PartitionedBlob.HasFormedBlobs()) {
                //clear currently-writed blobs
                auto oldCmdWrite = request->Record.GetCmdWrite();
                request->Record.ClearCmdWrite();
                for (ui32 i = 0; i < (ui32)oldCmdWrite.size(); ++i) {
                    TKey key(oldCmdWrite.Get(i).GetKey());
                    if (key.GetType() != TKeyPrefix::TypeTmpData) {
                        request->Record.AddCmdWrite()->CopyFrom(oldCmdWrite.Get(i));
                    }
                }
            }
            PartitionedBlob = TPartitionedBlob(Partition, curOffset, p.Msg.SourceId, p.Msg.SeqNo,
                                               p.Msg.TotalParts, p.Msg.TotalSize, Head, NewHead,
                                               headCleared, needCompactHead, MaxBlobSize);
        }

        LOG_DEBUG_S(
                ctx, NKikimrServices::PERSQUEUE,
                "Topic '" << TopicConverter->GetClientsideName() << "' partition " << Partition
                    << " part blob processing sourceId '" << EscapeC(p.Msg.SourceId) <<
                    "' seqNo " << p.Msg.SeqNo << " partNo " << p.Msg.PartNo
        );
        TString s;
        if (!PartitionedBlob.IsNextPart(p.Msg.SourceId, p.Msg.SeqNo, p.Msg.PartNo, &s)) {
            //this must not be happen - client sends gaps, fail this client till the end
            CancelAllWritesOnWrite(ctx, request, s, p, sourceIdWriter);
            //now no changes will leak
            return false;
        }

        WriteNewSize += p.Msg.SourceId.size() + p.Msg.Data.size();
        WriteNewSizeInternal += p.Msg.External ? 0 : (p.Msg.SourceId.size() + p.Msg.Data.size());
        WriteNewSizeUncompressed += p.Msg.UncompressedSize + p.Msg.SourceId.size();
        if (p.Msg.PartNo == 0) {
             ++WriteNewMessages;
             if (!p.Msg.External)
                 ++WriteNewMessagesInternal;
        }

        TMaybe<TPartData> partData;
        if (p.Msg.TotalParts > 1) { //this is multi-part message
            partData = TPartData(p.Msg.PartNo, p.Msg.TotalParts, p.Msg.TotalSize);
        }
        WriteTimestamp = ctx.Now();
        WriteTimestampEstimate = p.Msg.WriteTimestamp > 0 ? TInstant::MilliSeconds(p.Msg.WriteTimestamp) : WriteTimestamp;
        TClientBlob blob(p.Msg.SourceId, p.Msg.SeqNo, p.Msg.Data, std::move(partData), WriteTimestampEstimate,
                            TInstant::MilliSeconds(p.Msg.CreateTimestamp == 0 ? curOffset : p.Msg.CreateTimestamp),
                            p.Msg.UncompressedSize, p.Msg.PartitionKey, p.Msg.ExplicitHashKey); //remove curOffset when LB will report CTime

        const ui64 writeLagMs =
            (WriteTimestamp - TInstant::MilliSeconds(p.Msg.CreateTimestamp)).MilliSeconds();
        WriteLagMs.Update(writeLagMs, WriteTimestamp);
        if (InputTimeLag) {
            InputTimeLag->IncFor(writeLagMs, 1);
            if (p.Msg.PartNo == 0) {
                MessageSize->IncFor(p.Msg.TotalSize + p.Msg.SourceId.size(), 1);
            }
        }

        bool lastBlobPart = blob.IsLastPart();

        //will return compacted tmp blob
        std::pair<TKey, TString> newWrite = PartitionedBlob.Add(std::move(blob));

        if (!newWrite.second.empty()) {
            auto write = request->Record.AddCmdWrite();
            write->SetKey(newWrite.first.Data(), newWrite.first.Size());
            write->SetValue(newWrite.second);
            Y_VERIFY(!newWrite.first.IsHead());
            auto channel = GetChannel(NextChannel(newWrite.first.IsHead(), newWrite.second.Size()));
            write->SetStorageChannel(channel);
            write->SetTactic(AppData(ctx)->PQConfig.GetTactic());

            TKey resKey = newWrite.first;
            resKey.SetType(TKeyPrefix::TypeData);
            write->SetKeyToCache(resKey.Data(), resKey.Size());
            WriteCycleSize += newWrite.second.size();

            LOG_DEBUG_S(
                    ctx, NKikimrServices::PERSQUEUE,
                    "Topic '" << TopicConverter->GetClientsideName() <<
                        "' partition " << Partition <<
                        " part blob sourceId '" << EscapeC(p.Msg.SourceId) <<
                        "' seqNo " << p.Msg.SeqNo << " partNo " << p.Msg.PartNo <<
                        " result is " << TStringBuf(newWrite.first.Data(), newWrite.first.Size()) <<
                        " size " << newWrite.second.size()
            );
        }

        if (lastBlobPart) {
            Y_VERIFY(PartitionedBlob.IsComplete());
            ui32 curWrites = 0;
            for (ui32 i = 0; i < request->Record.CmdWriteSize(); ++i) { //change keys for yet to be writed KV pairs
                TKey key(request->Record.GetCmdWrite(i).GetKey());
                if (key.GetType() == TKeyPrefix::TypeTmpData) {
                    key.SetType(TKeyPrefix::TypeData);
                    request->Record.MutableCmdWrite(i)->SetKey(TString(key.Data(), key.Size()));
                    ++curWrites;
                }
            }
            Y_VERIFY(curWrites <= PartitionedBlob.GetFormedBlobs().size());
            auto formedBlobs = PartitionedBlob.GetFormedBlobs();
            for (ui32 i = 0; i < formedBlobs.size(); ++i) {
                const auto& x = formedBlobs[i];
                if (i + curWrites < formedBlobs.size()) { //this KV pair is already writed, rename needed
                    auto rename = request->Record.AddCmdRename();
                    TKey key = x.first;
                    rename->SetOldKey(TString(key.Data(), key.Size()));
                    key.SetType(TKeyPrefix::TypeData);
                    rename->SetNewKey(TString(key.Data(), key.Size()));
                }
                if (!DataKeysBody.empty() && CompactedKeys.empty()) {
                    Y_VERIFY(DataKeysBody.back().Key.GetOffset() + DataKeysBody.back().Key.GetCount() <= x.first.GetOffset(),
                        "LAST KEY %s, HeadOffset %lu, NEWKEY %s", DataKeysBody.back().Key.ToString().c_str(), Head.Offset, x.first.ToString().c_str());
                }
                LOG_DEBUG_S(
                        ctx, NKikimrServices::PERSQUEUE,
                        "writing blob: topic '" << TopicConverter->GetClientsideName() << "' partition " << Partition
                            << " " << x.first.ToString() << " size " << x.second << " WTime " << ctx.Now().MilliSeconds()
                );

                CompactedKeys.push_back(x);
                CompactedKeys.back().first.SetType(TKeyPrefix::TypeData);
            }
            if (PartitionedBlob.HasFormedBlobs()) { //Head and newHead are cleared
                headCleared = true;
                NewHead.Clear();
                NewHead.Offset = PartitionedBlob.GetOffset();
                NewHead.PartNo = PartitionedBlob.GetHeadPartNo();
                NewHead.PackedSize = 0;
            }
            ui32 countOfLastParts = 0;
            for (auto& x : PartitionedBlob.GetClientBlobs()) {
                if (NewHead.Batches.empty() || NewHead.Batches.back().Packed) {
                    NewHead.Batches.emplace_back(curOffset, x.GetPartNo(), TVector<TClientBlob>());
                    NewHead.PackedSize += GetMaxHeaderSize(); //upper bound for packed size
                }
                if (x.IsLastPart()) {
                    ++countOfLastParts;
                }
                Y_VERIFY(!NewHead.Batches.back().Packed);
                NewHead.Batches.back().AddBlob(x);
                NewHead.PackedSize += x.GetBlobSize();
                if (NewHead.Batches.back().GetUnpackedSize() >= BATCH_UNPACK_SIZE_BORDER) {
                    NewHead.Batches.back().Pack();
                    NewHead.PackedSize += NewHead.Batches.back().GetPackedSize(); //add real packed size for this blob

                    NewHead.PackedSize -= GetMaxHeaderSize(); //instead of upper bound
                    NewHead.PackedSize -= NewHead.Batches.back().GetUnpackedSize();
                }
            }

            Y_VERIFY(countOfLastParts == 1);

            LOG_DEBUG_S(
                    ctx, NKikimrServices::PERSQUEUE,
                    "Topic '" << TopicConverter->GetClientsideName() << "' partition " << Partition
                        << " part blob complete sourceId '" << EscapeC(p.Msg.SourceId) << "' seqNo " << p.Msg.SeqNo
                        << " partNo " << p.Msg.PartNo << " FormedBlobsCount " << PartitionedBlob.GetFormedBlobs().size()
                        << " NewHead: " << NewHead
            );

            if (it_inMemory == SourceIdStorage.GetInMemorySourceIds().end()) {
                sourceIdWriter.RegisterSourceId(p.Msg.SourceId, p.Msg.SeqNo, curOffset, CurrentTimestamp);
            } else {
                sourceIdWriter.RegisterSourceId(p.Msg.SourceId, it_inMemory->second.Updated(p.Msg.SeqNo, curOffset, CurrentTimestamp));
            }

            ++curOffset;
            PartitionedBlob = TPartitionedBlob(Partition, 0, "", 0, 0, 0, Head, NewHead, true, false, MaxBlobSize);
        }
        TString().swap(p.Msg.Data);
        EmplaceResponse(std::move(pp), ctx);
    }

    UpdateWriteBufferIsFullState(ctx.Now());

    if (!NewHead.Batches.empty() && !NewHead.Batches.back().Packed) {
        NewHead.Batches.back().Pack();
        NewHead.PackedSize += NewHead.Batches.back().GetPackedSize(); //add real packed size for this blob

        NewHead.PackedSize -= GetMaxHeaderSize(); //instead of upper bound
        NewHead.PackedSize -= NewHead.Batches.back().GetUnpackedSize();
    }

    Y_VERIFY((headCleared ? 0 : Head.PackedSize) + NewHead.PackedSize <= MaxBlobSize); //otherwise last PartitionedBlob.Add must compact all except last cl
    MaxWriteResponsesSize = Max<ui32>(MaxWriteResponsesSize, Responses.size());

    return headCleared;
}


std::pair<TKey, ui32> TPartition::GetNewWriteKey(bool headCleared) {
    bool needCompaction = false;
    ui32 HeadSize = headCleared ? 0 : Head.PackedSize;
    if (HeadSize + NewHead.PackedSize > 0 && HeadSize + NewHead.PackedSize
                                                        >= Min<ui32>(MaxBlobSize, Config.GetPartitionConfig().GetLowWatermark()))
        needCompaction = true;

    if (PartitionedBlob.IsInited()) { //has active partitioned blob - compaction is forbiden, head and newHead will be compacted when this partitioned blob is finished
        needCompaction = false;
    }

    Y_VERIFY(NewHead.PackedSize > 0 || needCompaction); //smthing must be here

    TKey key(TKeyPrefix::TypeData, Partition, NewHead.Offset, NewHead.PartNo, NewHead.GetCount(), NewHead.GetInternalPartsCount(), !needCompaction);

    if (NewHead.PackedSize > 0)
        DataKeysHead[TotalLevels - 1].AddKey(key, NewHead.PackedSize);
    Y_VERIFY(HeadSize + NewHead.PackedSize <= 3 * MaxSizeCheck);

    std::pair<TKey, ui32> res;

    if (needCompaction) { //compact all
        for (ui32 i = 0; i < TotalLevels; ++i) {
            DataKeysHead[i].Clear();
        }
        if (!headCleared) { //compacted blob must contain both head and NewHead
            key = TKey(TKeyPrefix::TypeData, Partition, Head.Offset, Head.PartNo, NewHead.GetCount() + Head.GetCount(),
                        Head.GetInternalPartsCount() +  NewHead.GetInternalPartsCount(), false);
        } //otherwise KV blob is not from head (!key.IsHead()) and contains only new data from NewHead
        res = std::make_pair(key, HeadSize + NewHead.PackedSize);
    } else {
        res = Compact(key, NewHead.PackedSize, headCleared);
        Y_VERIFY(res.first.IsHead());//may compact some KV blobs from head, but new KV blob is from head too
        Y_VERIFY(res.second >= NewHead.PackedSize); //at least new data must be writed
    }
    Y_VERIFY(res.second <= MaxBlobSize);
    return res;
}

void TPartition::AddNewWriteBlob(std::pair<TKey, ui32>& res, TEvKeyValue::TEvRequest* request, bool headCleared, const TActorContext& ctx) {
    const auto& key = res.first;

    TString valueD;
    valueD.reserve(res.second);
    ui32 pp = Head.FindPos(key.GetOffset(), key.GetPartNo());
    if (pp < Max<ui32>() && key.GetOffset() < EndOffset) { //this batch trully contains this offset
        Y_VERIFY(pp < Head.Batches.size());
        Y_VERIFY(Head.Batches[pp].GetOffset() == key.GetOffset());
        Y_VERIFY(Head.Batches[pp].GetPartNo() == key.GetPartNo());
        for (; pp < Head.Batches.size(); ++pp) { //TODO - merge small batches here
            Y_VERIFY(Head.Batches[pp].Packed);
            valueD += Head.Batches[pp].Serialize();
        }
    }
    for (auto& b : NewHead.Batches) {
        Y_VERIFY(b.Packed);
        valueD += b.Serialize();
    }

    Y_VERIFY(res.second >= valueD.size());

    if (res.second > valueD.size() && res.first.IsHead()) { //change to real size if real packed size is smaller

        Y_FAIL("Can't be here right now, only after merging of small batches");

        for (auto it = DataKeysHead.rbegin(); it != DataKeysHead.rend(); ++it) {
            if (it->KeysCount() > 0 ) {
                auto res2 = it->PopBack();
                Y_VERIFY(res2 == res);
                res2.second = valueD.size();

                DataKeysHead[TotalLevels - 1].AddKey(res2.first, res2.second);

                res2 = Compact(res2.first, res2.second, headCleared);

                Y_VERIFY(res2.first == res.first);
                Y_VERIFY(res2.second == valueD.size());
                res = res2;
                break;
            }
        }
    }

    Y_VERIFY(res.second == valueD.size() || res.first.IsHead());

    CheckBlob(key, valueD);

    auto write = request->Record.AddCmdWrite();
    write->SetKey(key.Data(), key.Size());
    write->SetValue(valueD);

    if (!key.IsHead())
        write->SetKeyToCache(key.Data(), key.Size());

    bool isInline = key.IsHead() && valueD.size() < MAX_INLINE_SIZE;

    if (isInline)
        write->SetStorageChannel(NKikimrClient::TKeyValueRequest::INLINE);
    else {
        auto channel = GetChannel(NextChannel(key.IsHead(), valueD.size()));
        write->SetStorageChannel(channel);
        write->SetTactic(AppData(ctx)->PQConfig.GetTactic());
    }

    //Need to clear all compacted blobs
    TKey k = CompactedKeys.empty() ? key : CompactedKeys.front().first;
    ClearOldHead(k.GetOffset(), k.GetPartNo(), request);

    if (!key.IsHead()) {
        if (!DataKeysBody.empty() && CompactedKeys.empty()) {
            Y_VERIFY(DataKeysBody.back().Key.GetOffset() + DataKeysBody.back().Key.GetCount() <= key.GetOffset(),
                "LAST KEY %s, HeadOffset %lu, NEWKEY %s", DataKeysBody.back().Key.ToString().c_str(), Head.Offset, key.ToString().c_str());
        }
        CompactedKeys.push_back(res);
        NewHead.Clear();
        NewHead.Offset = res.first.GetOffset() + res.first.GetCount();
        NewHead.PartNo = 0;
    } else {
        Y_VERIFY(NewHeadKey.Size == 0);
        NewHeadKey = {key, res.second, CurrentTimestamp, 0};
    }
    WriteCycleSize += write->GetValue().size();
    UpdateWriteBufferIsFullState(ctx.Now());
}


ui32 TPartition::NextChannel(bool isHead, ui32 blobSize) {

    if (isHead) {
        ui32 i = 0;
        for (ui32 j = 1; j < TotalChannelWritesByHead.size(); ++j) {
            if (TotalChannelWritesByHead[j] < TotalChannelWritesByHead[i])
                i = j;
        }
        TotalChannelWritesByHead[i] += blobSize;

        return i;
    };

    ui32 res = Channel;
    Channel = (Channel + 1) % Config.GetPartitionConfig().GetNumChannels();

    return res;
}

void TPartition::SetDeadlinesForWrites(const TActorContext& ctx) {
    if (AppData(ctx)->PQConfig.GetQuotingConfig().GetQuotaWaitDurationMs() > 0 && QuotaDeadline == TInstant::Zero()) {

        QuotaDeadline = ctx.Now() + TDuration::MilliSeconds(AppData(ctx)->PQConfig.GetQuotingConfig().GetQuotaWaitDurationMs());

        ctx.Schedule(QuotaDeadline, new TEvPQ::TEvQuotaDeadlineCheck());
    }
}

void TPartition::Handle(TEvPQ::TEvQuotaDeadlineCheck::TPtr&, const TActorContext& ctx) {
    FilterDeadlinedWrites(ctx);
}

bool TPartition::ProcessWrites(TEvKeyValue::TEvRequest* request, TInstant now, const TActorContext& ctx) {

    FilterDeadlinedWrites(ctx);

    if (!WriteQuota->CanExaust(now)) { // Waiting for partition quota.
        SetDeadlinesForWrites(ctx);
        return false;
    }

    if (WaitingForPreviousBlobQuota() || WaitingForSubDomainQuota(ctx)) { // Waiting for topic quota.
        SetDeadlinesForWrites(ctx);

        if (StartTopicQuotaWaitTimeForCurrentBlob == TInstant::Zero() && !Requests.empty()) {
            StartTopicQuotaWaitTimeForCurrentBlob = now;
        }
        return false;
    }

    QuotaDeadline = TInstant::Zero();

    if (Requests.empty())
        return false;

    Y_VERIFY(request->Record.CmdWriteSize() == 0);
    Y_VERIFY(request->Record.CmdRenameSize() == 0);
    Y_VERIFY(request->Record.CmdDeleteRangeSize() == 0);
    const auto format = AppData(ctx)->PQConfig.GetEnableProtoSourceIdInfo()
        ? ESourceIdFormat::Proto
        : ESourceIdFormat::Raw;
    TSourceIdWriter sourceIdWriter(format);

    bool headCleared = AppendHeadWithNewWrites(request, ctx, sourceIdWriter);

    if (headCleared) {
        Y_VERIFY(!CompactedKeys.empty() || Head.PackedSize == 0);
        for (ui32 i = 0; i < TotalLevels; ++i) {
            DataKeysHead[i].Clear();
        }
    }

    if (NewHead.PackedSize == 0) { //nothing added to head - just compaction or tmp part blobs writed
        if (sourceIdWriter.GetSourceIdsToWrite().empty()) {
            return request->Record.CmdWriteSize() > 0
                || request->Record.CmdRenameSize() > 0
                || request->Record.CmdDeleteRangeSize() > 0;
        } else {
            sourceIdWriter.FillRequest(request, Partition);
            return true;
        }
    }

    sourceIdWriter.FillRequest(request, Partition);

    std::pair<TKey, ui32> res = GetNewWriteKey(headCleared);
    const auto& key = res.first;

    LOG_DEBUG_S(
            ctx, NKikimrServices::PERSQUEUE,
            "writing blob: topic '" << TopicConverter->GetClientsideName() << "' partition " << Partition
                << " compactOffset " << key.GetOffset() << "," << key.GetCount()
                << " HeadOffset " << Head.Offset << " endOffset " << EndOffset << " curOffset "
                << NewHead.GetNextOffset() << " " << key.ToString()
                << " size " << res.second << " WTime " << ctx.Now().MilliSeconds()
    );

    AddNewWriteBlob(res, request, headCleared, ctx);
    return true;
}

void TPartition::FilterDeadlinedWrites(const TActorContext& ctx) {
    if (QuotaDeadline == TInstant::Zero() || QuotaDeadline > ctx.Now())
        return;

    std::deque<TMessage> newRequests;
    for (auto& w : Requests) {
        if (!w.IsWrite() || w.GetWrite().Msg.IgnoreQuotaDeadline) {
            newRequests.emplace_back(std::move(w));
            continue;
        }
        if (w.IsWrite()) {
            const auto& msg = w.GetWrite().Msg;

            TabletCounters.Cumulative()[COUNTER_PQ_WRITE_ERROR].Increment(1);
            TabletCounters.Cumulative()[COUNTER_PQ_WRITE_BYTES_ERROR].Increment(msg.Data.size() + msg.SourceId.size());
            WriteInflightSize -= msg.Data.size();
        }

        ReplyError(ctx, w.GetCookie(), NPersQueue::NErrorCode::OVERLOAD, "quota exceeded");
    }
    Requests = std::move(newRequests);
    QuotaDeadline = TInstant::Zero();

    UpdateWriteBufferIsFullState(ctx.Now());
}


void TPartition::HandleWrites(const TActorContext& ctx) {
    Become(&TThis::StateWrite);

    THolder<TEvKeyValue::TEvRequest> request(new TEvKeyValue::TEvRequest);

    Y_VERIFY(Head.PackedSize + NewHead.PackedSize <= 2 * MaxSizeCheck);
    
    TInstant now = ctx.Now();
    WriteCycleStartTime = now;

    bool haveData = false;
    bool haveCheckDisk = false;

    if (!Requests.empty() && DiskIsFull) {
        CancelAllWritesOnIdle(ctx);
        AddCheckDiskRequest(request.Get(), Config.GetPartitionConfig().GetNumChannels());
        haveCheckDisk = true;
    } else {
        haveData = ProcessWrites(request.Get(), now, ctx);
    }
    bool haveDrop = CleanUp(request.Get(), haveData, ctx);

    ProcessReserveRequests(ctx);
    if (!haveData && !haveDrop && !haveCheckDisk) { //no data writed/deleted
        if (!Requests.empty()) { //there could be change ownership requests that
            bool res = ProcessWrites(request.Get(), now, ctx);
            Y_VERIFY(!res);
        }
        Y_VERIFY(Requests.empty() || !WriteQuota->CanExaust(now) || WaitingForPreviousBlobQuota() || WaitingForSubDomainQuota(ctx)); //in this case all writes must be processed or no quota left
        AnswerCurrentWrites(ctx); //in case if all writes are already done - no answer will be called on kv write, no kv write at all
        BecomeIdle(ctx);
        return;
    }

    WritesTotal.Inc();
    WriteBlobWithQuota(std::move(request));
}


void TPartition::ProcessRead(const TActorContext& ctx, TReadInfo&& info, const ui64 cookie, bool subscription) {
    ui32 count = 0;
    ui32 size = 0;

    Y_VERIFY(!info.User.empty());
    auto& userInfo = UsersInfoStorage->GetOrCreate(info.User, ctx);

    if (subscription) {
        userInfo.ForgetSubscription(ctx.Now());
    }

    if (!userInfo.ReadQuota.CanExaust(ctx.Now())) {
        userInfo.ReadRequests.push_back({std::move(info), cookie});
        userInfo.UpdateReadingTimeAndState(ctx.Now());
        return;
    }
    TVector<TRequestedBlob> blobs = GetReadRequestFromBody(info.Offset, info.PartNo, info.Count, info.Size, &count, &size);
    info.Blobs = blobs;
    ui64 lastOffset = info.Offset + Min(count, info.Count);
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "read cookie " << cookie << " added " << info.Blobs.size()
                << " blobs, size " << size << " count " << count << " last offset " << lastOffset);

    if (blobs.empty() || blobs.back().Key == DataKeysBody.back().Key) { // read from head only when all blobs from body processed
        ui64 insideHeadOffset{0};
        info.Cached = GetReadRequestFromHead(info.Offset, info.PartNo, info.Count, info.Size, info.ReadTimestampMs, &count, &size, &insideHeadOffset);
        info.CachedOffset = insideHeadOffset;
    }
    if (info.Destination != 0) {
        ++userInfo.ActiveReads;
        userInfo.UpdateReadingTimeAndState(ctx.Now());
    }

    if (info.Blobs.empty()) { //all from head, answer right now
        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "Reading cookie " << cookie << ". All data is from uncompacted head.");

        TReadAnswer answer(info.FormAnswer(
            ctx, EndOffset, Partition, &UsersInfoStorage->GetOrCreate(info.User, ctx),
            info.Destination, GetSizeLag(info.Offset), Tablet, Config.GetMeteringMode()
        ));
        const auto& resp = dynamic_cast<TEvPQ::TEvProxyResponse*>(answer.Event.Get())->Response;
        if (info.IsSubscription) {
            TabletCounters.Cumulative()[COUNTER_PQ_READ_SUBSCRIPTION_OK].Increment(1);
        }
        TabletCounters.Cumulative()[COUNTER_PQ_READ_HEAD_ONLY_OK].Increment(1);
        TabletCounters.Percentile()[COUNTER_LATENCY_PQ_READ_HEAD_ONLY].IncrementFor((ctx.Now() - info.Timestamp).MilliSeconds());
        TabletCounters.Cumulative()[COUNTER_PQ_READ_BYTES].Increment(resp.ByteSize());
        ctx.Send(info.Destination != 0 ? Tablet : ctx.SelfID, answer.Event.Release());
        OnReadRequestFinished(std::move(info), answer.Size);
        return;
    }

    const TString user = info.User;
    bool res = ReadInfo.insert({cookie, std::move(info)}).second;
    Y_VERIFY(res);

    THolder<TEvPQ::TEvBlobRequest> request(new TEvPQ::TEvBlobRequest(user, cookie, Partition,
                                                                     lastOffset, std::move(blobs)));

    ctx.Send(BlobCache, request.Release());
}

void TPartition::Handle(TEvQuota::TEvClearance::TPtr& ev, const TActorContext& ctx) {
    const ui64 cookie = ev->Cookie;
    LOG_DEBUG_S(
            ctx, NKikimrServices::PERSQUEUE,
            "Got quota." <<
            " Topic: \"" << TopicConverter->GetClientsideName() << "\"." <<
            " Partition: " << Partition << ": " << ev->Get()->Result << "." <<
            " Cookie: " << cookie
    );
    // Check
    if (Y_UNLIKELY(ev->Get()->Result != TEvQuota::TEvClearance::EResult::Success)) {
        // We set deadline == inf in quota request.
        Y_VERIFY(ev->Get()->Result != TEvQuota::TEvClearance::EResult::Deadline);
        LOG_ERROR_S(
                ctx, NKikimrServices::PERSQUEUE,
                "Got quota error." <<
                " Topic: \"" << TopicConverter->GetClientsideName() << "\"." <<
                " Partition " << Partition << ": " << ev->Get()->Result
        );
        ctx.Send(Tablet, new TEvents::TEvPoisonPill());
        return;
    }

    // Search for proper request
    Y_VERIFY(TopicQuotaRequestCookie == cookie);
    TopicQuotaRequestCookie = 0;
    Y_ASSERT(!WaitingForPreviousBlobQuota());

    // Metrics
    TopicQuotaWaitTimeForCurrentBlob = StartTopicQuotaWaitTimeForCurrentBlob ? TActivationContext::Now() - StartTopicQuotaWaitTimeForCurrentBlob : TDuration::Zero();
    if (TopicWriteQuotaWaitCounter) {
        TopicWriteQuotaWaitCounter->IncFor(TopicQuotaWaitTimeForCurrentBlob.MilliSeconds());
    }
    // Reset quota wait time
    StartTopicQuotaWaitTimeForCurrentBlob = TInstant::Zero();

    if (CurrentStateFunc() == &TThis::StateIdle)
        HandleWrites(ctx);
}

size_t TPartition::GetQuotaRequestSize(const TEvKeyValue::TEvRequest& request) {
    if (Config.GetMeteringMode() == NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS) {
        return 0;
    }
    if (AppData()->PQConfig.GetQuotingConfig().GetTopicWriteQuotaEntityToLimit() ==
        NKikimrPQ::TPQConfig::TQuotingConfig::USER_PAYLOAD_SIZE) {
        return WriteNewSize;
    } else {
        return std::accumulate(request.Record.GetCmdWrite().begin(), request.Record.GetCmdWrite().end(), 0ul,
                               [](size_t sum, const auto& el) { return sum + el.GetValue().size(); });
    }
}

void TPartition::RequestQuotaForWriteBlobRequest(size_t dataSize, ui64 cookie) {
    LOG_DEBUG_S(
            TActivationContext::AsActorContext(), NKikimrServices::PERSQUEUE,
            "Send write quota request." <<
            " Topic: \"" << TopicConverter->GetClientsideName() << "\"." <<
            " Partition: " << Partition << "." <<
            " Amount: " << dataSize << "." <<
            " Cookie: " << cookie
    );

    Send(MakeQuoterServiceID(),
        new TEvQuota::TEvRequest(
            TEvQuota::EResourceOperator::And,
            { TEvQuota::TResourceLeaf(TopicWriteQuoterPath, TopicWriteQuotaResourcePath, dataSize) },
            TDuration::Max()),
        0,
        cookie);
}

bool TPartition::WaitingForPreviousBlobQuota() const {
    return TopicQuotaRequestCookie != 0;
}

bool TPartition::WaitingForSubDomainQuota(const TActorContext& ctx, const ui64 withSize) const {
    return SubDomainOutOfSpace && AppData()->FeatureFlags.GetEnableTopicDiskSubDomainQuota() && MeteringDataSize(ctx) + withSize > ReserveSize();
}

void TPartition::WriteBlobWithQuota(THolder<TEvKeyValue::TEvRequest>&& request) {
    // Request quota and write blob.
    // Mirrored topics are not quoted in local dc.
    const bool skip = !IsQuotingEnabled() || TopicWriteQuotaResourcePath.empty();
    if (size_t quotaRequestSize = skip ? 0 : GetQuotaRequestSize(*request)) {
        // Request with data. We should check before attempting to write data whether we have enough quota.
        Y_VERIFY(!WaitingForPreviousBlobQuota());

        TopicQuotaRequestCookie = NextTopicWriteQuotaRequestCookie++;
        RequestQuotaForWriteBlobRequest(quotaRequestSize, TopicQuotaRequestCookie);
    }

    AddMetaKey(request.Get());

    WriteStartTime = TActivationContext::Now();
    // Write blob
#if 1
    // PQ -> CacheProxy -> KV
    Send(BlobCache, request.Release());
#else
    Send(Tablet, request.Release());
#endif
}

void TPartition::CreateMirrorerActor() {
    Mirrorer = MakeHolder<TMirrorerInfo>(
        Register(new TMirrorer(Tablet, SelfId(), TopicConverter, Partition, IsLocalDC,  EndOffset, Config.GetPartitionConfig().GetMirrorFrom(), TabletCounters)),
        TabletCounters
    );
}

bool TPartition::IsQuotingEnabled() const
{
    return NPQ::IsQuotingEnabled(AppData()->PQConfig,
                                 IsLocalDC);
}

void TPartition::Handle(TEvPQ::TEvSubDomainStatus::TPtr& ev, const TActorContext& ctx)
{
    const TEvPQ::TEvSubDomainStatus& event = *ev->Get();

    bool statusChanged = SubDomainOutOfSpace != event.SubDomainOutOfSpace();
    SubDomainOutOfSpace = event.SubDomainOutOfSpace();

    if (statusChanged) {
        LOG_INFO_S(
            ctx, NKikimrServices::PERSQUEUE,
            "SubDomainOutOfSpace was changed." <<
            " Topic: \"" << TopicConverter->GetClientsideName() << "\"." <<
            " Partition: " << Partition << "." <<
            " SubDomainOutOfSpace: " << SubDomainOutOfSpace 
        );

        if (!SubDomainOutOfSpace) {
            if (CurrentStateFunc() == &TThis::StateIdle) {
                HandleWrites(ctx);
            }
        }
    }
}


} // namespace NKikimr::NPQ
