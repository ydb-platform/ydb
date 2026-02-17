#include "autopartitioning_manager.h"
#include "message_id_deduplicator.h"
#include "offload_actor.h"
#include "partition.h"
#include "partition_compactification.h"
#include "partition_util.h"
#include <ydb/core/persqueue/common/percentiles.h>
#include <ydb/core/persqueue/pqtablet/common/logging.h>
#include <ydb/core/persqueue/pqtablet/common/constants.h>

#include <memory>

#define PQ_INIT_ENSURE(condition) AFL_ENSURE(condition)("tablet_id", Partition()->TabletId)("partition_id", Partition()->Partition)

namespace NKikimr::NPQ {

static const ui32 LEVEL0 = 32;
static const TString WRITE_QUOTA_ROOT_PATH = "write-quota";

bool DiskIsFull(TEvKeyValue::TEvResponse::TPtr& ev);
void RequestInfoRange(const TActorContext& ctx, const TActorId& dst, const TPartitionId& partition, const TString& key);
void RequestDataRange(const TActorContext& ctx, const TActorId& dst, const TPartitionId& partition, const TString& key);
void RequestDeduplicatorRange(const TActorContext& ctx, const TActorId& dst, const TPartitionId& partition, const TString& key);
void ValidateResponse(const TInitializerStep& step, TEvKeyValue::TEvResponse::TPtr& ev);

//
// TInitializer
//

TInitializer::TInitializer(TPartition* partition)
    : Partition(partition)
    , InProgress(false)
{
    Steps.push_back(MakeHolder<TInitConfigStep>(this));
    Steps.push_back(MakeHolder<TInitInternalFieldsStep>(this));
    Steps.push_back(MakeHolder<TInitDiskStatusStep>(this));
    Steps.push_back(MakeHolder<TInitMetaStep>(this));
    Steps.push_back(MakeHolder<TInitInfoRangeStep>(this));
    Steps.push_back(MakeHolder<TInitDataRangeStep>(this));
    Steps.push_back(MakeHolder<TInitDataStep>(this));
    Steps.push_back(MakeHolder<TInitEndWriteTimestampStep>(this));
    Steps.push_back(MakeHolder<TInitMessageDeduplicatorStep>(this));
    Steps.push_back(MakeHolder<TDeleteKeysStep>(this));
    Steps.push_back(MakeHolder<TInitFieldsStep>(this));

    CurrentStep = Steps.begin();
}

void TInitializer::Execute(const TActorContext& ctx) {
    AFL_ENSURE(!InProgress)("description", "Initialization already in progress");
    InProgress = true;
    DoNext(ctx);
}

bool TInitializer::Handle(STFUNC_SIG) {
    AFL_ENSURE(InProgress)("description", "Initialization is not started");
    return CurrentStep->Get()->Handle(ev);
}

void TInitializer::Next(const TActorContext& ctx) {
    ++CurrentStep;
    DoNext(ctx);
}

void TInitializer::Done(const TActorContext& ctx) {
    PQ_LOG_D("Initializing completed.");
    InProgress = false;
    Partition->InitComplete(ctx);
}

void TInitializer::DoNext(const TActorContext& ctx) {
    if (CurrentStep == Steps.end()) {
        Done(ctx);
        return;
    }

    if (Partition->NewPartition) {
        while(CurrentStep->Get()->SkipNewPartition) {
            if (++CurrentStep == Steps.end()) {
                Done(ctx);
                return;
            }
        }
    }

    PQ_LOG_D("Start initializing step " << CurrentStep->Get()->Name);
    CurrentStep->Get()->Execute(ctx);
}

TString TInitializer::LogPrefix() const {
    return TStringBuilder() << "[" << Partition->TopicName() << ":" << Partition->Partition << ":Initializer] ";
}


//
// TInitializerStep
//

TInitializerStep::TInitializerStep(TInitializer* initializer, TString name, bool skipNewPartition)
    : Name(name)
    , SkipNewPartition(skipNewPartition)
    , Initializer(initializer) {
}

void TInitializerStep::Done(const TActorContext& ctx) {
    Initializer->Next(ctx);
}

bool TInitializerStep::Handle(STFUNC_SIG) {
    Y_UNUSED(ev);

    return false;
}

TPartition* TInitializerStep::Partition() const {
    return Initializer->Partition;
}

const TPartitionId& TInitializerStep::PartitionId() const {
    return Initializer->Partition->Partition;
}

const TString& TInitializerStep::TopicName() const {
    return Partition()->TopicName();
}

TInitializionContext& TInitializerStep::GetContext() {
    return Initializer->Ctx;
}

TString TInitializerStep::LogPrefix() const {
    return TStringBuilder() << "[" << Partition()->TopicName() << ":" << Partition()->Partition << ":" << Name << "] ";
}



//
// TBaseKVStep
//

TBaseKVStep::TBaseKVStep(TInitializer* initializer, TString name, bool skipNewPartition)
    : TInitializerStep(initializer, name, skipNewPartition) {
}

bool TBaseKVStep::Handle(STFUNC_SIG) {
    switch(ev->GetTypeRewrite())
    {
        HFuncCtx(TEvKeyValue::TEvResponse, Handle, TActivationContext::AsActorContext());
        default:
            return false;
    }
    return true;
}


//
// TInitConfigStep
//

TInitConfigStep::TInitConfigStep(TInitializer* initializer)
    : TBaseKVStep(initializer, "TInitConfigStep", false) {
}

void TInitConfigStep::Execute(const TActorContext& ctx) {
    auto event = MakeHolder<TEvKeyValue::TEvRequest>();
    auto read = event->Record.AddCmdRead();
    read->SetKey(Partition()->GetKeyConfig());

    ctx.Send(Partition()->TabletActorId, event.Release());
}

void TInitConfigStep::Handle(TEvKeyValue::TEvResponse::TPtr& ev, const TActorContext& ctx) {
    ValidateResponse(*this, ev);

    auto& res = ev->Get()->Record;
    PQ_INIT_ENSURE(res.ReadResultSize() == 1);

    auto& response = res.GetReadResult(0);

    switch (response.GetStatus()) {
    case NKikimrProto::OK:
        PQ_INIT_ENSURE(Partition()->Config.ParseFromString(response.GetValue()));

        Migrate(Partition()->Config);

        if (Partition()->Config.GetVersion() < Partition()->TabletConfig.GetVersion()) {
            auto event = MakeHolder<TEvPQ::TEvChangePartitionConfig>(Partition()->TopicConverter,
                                                                     Partition()->TabletConfig);
            Partition()->PushFrontDistrTx(event.Release());
        }
        break;

    case NKikimrProto::NODATA:
        Partition()->Config = Partition()->TabletConfig;
        break;

    default:
        AFL_ENSURE(false)("status", response.GetStatus());
    };

    // There should be no consumers in the configuration of the background partition. When creating a partition,
    // the PQ tablet specifically removes all consumer settings from the config.
    PQ_INIT_ENSURE(!Partition()->IsSupportive() ||
                   (Partition()->Config.GetConsumers().empty() && Partition()->TabletConfig.GetConsumers().empty()));

    Partition()->PartitionConfig = GetPartitionConfig(Partition()->Config, Partition()->Partition.OriginalPartitionId);
    Partition()->PartitionGraph = MakePartitionGraph(Partition()->Config);

    Done(ctx);
}


//
// TInitInternalFieldsStep
//
TInitInternalFieldsStep::TInitInternalFieldsStep(TInitializer* initializer)
    : TInitializerStep(initializer, "TInitInternalFieldsStep", false) {
}

void TInitInternalFieldsStep::Execute(const TActorContext &ctx) {
    Partition()->Initialize(ctx);

    Done(ctx);
}


//
// InitDiskStatusStep
//

TInitDiskStatusStep::TInitDiskStatusStep(TInitializer* initializer)
    : TBaseKVStep(initializer, "TInitDiskStatusStep", true) {
}

void TInitDiskStatusStep::Execute(const TActorContext& ctx) {
    THolder<TEvKeyValue::TEvRequest> request(new TEvKeyValue::TEvRequest);

    AddCheckDiskRequest(request.Get(), Partition()->NumChannels);

    ctx.Send(Partition()->TabletActorId, request.Release());
}

void TInitDiskStatusStep::Handle(TEvKeyValue::TEvResponse::TPtr& ev, const TActorContext& ctx) {
    ValidateResponse(*this, ev);

    auto& response = ev->Get()->Record;
    PQ_INIT_ENSURE(response.GetStatusResultSize());

    Partition()->DiskIsFull = DiskIsFull(ev);
    if (Partition()->DiskIsFull) {
        Partition()->LogAndCollectError(NKikimrServices::PERSQUEUE, "disk is full", ctx);
    }

    Done(ctx);
}


//
// TInitMetaStep
//

TInitMetaStep::TInitMetaStep(TInitializer* initializer)
    : TBaseKVStep(initializer, "TInitMetaStep", true) {
}

void TInitMetaStep::Execute(const TActorContext& ctx) {
    auto addKey = [](NKikimrClient::TKeyValueRequest& request, TKeyPrefix::EType type, const TPartitionId& partition) {
        auto read = request.AddCmdRead();
        TKeyPrefix key{type, partition};
        read->SetKey(key.Data(), key.Size());
    };

    THolder<TEvKeyValue::TEvRequest> request(new TEvKeyValue::TEvRequest);

    addKey(request->Record, TKeyPrefix::TypeMeta, PartitionId());
    addKey(request->Record, TKeyPrefix::TypeTxMeta, PartitionId());

    ctx.Send(Partition()->TabletActorId, request.Release());
}

void TInitMetaStep::Handle(TEvKeyValue::TEvResponse::TPtr &ev, const TActorContext &ctx) {
    ValidateResponse(*this, ev);

    auto& response = ev->Get()->Record;
    PQ_INIT_ENSURE(response.ReadResultSize() == 2);
    LoadMeta(response);
    Done(ctx);
}

void TInitMetaStep::LoadMeta(const NKikimrClient::TResponse& kvResponse) {
    auto handleReadResult = [&](const NKikimrClient::TKeyValueResponse::TReadResult& response, auto&& action) {
        switch (response.GetStatus()) {
        case NKikimrProto::OK:
            action(response);
            break;
        case NKikimrProto::NODATA:
            break;
        default:
            AFL_ENSURE(false)("status", response.GetStatus());
        };
    };

    auto loadMeta = [&](const NKikimrClient::TKeyValueResponse::TReadResult& response) {
        NKikimrPQ::TPartitionMeta meta;
        bool res = meta.ParseFromString(response.GetValue());
        PQ_INIT_ENSURE(res);

        Partition()->BlobEncoder.StartOffset = meta.GetStartOffset();
        Partition()->BlobEncoder.EndOffset = meta.GetEndOffset();
        Partition()->BlobEncoder.FirstUncompactedOffset = meta.GetFirstUncompactedOffset();

        if (Partition()->BlobEncoder.StartOffset == Partition()->BlobEncoder.EndOffset) {
           Partition()->BlobEncoder.NewHead.Offset = Partition()->BlobEncoder.Head.Offset = Partition()->BlobEncoder.EndOffset;
        }

        if (meta.HasStartOffset()) {
            GetContext().StartOffset = meta.GetStartOffset();
        }
        if (meta.HasEndOffset()) {
            GetContext().EndOffset = meta.GetEndOffset();
        }

        Partition()->SubDomainOutOfSpace = meta.GetSubDomainOutOfSpace();
        Partition()->EndWriteTimestamp = TInstant::MilliSeconds(meta.GetEndWriteTimestamp());
        Partition()->PendingWriteTimestamp = Partition()->EndWriteTimestamp;
        Partition()->MessageIdDeduplicator.NextMessageIdDeduplicatorWAL = meta.GetNextMessageIdDeduplicatorWAL();
        if (Partition()->IsSupportive()) {
            const auto& counterData = meta.GetCounterData();
            Partition()->BytesWrittenGrpc.SetSavedValue(counterData.GetBytesWrittenGrpc());
            Partition()->BytesWrittenTotal.SetSavedValue(counterData.GetBytesWrittenTotal());
            Partition()->BytesWrittenUncompressed.SetSavedValue(counterData.GetBytesWrittenUncompressed());
            Partition()->MsgsWrittenGrpc.SetSavedValue(counterData.GetMessagesWrittenGrpc());
            Partition()->MsgsWrittenTotal.SetSavedValue(counterData.GetMessagesWrittenTotal());

            Partition()->MessageSize.SetValues(counterData.GetMessagesSizes());
        }
    };
    handleReadResult(kvResponse.GetReadResult(0), loadMeta);

    auto loadTxMeta = [this](const NKikimrClient::TKeyValueResponse::TReadResult& response) {
        NKikimrPQ::TPartitionTxMeta meta;
        bool res = meta.ParseFromString(response.GetValue());
        PQ_INIT_ENSURE(res);

        if (meta.HasPlanStep()) {
            Partition()->PlanStep = meta.GetPlanStep();
        }
        if (meta.HasTxId()) {
            Partition()->TxId = meta.GetTxId();
        }
    };
    handleReadResult(kvResponse.GetReadResult(1), loadTxMeta);
}


//
// TInitInfoRangeStep
//

TInitInfoRangeStep::TInitInfoRangeStep(TInitializer* initializer)
    : TBaseKVStep(initializer, "TInitInfoRangeStep", true) {
}

void TInitInfoRangeStep::Execute(const TActorContext &ctx) {
    RequestInfoRange(ctx, Partition()->TabletActorId, PartitionId(), "");
}

void TInitInfoRangeStep::Handle(TEvKeyValue::TEvResponse::TPtr &ev, const TActorContext &ctx) {
    ValidateResponse(*this, ev);

    auto& response = ev->Get()->Record;
    PQ_INIT_ENSURE(response.ReadRangeResultSize() == 1);

    auto& range = response.GetReadRangeResult(0);
    auto now = ctx.Now();

    PQ_INIT_ENSURE(response.ReadRangeResultSize() == 1);
    //megaqc check here all results
    PQ_INIT_ENSURE(range.HasStatus());
    const TString *key = nullptr;
    switch (range.GetStatus()) {
        case NKikimrProto::OK:
        case NKikimrProto::OVERRUN: {
            auto& sourceIdStorage = Partition()->SourceIdStorage;
            auto& usersInfoStorage = Partition()->UsersInfoStorage;
            const bool isSupportive = Partition()->IsSupportive();

            for (ui32 i = 0; i < range.PairSize(); ++i) {
                const auto& pair = range.GetPair(i);
                PQ_INIT_ENSURE(pair.HasStatus());

                AFL_ENSURE(pair.GetStatus() == NKikimrProto::OK)
                    ("status", pair.GetStatus())
                    ("key", pair.GetKey());

                PQ_INIT_ENSURE(pair.HasKey());
                PQ_INIT_ENSURE(pair.HasValue());

                key = &pair.GetKey();
                const auto type = (*key)[TKeyPrefix::MarkPosition()];
                if (type == TKeyPrefix::MarkSourceId) {
                    sourceIdStorage.LoadSourceIdInfo(*key, pair.GetValue(), now);
                } else if (type == TKeyPrefix::MarkProtoSourceId) {
                    sourceIdStorage.LoadSourceIdInfo(*key, pair.GetValue(), now);
                } else if ((type == TKeyPrefix::MarkUser) && !isSupportive) {
                    usersInfoStorage->Parse(*key, pair.GetValue(), ctx);
                } else if ((type == TKeyPrefix::MarkUserDeprecated) && !isSupportive) {
                    usersInfoStorage->ParseDeprecated(*key, pair.GetValue(), ctx);
                }
            }
            //make next step
            if (range.GetStatus() == NKikimrProto::OVERRUN) {
                PQ_INIT_ENSURE(key);
                RequestInfoRange(ctx, Partition()->TabletActorId, PartitionId(), *key);
            } else {
                PostProcessing(ctx);
            }
            break;
        }
        case NKikimrProto::NODATA:
            PostProcessing(ctx);
            break;
        default:
            AFL_ENSURE(false)("status", range.GetStatus());
    };
}

void TInitInfoRangeStep::PostProcessing(const TActorContext& ctx) {
    auto& usersInfoStorage = Partition()->UsersInfoStorage;
    for (auto&& [_, userInfo] : usersInfoStorage->GetAll()) {
        userInfo.AnyCommits = userInfo.Offset > (i64)Partition()->BlobEncoder.StartOffset;
    }

    Done(ctx);
}


//
// TInitDataRangeStep
//

TInitDataRangeStep::TInitDataRangeStep(TInitializer* initializer)
    : TBaseKVStep(initializer, "TInitDataRangeStep", true) {
}

void TInitDataRangeStep::Execute(const TActorContext &ctx) {
    Ranges.clear();
    RequestDataRange(ctx, Partition()->TabletActorId, PartitionId(), "");
}

void TInitDataRangeStep::Handle(TEvKeyValue::TEvResponse::TPtr &ev, const TActorContext &ctx) {
    ValidateResponse(*this, ev);

    auto& response = ev->Get()->Record;
    PQ_INIT_ENSURE(response.ReadRangeResultSize() == 1);

    auto& range = response.GetReadRangeResult(0);

    PQ_INIT_ENSURE(range.HasStatus());
    switch(range.GetStatus()) {
        case NKikimrProto::OK:
        case NKikimrProto::OVERRUN:
            Ranges.push_back(range);

            if (range.GetStatus() == NKikimrProto::OVERRUN) { //request rest of range
                PQ_INIT_ENSURE(range.PairSize());
                RequestDataRange(ctx, Partition()->TabletActorId, PartitionId(), range.GetPair(range.PairSize() - 1).GetKey());
                return;
            }

            FillBlobsMetaData(ctx);
            FormHeadAndProceed();

            // AFL_ENSURE(!GetContext().StartOffset || *GetContext().StartOffset >= Partition()->GetStartOffset())
            //     ("d", "StartOffset from meta and blobs are different")
            //     ("l", *GetContext().StartOffset)
            //     ("r", Partition()->GetStartOffset());

            // AFL_ENSURE(!GetContext().EndOffset || *GetContext().EndOffset == Partition()->GetEndOffset())
            //     ("d", "EndOffset from meta and blobs are different")
            //     ("l", *GetContext().EndOffset)
            //     ("r", Partition()->GetEndOffset());

            [[fallthrough]];

        case NKikimrProto::NODATA:
            Done(ctx);
            break;
        default:
            AFL_ENSURE(false)("status", range.GetStatus());
    };
}

THashSet<TString> FilterBlobsMetaData(const TVector<NKikimrClient::TKeyValueResponse::TReadRangeResult>& ranges,
                                      const TPartitionId& partitionId)
{
    TVector<TString> keys;

    for (const auto& range : ranges) {
        for (ui32 i = 0; i < range.PairSize(); ++i) {
            const auto& pair = range.GetPair(i);
            AFL_ENSURE(pair.GetStatus() == NKikimrProto::OK) //this is readrange without keys, only OK could be here
                ("status", pair.GetStatus())
                ("key", pair.GetKey());
            keys.push_back(pair.GetKey());
        }
    }

    auto compare = [](const TString& lhs, const TString& rhs) {
        auto getKeySuffix = [](const TString& v) {
            return (v.back() == TKey::ESuffix::FastWrite) ? TKey::ESuffix::FastWrite : TKey::ESuffix::Head;
        };

        if (getKeySuffix(lhs) == getKeySuffix(rhs)) {
            return lhs < rhs;
        }

        return getKeySuffix(lhs) == TKey::ESuffix::Head;
    };
    std::sort(keys.begin(), keys.end(), compare);

    for (size_t i = 0; i < keys.size(); ++i) {
        PQ_INIT_LOG_D("key[" << i << "]: " << keys[i]);
    }

    TVector<TString> filtered;
    TKey lastKey;

    for (auto& k : keys) {
        if (filtered.empty()) {
            PQ_INIT_LOG_D("add key " << k);
            filtered.push_back(std::move(k));
            lastKey = TKey::FromString(filtered.back(), partitionId);
        } else {
            auto candidate = TKey::FromString(k, partitionId);

            if (lastKey.GetOffset() == candidate.GetOffset()) {
                if (lastKey.GetPartNo() == candidate.GetPartNo()) {
                    if (lastKey.GetCount() < candidate.GetCount()) {
                        // candidate содержит lastKey
                        PQ_INIT_LOG_D("replace key " << filtered.back() << " to " << k);
                        filtered.back() = std::move(k);
                        lastKey = candidate;
                    } else if (lastKey.GetCount() == candidate.GetCount()) {
                        if (lastKey.GetInternalPartsCount() < candidate.GetInternalPartsCount()) {
                            // candidate содержит lastKey
                            PQ_INIT_LOG_D("replace key " << filtered.back() << " to " << k);
                            filtered.back() = std::move(k);
                            lastKey = candidate;
                        } else {
                            // lastKey содержит candidate
                            PQ_INIT_LOG_D("ignore key " << k);
                        }
                    } else {
                        // lastKey содержит candidate
                        PQ_INIT_LOG_D("ignore key " << k);
                    }
                } else if (lastKey.GetPartNo() > candidate.GetPartNo()) {
                    // lastKey содержит candidate
                    PQ_INIT_LOG_D("ignore key " << k);
                } else {
                    // candidate после lastKey
                    PQ_INIT_LOG_D("add key " << k);
                    filtered.push_back(std::move(k));
                    lastKey = candidate;
                }
            } else {
                if (const ui64 nextOffset = lastKey.GetOffset() + lastKey.GetCount(); nextOffset > candidate.GetOffset()) {
                    // lastKey содержит candidate
                    PQ_INIT_LOG_D("ignore key " << k);
                } else {
                    // candidate после lastKey или пропуск между lastKey и candidate
                    PQ_INIT_LOG_D("add key " << k);
                    filtered.push_back(std::move(k));
                    lastKey = candidate;
                }
            }
        }
    }

    return {filtered.begin(), filtered.end()};
}

static void CheckKeysTimestampOrder(const std::deque<TDataKey>& keys) {
    if (keys.size() < 2) {
        return;
    }
    ui64 disorderPairCount = 0;
    TString sample;
    auto prev = keys.begin();
    auto curr = std::next(prev);
    while (curr != keys.end()) {
        if (curr->Timestamp < prev->Timestamp) {
            ++disorderPairCount;
            if (sample.empty()) {
                TStringOutput out(sample);
                out << " prev_timestamp=" << prev->Timestamp
                    << " curr_timestamp=" << curr->Timestamp
                    << " prev_key=" << prev->Key.ToString()
                    << " curr_key=" << curr->Key.ToString()
                    << " index=" << std::distance(keys.begin(), curr);
            }
        }
        prev = curr++;
    }
    if (disorderPairCount > 0) {
        PQ_LOG_ERROR("Data keys have " << disorderPairCount << " misarranged timestamps; sample: " << sample);
    }
}

void TInitDataRangeStep::FillBlobsMetaData(const TActorContext&) {
    auto& endOffset = Partition()->BlobEncoder.EndOffset;
    auto& startOffset = Partition()->BlobEncoder.StartOffset;
    auto& head = Partition()->BlobEncoder.Head;
    auto& dataKeysBody = Partition()->BlobEncoder.DataKeysBody;
    auto& gapOffsets = Partition()->GapOffsets;
    auto& gapSize = Partition()->GapSize;
    auto& bodySize = Partition()->BlobEncoder.BodySize;

    // If there are multiple keys for a message, then only the key that contains more messages remains.
    //
    // Extra keys will be added to the queue for deletion.
    const auto actualKeys = FilterBlobsMetaData(Ranges, PartitionId());

    for (const auto& range : Ranges) {
        for (ui32 i = 0; i < range.PairSize(); ++i) {
            const auto& pair = range.GetPair(i);
            PQ_INIT_ENSURE(pair.GetStatus() == NKikimrProto::OK); //this is readrange without keys, only OK could be here
            PQ_LOG_D("check key " << pair.GetKey());
            const auto k = TKey::FromString(pair.GetKey(), PartitionId());
            if (!actualKeys.contains(pair.GetKey())) {
                PQ_LOG_D("unknown key " << pair.GetKey() << " will be deleted");
                GetContext().DeletedKeys.emplace_back(k.ToString());
                continue;
            }
            if (dataKeysBody.empty()) { //no data - this is first pair of first range
                head.Offset = endOffset = startOffset = k.GetOffset();
                if (k.GetPartNo() > 0) {
                    ++startOffset;
                }
                head.PartNo = 0;
            } else {
                PQ_INIT_ENSURE(endOffset <= k.GetOffset())("endOffset", endOffset)("key", pair.GetKey());
                if (endOffset < k.GetOffset()) {
                    gapOffsets.push_back(std::make_pair(endOffset, k.GetOffset()));
                    gapSize += k.GetOffset() - endOffset;
                }
            }
            PQ_INIT_ENSURE(k.GetCount() + k.GetInternalPartsCount() > 0);
            PQ_INIT_ENSURE(k.GetOffset() >= endOffset);
            endOffset = k.GetOffset() + k.GetCount();
            //at this point EndOffset > StartOffset
            if (!k.HasSuffix() || !k.IsHead()) { //head.Size will be filled after read or head blobs
                bodySize += pair.GetValueSize();
            }

            PQ_LOG_D("Got data offset " << k.GetOffset() << " count " << k.GetCount() << " size " << pair.GetValueSize()
                     << " so " << startOffset << " eo " << endOffset << " " << pair.GetKey()
                    );
            dataKeysBody.emplace_back(k,
                                      pair.GetValueSize(),
                                      TInstant::Seconds(pair.GetCreationUnixTime()),
                                      dataKeysBody.empty() ? 0 : dataKeysBody.back().CumulativeSize + dataKeysBody.back().Size,
                                      Partition()->MakeBlobKeyToken(k.ToString()));
        }
    }
    CheckKeysTimestampOrder(dataKeysBody);

    PQ_INIT_ENSURE(endOffset >= startOffset);
}

struct TKeyBoundaries {
    // [0, Head)         -- body
    // [Head, fastWrite) -- Head
    // [FastWrite, inf)  -- FastWrite
    size_t Head = 0;
    size_t FastWrite = 0;
};

TKeyBoundaries SplitBodyHeadAndFastWrite(const std::deque<TDataKey>& keys)
{
    TKeyBoundaries b;

    for (; b.Head < keys.size(); ++b.Head) {
        const auto& e = keys[b.Head];
        if (e.Key.HasSuffix()) {
            // Head or FastWrite
            break;
        }
    }

    for (b.FastWrite = b.Head; b.FastWrite < keys.size(); ++b.FastWrite) {
        const auto& e = keys[b.FastWrite];
        if (!e.Key.IsHead()) {
            break;
        }
    }

    AFL_ENSURE(b.Head <= b.FastWrite)("head", b.Head)("fastWrite", b.FastWrite);

    return b;
}

void TInitDataRangeStep::FormHeadAndProceed() {
    auto& endOffset = Partition()->BlobEncoder.EndOffset;
    auto& startOffset = Partition()->BlobEncoder.StartOffset;
    auto& dataKeysBody = Partition()->BlobEncoder.DataKeysBody;

    auto keys = std::move(dataKeysBody);
    dataKeysBody.clear();

    auto& cz = Partition()->CompactionBlobEncoder; // Compaction zone
    auto& fwz = Partition()->BlobEncoder;   // FastWrite zone

    cz.StartOffset = Max<ui64>();
    cz.EndOffset = Min<ui64>();
    cz.BodySize = 0;

    fwz.Head.Offset = endOffset;
    fwz.Head.PartNo = 0;
    fwz.StartOffset = Max<ui64>();
    fwz.EndOffset = Min<ui64>();
    fwz.BodySize = 0;

    auto kb = SplitBodyHeadAndFastWrite(keys);

    // Compaction Body
    for (size_t k = 0; k < kb.Head; ++k) {
        cz.BodySize += keys[k].Size;
        keys[k].CumulativeSize = cz.DataKeysBody.empty() ? 0 : (cz.DataKeysBody.back().CumulativeSize + cz.DataKeysBody.back().Size);
        cz.DataKeysBody.push_back(std::move(keys[k]));

        const auto& bodyKey = cz.DataKeysBody.back().Key;

        cz.Head.Offset = bodyKey.GetOffset() + bodyKey.GetCount();
        cz.Head.PartNo = 0;
    }

    // Compaction Head
    for (size_t k = kb.Head; k < kb.FastWrite; ++k) {
        cz.HeadKeys.push_back(std::move(keys[k]));
    }

    if (!cz.HeadKeys.empty()) {
        const auto& headKey = cz.HeadKeys.front().Key;

        cz.Head.Offset = headKey.GetOffset();
        cz.Head.PartNo = headKey.GetPartNo();

        Partition()->WasTheLastBlobBig = false;
    }

    // FastWrite Body
    for (size_t k = kb.FastWrite; k < keys.size(); ++k) {
        fwz.BodySize += keys[k].Size;
        keys[k].CumulativeSize = fwz.DataKeysBody.empty() ? 0 : (fwz.DataKeysBody.back().CumulativeSize + fwz.DataKeysBody.back().Size);
        fwz.DataKeysBody.push_back(std::move(keys[k]));

        const auto& bodyKey = fwz.DataKeysBody.back().Key;

        fwz.Head.Offset = bodyKey.GetOffset() + bodyKey.GetCount();
        fwz.Head.PartNo = 0;
    }

    auto getStartOffset = [](const TKey& k) {
        return k.GetOffset() + (k.GetPartNo() ? 1 : 0);
    };
    auto getEndOffset = [](const TKey& k) {
        return k.GetOffset() + k.GetCount();
    };

    if (!cz.HeadKeys.empty()) {
        const auto& front = cz.HeadKeys.front();
        const auto& back = cz.HeadKeys.back();

        cz.StartOffset = getStartOffset(front.Key);
        cz.EndOffset = getEndOffset(back.Key);
    }

    if (!cz.DataKeysBody.empty()) {
        const auto& front = cz.DataKeysBody.front();
        const auto& back = cz.DataKeysBody.back();

        cz.StartOffset = Min<ui64>(getStartOffset(front.Key), cz.StartOffset);
        cz.EndOffset = Max<ui64>(getEndOffset(back.Key), cz.EndOffset);
    }

    if (!fwz.DataKeysBody.empty()) {
        const auto& front = fwz.DataKeysBody.front();
        const auto& back = fwz.DataKeysBody.back();

        fwz.StartOffset = getStartOffset(front.Key);
        fwz.EndOffset = getEndOffset(back.Key);
    }

    if (cz.StartOffset > cz.EndOffset) {
        cz.StartOffset = cz.EndOffset = fwz.StartOffset;
    }

    if (fwz.StartOffset > fwz.EndOffset) {
        fwz.StartOffset = fwz.EndOffset = cz.EndOffset;
    }

    if (cz.IsEmpty()) {
        cz.Head.Offset = fwz.StartOffset;
    }

    PQ_INIT_ENSURE((cz.StartOffset <= cz.EndOffset) && (fwz.StartOffset <= fwz.EndOffset) && (cz.EndOffset <= fwz.StartOffset))
        ("cz.StartOffset", cz.StartOffset)("cz.EndOffset", cz.EndOffset)
        ("fwz.StartOffset", fwz.StartOffset)("fwz.EndOffset", fwz.EndOffset);

    PQ_INIT_ENSURE(fwz.HeadKeys.empty() || fwz.Head.Offset == fwz.HeadKeys.front().Key.GetOffset() && fwz.Head.PartNo == fwz.HeadKeys.front().Key.GetPartNo());
    PQ_INIT_ENSURE(fwz.Head.Offset < endOffset || fwz.Head.Offset == endOffset && fwz.HeadKeys.empty());
    PQ_INIT_ENSURE(fwz.Head.Offset >= startOffset || fwz.Head.Offset == startOffset - 1 && fwz.Head.PartNo > 0);
}

//
// TDeleteKeysStep
//

TDeleteKeysStep::TDeleteKeysStep(TInitializer* initializer)
    : TBaseKVStep(initializer, "TDeleteKeysStep", true) {
}

void TDeleteKeysStep::Execute(const TActorContext &ctx) {
    if (GetContext().DeletedKeys.empty()) {
        Done(ctx);
        return;
    }

    auto request = std::make_unique<TEvKeyValue::TEvRequest>();
    for (auto& key : GetContext().DeletedKeys) {
        auto* cmd = request->Record.AddCmdDeleteRange();
        cmd->MutableRange()->SetFrom(key);
        cmd->MutableRange()->SetIncludeFrom(true);
        cmd->MutableRange()->SetTo(std::move(key));
        cmd->MutableRange()->SetIncludeTo(true);
    }
    GetContext().DeletedKeys = {};

    ctx.Send(Partition()->TabletActorId, request.release());
}

void TDeleteKeysStep::Handle(TEvKeyValue::TEvResponse::TPtr &ev, const TActorContext &ctx) {
    ValidateResponse(*this, ev);

    Done(ctx);
}

//
// TInitMessageDeduplicatorStep
//

TInitMessageDeduplicatorStep::TInitMessageDeduplicatorStep(TInitializer* initializer)
    : TBaseKVStep(initializer, "TInitMessageDeduplicatorStep", true) {
}

void TInitMessageDeduplicatorStep::Execute(const TActorContext &ctx) {
    if (MirroringEnabled(Partition()->Config) || Partition()->Partition.IsSupportivePartition()) {
        return Done(ctx);
    }
    auto firstKey = MakeDeduplicatorWALKey(Partition()->Partition.OriginalPartitionId, 0);
    RequestDeduplicatorRange(ctx, Partition()->TabletActorId, PartitionId(), firstKey);
}

void TInitMessageDeduplicatorStep::Handle(TEvKeyValue::TEvResponse::TPtr &ev, const TActorContext &ctx) {
    ValidateResponse(*this, ev);

    auto& response = ev->Get()->Record;
    PQ_INIT_ENSURE(response.ReadRangeResultSize() == 1);

    auto* range = response.MutableReadRangeResult(0);

    PQ_INIT_ENSURE(range->HasStatus());
    switch(range->GetStatus()) {
        case NKikimrProto::OK:
        case NKikimrProto::OVERRUN:
            for (auto& w : *range->MutablePair()) {
                NKikimrPQ::TMessageDeduplicationIdWAL wal;
                auto r = wal.ParseFromString(w.GetValue());
                AFL_ENSURE(r)("key", w.key());

                auto a = Partition()->MessageIdDeduplicator.ApplyWAL(std::move(*w.MutableKey()), std::move(wal));
                AFL_ENSURE(a)("key", w.key());
            }

            if (range->GetStatus() == NKikimrProto::OVERRUN) { //request rest of range
                PQ_INIT_ENSURE(range->PairSize());
                RequestDeduplicatorRange(ctx, Partition()->TabletActorId, PartitionId(), range->GetPair(range->PairSize() - 1).GetKey());
                return;
            }

            [[fallthrough]];
        case NKikimrProto::NODATA:
            Done(ctx);
            break;
        default:
            AFL_ENSURE(false && "bad status")("status", range->GetStatus());
    };
}



//
// TInitDataStep
//

TInitDataStep::TInitDataStep(TInitializer* initializer)
    : TBaseKVStep(initializer, "TInitDataStep", true) {
}

void TInitDataStep::Execute(const TActorContext &ctx) {
    TVector<TString> keys;
    //form head request
    for (const auto& p : Partition()->CompactionBlobEncoder.HeadKeys) {
        keys.emplace_back(p.Key.Data(), p.Key.Size());
    }
    PQ_INIT_ENSURE(keys.size() < Partition()->TotalMaxCount);
    if (keys.empty()) {
        Done(ctx);
        return;
    }

    THolder<TEvKeyValue::TEvRequest> request(new TEvKeyValue::TEvRequest);
    for (auto& key: keys) {
        auto read = request->Record.AddCmdRead();
        read->SetKey(key);
    }
    ctx.Send(Partition()->TabletActorId, request.Release());
}

void TInitDataStep::Handle(TEvKeyValue::TEvResponse::TPtr &ev, const TActorContext &ctx) {
    ValidateResponse(*this, ev);

    auto& response = ev->Get()->Record;
    PQ_INIT_ENSURE(response.ReadResultSize());

    auto& head = Partition()->CompactionBlobEncoder.Head;
    auto& headKeys = Partition()->CompactionBlobEncoder.HeadKeys;
    auto& dataKeysHead = Partition()->CompactionBlobEncoder.DataKeysHead;
    auto& compactLevelBorder = Partition()->CompactLevelBorder;
    auto totalLevels = Partition()->TotalLevels;

    ui32 currentLevel = 0;
    PQ_INIT_ENSURE(headKeys.size() == response.ReadResultSize());
    for (ui32 i = 0; i < response.ReadResultSize(); ++i) {
        auto& read = response.GetReadResult(i);
        PQ_INIT_ENSURE(read.HasStatus());
        switch(read.GetStatus()) {
            case NKikimrProto::OK: {
                const TKey& key = headKeys[i].Key;
                PQ_INIT_ENSURE(key.HasSuffix());

                ui32 size = headKeys[i].Size;
                ui64 offset = key.GetOffset();
                while (currentLevel + 1 < totalLevels && size < compactLevelBorder[currentLevel + 1]) {
                    ++currentLevel;
                }
                PQ_INIT_ENSURE(size < compactLevelBorder[currentLevel])
                    ("l", size)
                    ("r", compactLevelBorder[currentLevel])
                    ("c", currentLevel);

                dataKeysHead[currentLevel].AddKey(key, size);
                PQ_INIT_ENSURE(dataKeysHead[currentLevel].KeysCount() < AppData(ctx)->PQConfig.GetMaxBlobsPerLevel())
                    ("l", dataKeysHead[currentLevel].KeysCount())
                    ("r", AppData(ctx)->PQConfig.GetMaxBlobsPerLevel())
                    ("c", currentLevel);
                PQ_INIT_ENSURE(!dataKeysHead[currentLevel].NeedCompaction())
                    ("c", currentLevel);

                PQ_LOG_D("read res partition offset " << offset << " endOffset " << Partition()->BlobEncoder.EndOffset
                        << " key " << key.GetOffset() << "," << key.GetCount() << " valuesize " << read.GetValue().size()
                        << " expected " << size
                );

                PQ_INIT_ENSURE(offset + 1 >= Partition()->CompactionBlobEncoder.StartOffset);
                PQ_INIT_ENSURE(offset < Partition()->CompactionBlobEncoder.EndOffset)
                    ("offset", offset)("CompactionBlobEncoder.EndOffset", Partition()->CompactionBlobEncoder.EndOffset);
                PQ_INIT_ENSURE(size == read.GetValue().size())("size", size)("read.GetValue().size()", read.GetValue().size());

                for (TBlobIterator it(key, read.GetValue()); it.IsValid(); it.Next()) {
                    PQ_LOG_D("add batch");
                    head.AddBatch(it.GetBatch());
                }
                head.PackedSize += size;

                break;
                }
            case NKikimrProto::ERROR:
                AFL_ENSURE(false)
                    ("m", read.GetMessage())
                    ("e", response.GetErrorReason());
            default:
                AFL_ENSURE(false)("status", read.GetStatus());
        };
    }

    Partition()->InitFirstCompactionPart();

    Done(ctx);
}


//
// TInitEndWriteTimestampStep
//

TInitEndWriteTimestampStep::TInitEndWriteTimestampStep(TInitializer* initializer)
    : TInitializerStep(initializer, "TInitEndWriteTimestampStep", true) {
}

void TInitEndWriteTimestampStep::Execute(const TActorContext &ctx) {
    if (Partition()->EndWriteTimestamp != TInstant::Zero() ||
        (Partition()->BlobEncoder.IsEmpty() && Partition()->CompactionBlobEncoder.IsEmpty())) {
        PQ_LOG_I("Initializing EndWriteTimestamp skipped because already initialized.");
        return Done(ctx);
    }

    const TDataKey* lastKey = nullptr;
    if (!Partition()->BlobEncoder.IsEmpty()) {
        lastKey = Partition()->BlobEncoder.GetLastKey();
    } else if (!Partition()->CompactionBlobEncoder.IsEmpty()) {
        lastKey = Partition()->CompactionBlobEncoder.GetLastKey();
    }

    if (lastKey) {
        Partition()->EndWriteTimestamp = lastKey->Timestamp;
        Partition()->PendingWriteTimestamp = Partition()->EndWriteTimestamp;
    }

    PQ_LOG_I("Initializing EndWriteTimestamp from keys completed. Value " << Partition()->EndWriteTimestamp);

    return Done(ctx);
}

//
// TInitFieldsStep
//

TInitFieldsStep::TInitFieldsStep(TInitializer* initializer)
    : TInitializerStep(initializer, "TInitFieldsStep", false) {
}

void TInitFieldsStep::Execute(const TActorContext &ctx) {
    auto& config = Partition()->Config;

    Partition()->AutopartitioningManager.reset(CreateAutopartitioningManager(config, Partition()->Partition));

    return Done(ctx);
}

//
// TPartition
//

void TPartition::Bootstrap(const TActorContext& ctx) {
    Become(&TThis::StateInit);
    Initializer.Execute(ctx);
}

void TPartition::Initialize(const TActorContext& ctx) {
    if (MirroringEnabled(Config)) {
        ManageWriteTimestampEstimate = !Config.GetPartitionConfig().GetMirrorFrom().GetSyncWriteTime();
    } else {
        ManageWriteTimestampEstimate = IsLocalDC;
    }

    CreationTime = ctx.Now();
    WriteCycleStartTime = ctx.Now();

    ReadQuotaTrackerActor = RegisterWithSameMailbox(CreateReadQuoter(
        AppData(ctx)->PQConfig,
        TopicConverter,
        Config,
        Partition,
        TabletActorId,
        SelfId(),
        TabletId,
        Counters
    ));

    TotalPartitionWriteSpeed = Config.GetPartitionConfig().GetWriteSpeedInBytesPerSecond();
    WriteTimestamp = ctx.Now();
    LastUsedStorageMeterTimestamp = ctx.Now();
    WriteTimestampEstimate = ManageWriteTimestampEstimate ? ctx.Now() : TInstant::Zero();

    CloudId = Config.GetYcCloudId();
    DbId = Config.GetYdbDatabaseId();
    DbPath = Config.GetYdbDatabasePath();
    FolderId = Config.GetYcFolderId();
    MonitoringProjectId = Config.GetMonitoringProjectId();

    UsersInfoStorage.ConstructInPlace(DCId,
                                      TopicConverter,
                                      Partition.InternalPartitionId,
                                      Config,
                                      CloudId,
                                      DbId,
                                      Config.GetYdbDatabasePath(),
                                      IsServerless,
                                      FolderId);
    TotalChannelWritesByHead.resize(NumChannels);

    if (!IsSupportive()) {
        if (AppData()->PQConfig.GetTopicsAreFirstClassCitizen()) {
            PartitionCountersLabeled = CreateProtobufTabletLabeledCounters<EPartitionLabeledCounters_descriptor>(
                                                                         EscapeBadChars(TopicName()),
                                                                         Partition.InternalPartitionId,
                                                                         Config.GetYdbDatabasePath());

            PartitionCountersExtended = CreateProtobufTabletLabeledCounters<EPartitionExtendedLabeledCounters_descriptor>(
                                                                                  EscapeBadChars(TopicName()),
                                                                                  Partition.InternalPartitionId,
                                                                                  Config.GetYdbDatabasePath());
        } else {
            PartitionCountersLabeled = CreateProtobufTabletLabeledCounters<EPartitionLabeledCounters_descriptor>(
                                                                        TopicName(),
                                                                        Partition.InternalPartitionId);
            PartitionCountersExtended = CreateProtobufTabletLabeledCounters<EPartitionExtendedLabeledCounters_descriptor>(
                                                                        TopicName(),
                                                                        Partition.InternalPartitionId);
        }
    }

    UsersInfoStorage->Init(TabletActorId, SelfId(), ctx);

    PQ_ENSURE(AppData(ctx)->PQConfig.GetMaxBlobsPerLevel() > 0);
    ui32 border = LEVEL0;
    MaxSizeCheck = 0;
    MaxBlobSize = AppData(ctx)->PQConfig.GetMaxBlobSize();
    BlobEncoder.ClearPartitionedBlob(Partition, MaxBlobSize);
    for (ui32 i = 0; i < TotalLevels; ++i) {
        CompactLevelBorder.push_back(border);
        MaxSizeCheck += border;
        PQ_ENSURE(i + 1 < TotalLevels && border < MaxBlobSize || i + 1 == TotalLevels && border == MaxBlobSize);
        border *= AppData(ctx)->PQConfig.GetMaxBlobsPerLevel();
        border = Min(border, MaxBlobSize);
    }
    TotalMaxCount = AppData(ctx)->PQConfig.GetMaxBlobsPerLevel() * TotalLevels;

    std::reverse(CompactLevelBorder.begin(), CompactLevelBorder.end());

    for (ui32 i = 0; i < TotalLevels; ++i) {
        BlobEncoder.DataKeysHead.emplace_back(CompactLevelBorder[i]);
        CompactionBlobEncoder.DataKeysHead.emplace_back(CompactLevelBorder[i]);
    }

    if (Config.HasOffloadConfig() && !OffloadActor && !IsSupportive()) {
        OffloadActor = Register(CreateOffloadActor(TabletActorId, TabletId, Partition,
            Config.GetYdbDatabasePath(), Config.GetOffloadConfig()));
    }

    LOG_I("bootstrapping " << Partition << " " << ctx.SelfID);

    if (AppData(ctx)->Counters) {
        if (AppData()->PQConfig.GetTopicsAreFirstClassCitizen()) {
            SetupStreamCounters(ctx);
        } else {
            SetupTopicCounters(ctx);
        }

        SetupDetailedMetrics();
        UsersInfoStorage->SetupDetailedMetrics(ctx);
    }
}

void TPartition::SetupTopicCounters(const TActorContext& ctx) {
    auto counters = AppData(ctx)->Counters;
    auto labels = NPersQueue::GetLabels(TopicConverter);
    const TString suffix = IsLocalDC ? "Original" : "Mirrored";

    WriteBufferIsFullCounter.SetCounter(
        NPersQueue::GetCounters(counters, "writingTime", TopicConverter),
            {{"host", DCId},
            {"Partition", ToString<ui32>(Partition.InternalPartitionId)}},
            {"sensor", "BufferFullTime" + suffix, true});

    auto subGroup = GetServiceCounters(counters, "pqproxy|writeTimeLag");
    InputTimeLag = THolder<NKikimr::NPQ::TPercentileCounter>(new NKikimr::NPQ::TPercentileCounter(
        subGroup, labels, {{"sensor", "TimeLags" + suffix}}, "Interval",
        SLOW_LATENCY_MS_INTERVALS, true));


    subGroup = GetServiceCounters(counters, "pqproxy|writeInfo");
    {
        std::unique_ptr<TPercentileCounter> percentileCounter(new TPercentileCounter(
            subGroup, labels, {{"sensor", "MessageSize" + suffix}}, "Size",
            SIZE_KB_INTERVALS, true));

        MessageSize.Setup(IsSupportive(), std::move(percentileCounter));
    }

    subGroup = GetServiceCounters(counters, "pqproxy|writeSession");
    auto txSuffix = IsSupportive() ? "Uncommitted" : suffix;
    BytesWrittenTotal.Setup(
        IsSupportive(), true,
        NKikimr::NPQ::TMultiCounter(subGroup, labels, {}, {"BytesWritten" + txSuffix}, true));
    BytesWrittenUncompressed.Setup(
        IsSupportive(), false,
        NKikimr::NPQ::TMultiCounter(subGroup, labels, {}, {"UncompressedBytesWritten" + suffix}, true));
    BytesWrittenComp = NKikimr::NPQ::TMultiCounter(subGroup, labels, {}, {"CompactedBytesWritten" + suffix}, true);
    MsgsWrittenTotal.Setup(
        IsSupportive(), true,
        NKikimr::NPQ::TMultiCounter(subGroup, labels, {}, {"MessagesWritten" + txSuffix}, true));
    if (IsLocalDC) {
        MsgsDiscarded = NKikimr::NPQ::TMultiCounter(subGroup, labels, {}, {"DiscardedMessages"}, true);
        BytesDiscarded = NKikimr::NPQ::TMultiCounter(subGroup, labels, {}, {"DiscardedBytes"}, true);
    }

    TVector<NPersQueue::TPQLabelsInfo> aggr = {{{{"Account", TopicConverter->GetAccount()}}, {"total"}}};
    ui32 border = AppData(ctx)->PQConfig.GetWriteLatencyBigMs();
    subGroup = GetServiceCounters(counters, "pqproxy|SLI");
    WriteLatency = NKikimr::NPQ::CreateSLIDurationCounter(subGroup, aggr, "Write", border,
                                                          {100, 200, 500, 1000, 1500, 2000,
                                                           5000, 10'000, 30'000, 99'999'999});
    SLIBigLatency = NKikimr::NPQ::TMultiCounter(subGroup, aggr, {}, {"WriteBigLatency"}, true, "sensor", false);
    WritesTotal = NKikimr::NPQ::TMultiCounter(subGroup, aggr, {}, {"WritesTotal"}, true, "sensor", false);
    if (IsQuotingEnabled()) {
        TopicWriteQuotaWaitCounter = THolder<NKikimr::NPQ::TPercentileCounter>(
            new NKikimr::NPQ::TPercentileCounter(
                GetServiceCounters(counters, "pqproxy|topicWriteQuotaWait"), labels,
                    {{"sensor", "TopicWriteQuotaWait" + suffix}}, "Interval",
                        FAST_LATENCY_MS_INTERVALS, true)
        );
    }

    PartitionWriteQuotaWaitCounter = THolder<NKikimr::NPQ::TPercentileCounter>(
        new NKikimr::NPQ::TPercentileCounter(GetServiceCounters(counters, "pqproxy|partitionWriteQuotaWait"),
            labels, {{"sensor", "PartitionWriteQuotaWait" + suffix}}, "Interval",
                FAST_LATENCY_MS_INTERVALS, true)
    );
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

    if (IsSupportive()) {
        SupportivePartitionTimeLag = MakeHolder<TMultiBucketCounter>(
                TVector<ui64>{100, 200, 500, 1000, 2000, 5000, 10'000, 30'000, 60'000, 180'000, 9'999'999},
                DEFAULT_BUCKET_COUNTER_MULTIPLIER, ctx.Now().MilliSeconds());
    } else {
        InputTimeLag = THolder<NKikimr::NPQ::TPercentileCounter>(new NKikimr::NPQ::TPercentileCounter(
            NPersQueue::GetCountersForTopic(counters, IsServerless), {},
                        subgroups, "bin",
                        SLOW_LATENCY_INTERVALS, true));

    }
    subgroups.back().second = "topic.write.message_size_bytes";
    {
        std::unique_ptr<TPercentileCounter> percentileCounter(new TPercentileCounter(
            NPersQueue::GetCountersForTopic(counters, IsServerless), {},
            subgroups, "bin",
            SIZE_INTERVALS, true));
        MessageSize.Setup(IsSupportive(), std::move(percentileCounter));
    }

    subgroups.pop_back();
    TString bytesSuffix = IsSupportive() ? "uncommitted_bytes" : "bytes";
    TString messagesSuffix = IsSupportive() ? "uncommitted_messages" : "messages";
    BytesWrittenGrpc.Setup(
        IsSupportive(), true,
        NKikimr::NPQ::TMultiCounter(
        NPersQueue::GetCountersForTopic(counters, IsServerless), {}, subgroups,
                    {"api.grpc.topic.stream_write." + bytesSuffix} , true, "name"));
    BytesWrittenTotal.Setup(
        IsSupportive(), true,
        NKikimr::NPQ::TMultiCounter(
        NPersQueue::GetCountersForTopic(counters, IsServerless), {}, subgroups,
                    {"topic.write." + bytesSuffix} , true, "name"));

    MsgsWrittenGrpc.Setup(
        IsSupportive(), true,
        NKikimr::NPQ::TMultiCounter(
        NPersQueue::GetCountersForTopic(counters, IsServerless), {}, subgroups,
                    {"api.grpc.topic.stream_write." + messagesSuffix}, true, "name"));
    MsgsWrittenTotal.Setup(
        IsSupportive(), true,
        NKikimr::NPQ::TMultiCounter(
        NPersQueue::GetCountersForTopic(counters, IsServerless), {}, subgroups,
                    {"topic.write." + messagesSuffix}, true, "name"));

    MsgsDiscarded = NKikimr::NPQ::TMultiCounter(
        NPersQueue::GetCountersForTopic(counters, IsServerless), {}, subgroups,
                    {"topic.write.discarded_messages"}, true, "name");
    BytesDiscarded = NKikimr::NPQ::TMultiCounter(
        NPersQueue::GetCountersForTopic(counters, IsServerless), {}, subgroups,
                    {"topic.write.discarded_bytes"} , true, "name");

    BytesWrittenUncompressed.Setup(
        IsSupportive(), false,
        NKikimr::NPQ::TMultiCounter(
        NPersQueue::GetCountersForTopic(counters, IsServerless), {}, subgroups,
                    {"topic.write.uncompressed_bytes"}, true, "name"));

    CompactionUnprocessedCount = TMultiCounter{
        NPersQueue::GetCountersForTopic(counters, IsServerless),
        {},
        subgroups,
        {"topic.partition.blobs.uncompacted_count_max"},
        false,
        "name",
        false};
    CompactionUnprocessedBytes = TMultiCounter{
        NPersQueue::GetCountersForTopic(counters, IsServerless),
        {},
        subgroups,
        {"topic.partition.blobs.uncompacted_bytes_max"},
        false,
        "name",
        false};
    CompactionTimeLag = TMultiCounter{
        NPersQueue::GetCountersForTopic(counters, IsServerless),
        {},
        subgroups,
        {"topic.partition.blobs.compaction_lag_milliseconds_max"},
        false, // not deriv
        "name",
        false // not expiring
    };

    // KeyCompactionReadCyclesTotal = TMultiCounter{
    //     NPersQueue::GetCountersForTopic(counters, IsServerless),
    //     {},
    //     subgroups,
    //     {"topic.key_compaction.read_cycles_complete_total"},
    //     true, // deriv
    //     "name",
    //     true // expiring
    // };

    // KeyCompactionWriteCyclesTotal = TMultiCounter{
    //     NPersQueue::GetCountersForTopic(counters, IsServerless),
    //     {},
    //     subgroups,
    //     {"topic.key_compaction.write_cycles_complete_total"},
    //     true, // deriv
    //     "name",
    //     true // expiring
    // };

    TVector<NPersQueue::TPQLabelsInfo> aggr = {{{{"Account", TopicConverter->GetAccount()}}, {"total"}}};
    ui32 border = AppData(ctx)->PQConfig.GetWriteLatencyBigMs();
    auto subGroup = GetServiceCounters(counters, "pqproxy|SLI");
    WriteLatency = NKikimr::NPQ::CreateSLIDurationCounter(subGroup, aggr, "Write", border,
                                                          {100, 200, 500, 1000, 1500, 2000,
                                                           5000, 10'000, 30'000, 99'999'999});
    SLIBigLatency = NKikimr::NPQ::TMultiCounter(subGroup, aggr, {}, {"WriteBigLatency"}, true, "name", false);
    WritesTotal = NKikimr::NPQ::TMultiCounter(subGroup, aggr, {}, {"WritesTotal"}, true, "name", false);
    if (IsQuotingEnabled()) {
        subgroups.push_back({"name", "topic.write.topic_throttled_milliseconds"});
        TopicWriteQuotaWaitCounter = THolder<NKikimr::NPQ::TPercentileCounter>(
            new NKikimr::NPQ::TPercentileCounter(
                NPersQueue::GetCountersForTopic(counters, IsServerless), {},
                            subgroups, "bin",
                            FAST_LATENCY_INTERVALS, true)
        );
        subgroups.pop_back();
    }

    subgroups.push_back({"name", "topic.write.partition_throttled_milliseconds"});
    PartitionWriteQuotaWaitCounter = THolder<NKikimr::NPQ::TPercentileCounter>(
        new NKikimr::NPQ::TPercentileCounter(
            NPersQueue::GetCountersForTopic(counters, IsServerless), {}, subgroups, "bin",
            FAST_LATENCY_INTERVALS, true)
    );
}

void TPartition::CreateCompacter() {
    if (!IsKeyCompactionEnabled()) {
        if (!IsSupportive()) {
            Send(ReadQuotaTrackerActor, new TEvPQ::TEvReleaseExclusiveLock());
        }
        Compacter.Reset();
        PartitionKeyCompactionCounters.reset();
        return;
    }
    if (Compacter) {
        Compacter->TryCompactionIfPossible();
        return;
    }
    auto& userInfo = UsersInfoStorage->GetOrCreate(CLIENTID_COMPACTION_CONSUMER, ActorContext());
    ui64 compStartOffset = userInfo.Offset;
    Compacter = MakeHolder<TPartitionCompaction>(compStartOffset, ++CompacterCookie, this);

    //Init compacter counters
    if (AppData()->PQConfig.GetTopicsAreFirstClassCitizen()) {
        PartitionKeyCompactionCounters = CreateProtobufTabletLabeledCounters<EPartitionKeyCompactionLabeledCounters_descriptor>(
                                            EscapeBadChars(TopicName()),
                                            Partition.OriginalPartitionId,
                                            Config.GetYdbDatabasePath()
                                        );
    } else {
        PartitionKeyCompactionCounters = CreateProtobufTabletLabeledCounters<EPartitionKeyCompactionLabeledCounters_descriptor>(
                                            TopicName(),
                                            Partition.OriginalPartitionId
                                        );
    }
    Compacter->TryCompactionIfPossible();
}

//
// Functions
//

void ValidateResponse(const TInitializerStep& step, TEvKeyValue::TEvResponse::TPtr& ev) {
    auto& response = ev->Get()->Record;
    AFL_ENSURE(response.GetStatus() == NMsgBusProxy::MSTATUS_OK)
        ("d", "commands for topic are not processed at all")
        ("topic", step.TopicName())
        ("status", response.GetStatus());

    for (ui32 i = 0; i < response.GetStatusResultSize(); ++i) {
        auto& res = response.GetGetStatusResult(i);
        AFL_ENSURE(res.GetStatus() == NKikimrProto::OK)
            ("d", "got KV error in CmdGetStatus")
            ("topic", step.TopicName())
            ("status", res.GetStatus());
    }
}

bool DiskIsFull(TEvKeyValue::TEvResponse::TPtr& ev) {
    auto& response = ev->Get()->Record;

    bool diskIsOk = true;
    for (ui32 i = 0; i < response.GetStatusResultSize(); ++i) {
        auto& res = response.GetGetStatusResult(i);
        TStorageStatusFlags status = res.GetStatusFlags();
        diskIsOk = diskIsOk && !status.Check(NKikimrBlobStorage::StatusDiskSpaceYellowStop);
    }
    return !diskIsOk;
}

void AddCmdDeleteRange(TEvKeyValue::TEvRequest& request, TKeyPrefix::EType c, const TPartitionId& partitionId)
{
    auto keyPrefixes = MakeKeyPrefixRange(c, partitionId);
    const TKeyPrefix& from = keyPrefixes.first;
    const TKeyPrefix& to = keyPrefixes.second;

    auto del = request.Record.AddCmdDeleteRange();
    auto range = del->MutableRange();

    range->SetFrom(from.Data(), from.Size());
    range->SetIncludeFrom(true);
    range->SetTo(to.Data(), to.Size());
    range->SetIncludeTo(false);
}

static void RequestRange(const TActorContext& ctx, const TActorId& dst, const TPartitionId& partition,
                         TKeyPrefix::EType c, bool includeData = false, const TString& key = "", bool dropTmp = false) {
    THolder<TEvKeyValue::TEvRequest> request(new TEvKeyValue::TEvRequest);

    auto keyPrefixes = MakeKeyPrefixRange(c, partition);
    TKeyPrefix& from = keyPrefixes.first;
    const TKeyPrefix& to = keyPrefixes.second;

    if (!key.empty()) {
        AFL_ENSURE(key.StartsWith(TStringBuf(from.Data(), from.Size())));
        from.Clear();
        from.Append(key.data(), key.size());
    }

    auto read = request->Record.AddCmdReadRange();
    auto range = read->MutableRange();

    range->SetFrom(from.Data(), from.Size());
    range->SetTo(to.Data(), to.Size());

    if (includeData)
        read->SetIncludeData(true);

    if (dropTmp) {
        AddCmdDeleteRange(*request, TKeyPrefix::TypeTmpData, partition);
    }

    PQ_LOG_D("Read range request. From " << from.ToString() << " to " << to.ToString());

    ctx.Send(dst, request.Release());
}

void RequestInfoRange(const TActorContext& ctx, const TActorId& dst, const TPartitionId& partition, const TString& key) {
    RequestRange(ctx, dst, partition, TKeyPrefix::TypeInfo, true, key, key == "");
}

void RequestDataRange(const TActorContext& ctx, const TActorId& dst, const TPartitionId& partition, const TString& key) {
    RequestRange(ctx, dst, partition, TKeyPrefix::TypeData, false, key);
}

void RequestDeduplicatorRange(const TActorContext& ctx, const TActorId& dst, const TPartitionId& partition, const TString& key) {
    RequestRange(ctx, dst, partition, TKeyPrefix::TypeDeduplicator, true, key, key == "");
}

} // namespace NKikimr::NPQ
