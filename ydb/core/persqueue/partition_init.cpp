#include "partition.h"
#include "partition_util.h"

namespace NKikimr::NPQ {

static const ui32 LEVEL0 = 32;
static const TString WRITE_QUOTA_ROOT_PATH = "write-quota";


void CalcTopicWriteQuotaParams(const NKikimrPQ::TPQConfig& pqConfig,
                               bool isLocalDC,
                               NPersQueue::TTopicConverterPtr topicConverter,
                               ui64 tabletId,
                               const TActorContext& ctx,
                               TString& topicWriteQuoterPath,
                               TString& topicWriteQuotaResourcePath);
bool DiskIsFull(TEvKeyValue::TEvResponse::TPtr& ev);
void RequestInfoRange(const TActorContext& ctx, const TActorId& dst, ui32 partition, const TString& key);
void RequestDataRange(const TActorContext& ctx, const TActorId& dst, ui32 partition, const TString& key);
bool ValidateResponse(const TInitializerStep& step, TEvKeyValue::TEvResponse::TPtr& ev, const TActorContext& ctx);

//
// TInitializer
//

TInitializer::TInitializer(TPartition* partition)
    : Partition(partition)
    , InProgress(false) {

    Steps.push_back(MakeHolder<TInitConfigStep>(this));
    Steps.push_back(MakeHolder<TInitInternalFieldsStep>(this));
    Steps.push_back(MakeHolder<TInitDiskStatusStep>(this));
    Steps.push_back(MakeHolder<TInitMetaStep>(this));
    Steps.push_back(MakeHolder<TInitInfoRangeStep>(this));
    Steps.push_back(MakeHolder<TInitDataRangeStep>(this));
    Steps.push_back(MakeHolder<TInitDataStep>(this));

    CurrentStep = Steps.begin();
}

void TInitializer::Execute(const TActorContext& ctx) {
    Y_VERIFY(!InProgress, "Initialization already in progress");
    InProgress = true;
    DoNext(ctx);
}

bool TInitializer::Handle(STFUNC_SIG) {
    Y_VERIFY(InProgress, "Initialization is not started");
    return CurrentStep->Get()->Handle(ev);
}

void TInitializer::Next(const TActorContext& ctx) {
    ++CurrentStep;
    DoNext(ctx);
}

void TInitializer::Done(const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE,  "Initializing topic '" << Partition->TopicName()
                                                << "' partition " << Partition->Partition
                                                << ". Completed.");
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

    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE,  "Initializing topic '" << Partition->TopicName()
                                                << "' partition " << Partition->Partition
                                                << ". Step " << CurrentStep->Get()->Name);
    CurrentStep->Get()->Execute(ctx);
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

ui32 TInitializerStep::PartitionId() const {
    return Initializer->Partition->Partition;
}

void TInitializerStep::PoisonPill(const TActorContext& ctx) {
    ctx.Send(Partition()->Tablet, new TEvents::TEvPoisonPill());
}

TString TInitializerStep::TopicName() const {
    return Partition()->TopicName();
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

    ctx.Send(Partition()->Tablet, event.Release());
}

void TInitConfigStep::Handle(TEvKeyValue::TEvResponse::TPtr& ev, const TActorContext& ctx) {
    if (!ValidateResponse(*this, ev, ctx)) {
        PoisonPill(ctx);
        return;
    }

    auto& res = ev->Get()->Record;
    Y_VERIFY(res.ReadResultSize() == 1);

    auto& response = res.GetReadResult(0);

    switch (response.GetStatus()) {
    case NKikimrProto::OK:
        Y_VERIFY(Partition()->Config.ParseFromString(response.GetValue()));
        if (Partition()->Config.GetVersion() < Partition()->TabletConfig.GetVersion()) {
            auto event = MakeHolder<TEvPQ::TEvChangePartitionConfig>(Partition()->TopicConverter,
                                                                     Partition()->TabletConfig);
            Partition()->PushFrontDistrTx(event.Release());
        }
        break;

    case NKikimrProto::NODATA:
        Partition()->Config = Partition()->TabletConfig;
        break;

    case NKikimrProto::ERROR:
        LOG_ERROR_S(ctx, NKikimrServices::PERSQUEUE,
                    "Partition " << Partition()->Partition << " can't read config");
        PoisonPill(ctx);
        return;

    default:
        Cerr << "ERROR " << response.GetStatus() << "\n";
        Y_FAIL("bad status");
    };

    Done(ctx);
}


//
// TInitInternalFieldsStep
//
TInitInternalFieldsStep::TInitInternalFieldsStep(TInitializer* initializer)
    : TInitializerStep(initializer, "TInitializerStep", false) {
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

    ctx.Send(Partition()->Tablet, request.Release());
}

void TInitDiskStatusStep::Handle(TEvKeyValue::TEvResponse::TPtr& ev, const TActorContext& ctx) {
    if (!ValidateResponse(*this, ev, ctx)) {
        PoisonPill(ctx);
        return;
    }

    auto& response = ev->Get()->Record;
    Y_VERIFY(response.GetStatusResultSize());

    Partition()->DiskIsFull = DiskIsFull(ev);
    if (!Partition()->DiskIsFull) {
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
    auto addKey = [](NKikimrClient::TKeyValueRequest& request, TKeyPrefix::EType type, ui32 partition) {
        auto read = request.AddCmdRead();
        TKeyPrefix key{type, partition};
        read->SetKey(key.Data(), key.Size());
    };

    THolder<TEvKeyValue::TEvRequest> request(new TEvKeyValue::TEvRequest);

    addKey(request->Record, TKeyPrefix::TypeMeta, PartitionId());
    addKey(request->Record, TKeyPrefix::TypeTxMeta, PartitionId());

    ctx.Send(Partition()->Tablet, request.Release());
}

void TInitMetaStep::Handle(TEvKeyValue::TEvResponse::TPtr &ev, const TActorContext &ctx) {
    if (!ValidateResponse(*this, ev, ctx)) {
        PoisonPill(ctx);
        return;
    }

    auto& response = ev->Get()->Record;
    Y_VERIFY(response.ReadResultSize() == 2);

    auto handleReadResult = [&](const NKikimrClient::TKeyValueResponse::TReadResult& response, auto&& action) {
        switch (response.GetStatus()) {
        case NKikimrProto::OK:
            action(response);
            break;
        case NKikimrProto::NODATA:
            break;
        case NKikimrProto::ERROR:
            LOG_ERROR_S(
                    ctx, NKikimrServices::PERSQUEUE,
                    "read topic '" << TopicName() << "' partition " << PartitionId() << " error"
            );
            PoisonPill(ctx);
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
        Partition()->SubDomainOutOfSpace = meta.GetSubDomainOutOfSpace();
    };
    handleReadResult(response.GetReadResult(0), loadMeta);

    auto loadTxMeta = [this](const NKikimrClient::TKeyValueResponse::TReadResult& response) {
        NKikimrPQ::TPartitionTxMeta meta;
        bool res = meta.ParseFromString(response.GetValue());
        Y_VERIFY(res);

        if (meta.HasPlanStep()) {
            Partition()->PlanStep = meta.GetPlanStep();
        }
        if (meta.HasTxId()) {
            Partition()->TxId = meta.GetTxId();
        }
    };
    handleReadResult(response.GetReadResult(1), loadTxMeta);

    Done(ctx);
}


//
// TInitInfoRangeStep
//

TInitInfoRangeStep::TInitInfoRangeStep(TInitializer* initializer)
    : TBaseKVStep(initializer, "TInitInfoRangeStep", true) {
}

void TInitInfoRangeStep::Execute(const TActorContext &ctx) {
    RequestInfoRange(ctx, Partition()->Tablet, PartitionId(), "");
}

void TInitInfoRangeStep::Handle(TEvKeyValue::TEvResponse::TPtr &ev, const TActorContext &ctx) {
    if (!ValidateResponse(*this, ev, ctx)) {
        PoisonPill(ctx);
        return;
    }

    auto& response = ev->Get()->Record;
    Y_VERIFY(response.ReadRangeResultSize() == 1);

    auto& range = response.GetReadRangeResult(0);
    auto now = ctx.Now();

    Y_VERIFY(response.ReadRangeResultSize() == 1);
    //megaqc check here all results
    Y_VERIFY(range.HasStatus());
    const TString *key = nullptr;
    switch (range.GetStatus()) {
        case NKikimrProto::OK:
        case NKikimrProto::OVERRUN: {
            auto& sourceIdStorage = Partition()->SourceIdStorage;
            auto& usersInfoStorage = Partition()->UsersInfoStorage;

            for (ui32 i = 0; i < range.PairSize(); ++i) {
                const auto& pair = range.GetPair(i);
                Y_VERIFY(pair.HasStatus());
                if (pair.GetStatus() != NKikimrProto::OK) {
                    LOG_ERROR_S(
                            ctx, NKikimrServices::PERSQUEUE,
                            "read range error topic '" << TopicName() << "' partition " << PartitionId()
                                << " got status " << pair.GetStatus() << " for key " << (pair.HasKey() ? pair.GetKey() : "unknown")
                    );

                    PoisonPill(ctx);
                    return;
                }

                Y_VERIFY(pair.HasKey());
                Y_VERIFY(pair.HasValue());

                key = &pair.GetKey();
                const auto type = (*key)[TKeyPrefix::MarkPosition()];
                if (type == TKeyPrefix::MarkSourceId) {
                    sourceIdStorage.LoadSourceIdInfo(*key, pair.GetValue(), now);
                } else if (type == TKeyPrefix::MarkProtoSourceId) {
                    sourceIdStorage.LoadSourceIdInfo(*key, pair.GetValue(), now);
                } else if (type == TKeyPrefix::MarkUser) {
                    usersInfoStorage->Parse(*key, pair.GetValue(), ctx);
                } else if (type == TKeyPrefix::MarkUserDeprecated) {
                    usersInfoStorage->ParseDeprecated(*key, pair.GetValue(), ctx);
                }
            }
            //make next step
            if (range.GetStatus() == NKikimrProto::OVERRUN) {
                Y_VERIFY(key);
                RequestInfoRange(ctx, Partition()->Tablet, PartitionId(), *key);
            } else {
                Done(ctx);
            }
            break;
        }
        case NKikimrProto::NODATA:
            Done(ctx);
            break;
        case NKikimrProto::ERROR:
            LOG_ERROR_S(
                    ctx, NKikimrServices::PERSQUEUE,
                    "read topic '" << TopicName() << "' partition " << PartitionId() << " error"
            );
            PoisonPill(ctx);
            break;
        default:
            Cerr << "ERROR " << range.GetStatus() << "\n";
            Y_FAIL("bad status");
    };
}


//
// TInitDataRangeStep
//

TInitDataRangeStep::TInitDataRangeStep(TInitializer* initializer)
    : TBaseKVStep(initializer, "TInitDataRangeStep", true) {
}

void TInitDataRangeStep::Execute(const TActorContext &ctx) {
    RequestDataRange(ctx, Partition()->Tablet, PartitionId(), "");
}

void TInitDataRangeStep::Handle(TEvKeyValue::TEvResponse::TPtr &ev, const TActorContext &ctx) {
    if (!ValidateResponse(*this, ev, ctx)) {
        PoisonPill(ctx);
        return;
    }

    auto& response = ev->Get()->Record;
    Y_VERIFY(response.ReadRangeResultSize() == 1);

    auto& range = response.GetReadRangeResult(0);

    Y_VERIFY(range.HasStatus());
    switch(range.GetStatus()) {
        case NKikimrProto::OK:
        case NKikimrProto::OVERRUN:

            FillBlobsMetaData(range, ctx);

            if (range.GetStatus() == NKikimrProto::OVERRUN) { //request rest of range
                Y_VERIFY(range.PairSize());
                RequestDataRange(ctx, Partition()->Tablet, PartitionId(), range.GetPair(range.PairSize() - 1).GetKey());
                return;
            }
            FormHeadAndProceed();

            Done(ctx);
            break;
        case NKikimrProto::NODATA:
            Done(ctx);
            break;
        default:
            Cerr << "ERROR " << range.GetStatus() << "\n";
            Y_FAIL("bad status");
    };
}

void TInitDataRangeStep::FillBlobsMetaData(const NKikimrClient::TKeyValueResponse::TReadRangeResult& range, const TActorContext& ctx) {
    auto& endOffset = Partition()->EndOffset;
    auto& startOffset = Partition()->StartOffset;
    auto& head = Partition()->Head;
    auto& dataKeysBody = Partition()->DataKeysBody;
    auto& gapOffsets = Partition()->GapOffsets;
    auto& gapSize = Partition()->GapSize;
    auto& bodySize = Partition()->BodySize;

    for (ui32 i = 0; i < range.PairSize(); ++i) {
        auto pair = range.GetPair(i);
        Y_VERIFY(pair.GetStatus() == NKikimrProto::OK); //this is readrange without keys, only OK could be here
        TKey k(pair.GetKey());
        if (dataKeysBody.empty()) { //no data - this is first pair of first range
            head.Offset = endOffset = startOffset = k.GetOffset();
            if (k.GetPartNo() > 0) ++startOffset;
            head.PartNo = 0;
        } else {
            Y_VERIFY(endOffset <= k.GetOffset(), "%s", pair.GetKey().c_str());
            if (endOffset < k.GetOffset()) {
                gapOffsets.push_back(std::make_pair(endOffset, k.GetOffset()));
                gapSize += k.GetOffset() - endOffset;
            }
        }
        Y_VERIFY(k.GetCount() + k.GetInternalPartsCount() > 0);
        Y_VERIFY(k.GetOffset() >= endOffset);
        endOffset = k.GetOffset() + k.GetCount();
        //at this point EndOffset > StartOffset
        if (!k.IsHead()) //head.Size will be filled after read or head blobs
            bodySize += pair.GetValueSize();

        LOG_DEBUG_S(
                ctx, NKikimrServices::PERSQUEUE,
                "Got data topic " << TopicName() << " partition " << k.GetPartition()
                    << " offset " << k.GetOffset() << " count " << k.GetCount() << " size " << pair.GetValueSize()
                    << " so " << startOffset << " eo " << endOffset << " " << pair.GetKey()
        );
        dataKeysBody.push_back({k, pair.GetValueSize(),
                        TInstant::Seconds(pair.GetCreationUnixTime()),
                        dataKeysBody.empty() ? 0 : dataKeysBody.back().CumulativeSize + dataKeysBody.back().Size});
    }

    Y_VERIFY(endOffset >= startOffset);
}


void TInitDataRangeStep::FormHeadAndProceed() {
    auto& endOffset = Partition()->EndOffset;
    auto& startOffset = Partition()->StartOffset;
    auto& head = Partition()->Head;
    auto& headKeys = Partition()->HeadKeys;
    auto& dataKeysBody = Partition()->DataKeysBody;

    head.Offset = endOffset;
    head.PartNo = 0;

    while (dataKeysBody.size() > 0 && dataKeysBody.back().Key.IsHead()) {
        Y_VERIFY(dataKeysBody.back().Key.GetOffset() + dataKeysBody.back().Key.GetCount() == head.Offset); //no gaps in head allowed
        headKeys.push_front(dataKeysBody.back());
        head.Offset = dataKeysBody.back().Key.GetOffset();
        head.PartNo = dataKeysBody.back().Key.GetPartNo();
        dataKeysBody.pop_back();
    }
    for (const auto& p : dataKeysBody) {
        Y_VERIFY(!p.Key.IsHead());
    }

    Y_VERIFY(headKeys.empty() || head.Offset == headKeys.front().Key.GetOffset() && head.PartNo == headKeys.front().Key.GetPartNo());
    Y_VERIFY(head.Offset < endOffset || head.Offset == endOffset && headKeys.empty());
    Y_VERIFY(head.Offset >= startOffset || head.Offset == startOffset - 1 && head.PartNo > 0);
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
    for (auto& p : Partition()->HeadKeys) {
        keys.push_back({p.Key.Data(), p.Key.Size()});
    }
    Y_VERIFY(keys.size() < Partition()->TotalMaxCount);
    if (keys.empty()) {
        Done(ctx);
        return;
    }

    THolder<TEvKeyValue::TEvRequest> request(new TEvKeyValue::TEvRequest);
    for (auto& key: keys) {
        auto read = request->Record.AddCmdRead();
        read->SetKey(key);
    }
    ctx.Send(Partition()->Tablet, request.Release());
}

void TInitDataStep::Handle(TEvKeyValue::TEvResponse::TPtr &ev, const TActorContext &ctx) {
    if (!ValidateResponse(*this, ev, ctx)) {
        PoisonPill(ctx);
        return;
    }

    auto& response = ev->Get()->Record;
    Y_VERIFY(response.ReadResultSize());

    auto& head = Partition()->Head;
    auto& headKeys = Partition()->HeadKeys;
    auto& dataKeysHead = Partition()->DataKeysHead;
    auto& compactLevelBorder = Partition()->CompactLevelBorder;
    auto totalLevels = Partition()->TotalLevels;

    ui32 currentLevel = 0;
    Y_VERIFY(headKeys.size() == response.ReadResultSize());
    for (ui32 i = 0; i < response.ReadResultSize(); ++i) {
        auto& read = response.GetReadResult(i);
        Y_VERIFY(read.HasStatus());
        switch(read.GetStatus()) {
            case NKikimrProto::OK: {
                const TKey& key = headKeys[i].Key;
                Y_VERIFY(key.IsHead());

                ui32 size = headKeys[i].Size;
                ui64 offset = key.GetOffset();
                while (currentLevel + 1 < totalLevels && size < compactLevelBorder[currentLevel + 1])
                    ++currentLevel;
                Y_VERIFY(size < compactLevelBorder[currentLevel]);

                dataKeysHead[currentLevel].AddKey(key, size);
                Y_VERIFY(dataKeysHead[currentLevel].KeysCount() < AppData(ctx)->PQConfig.GetMaxBlobsPerLevel());
                Y_VERIFY(!dataKeysHead[currentLevel].NeedCompaction());

                LOG_DEBUG_S(
                        ctx, NKikimrServices::PERSQUEUE,
                        "read res partition topic '" << TopicName()
                            << "' parititon " << key.GetPartition() << " offset " << offset << " endOffset " << Partition()->EndOffset
                            << " key " << key.GetOffset() << "," << key.GetCount() << " valuesize " << read.GetValue().size()
                            << " expected " << size
                );

                Y_VERIFY(offset + 1 >= Partition()->StartOffset);
                Y_VERIFY(offset < Partition()->EndOffset);
                Y_VERIFY(size == read.GetValue().size());

                for (TBlobIterator it(key, read.GetValue()); it.IsValid(); it.Next()) {
                    head.Batches.emplace_back(it.GetBatch());
                }
                head.PackedSize += size;

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
                        "tablet " << Partition()->TabletID << " HandleOnInit topic '" << TopicName()
                            << "' partition " << PartitionId()
                            << " ReadResult " << i << " status NKikimrProto::ERROR result message: \"" << read.GetMessage()
                            << " \" errorReason: \"" << response.GetErrorReason() << "\""
                );
                PoisonPill(ctx);
                return;
            default:
                Cerr << "ERROR " << read.GetStatus() << " message: \"" << read.GetMessage() << "\"\n";
                Y_FAIL("bad status");

        };
    }

    Done(ctx);
}


//
// TPartition
//

void TPartition::Bootstrap(const TActorContext& ctx) {
    Become(&TThis::StateInit);
    Initializer.Execute(ctx);
}

void TPartition::Initialize(const TActorContext& ctx) {
    CreationTime = ctx.Now();
    WriteCycleStartTime = ctx.Now();
    WriteQuota.ConstructInPlace(Config.GetPartitionConfig().GetBurstSize(),
                                Config.GetPartitionConfig().GetWriteSpeedInBytesPerSecond(),
                                ctx.Now());
    ReadQuotaTrackerActor = Register(new TReadQuoter(
        ctx,
        SelfId(),
        TopicConverter,
        Config,
        Partition,
        Tablet,
        TabletID,
        Counters
    ));
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
                                      TopicConverter,
                                      Partition,
                                      Config,
                                      CloudId,
                                      DbId,
                                      Config.GetYdbDatabasePath(),
                                      IsServerless,
                                      FolderId);
    TotalChannelWritesByHead.resize(NumChannels);

    if (Config.GetPartitionConfig().HasMirrorFrom()) {
        ManageWriteTimestampEstimate = !Config.GetPartitionConfig().GetMirrorFrom().GetSyncWriteTime();
    } else {
        ManageWriteTimestampEstimate = IsLocalDC;
    }

    if (AppData()->PQConfig.GetTopicsAreFirstClassCitizen()) {
        PartitionCountersLabeled.Reset(new TPartitionLabeledCounters(EscapeBadChars(TopicName()),
                                                                     Partition,
                                                                     Config.GetYdbDatabasePath()));
    } else {
        PartitionCountersLabeled.Reset(new TPartitionLabeledCounters(TopicName(),
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

    LOG_INFO_S(ctx, NKikimrServices::PERSQUEUE, "bootstrapping " << Partition << " " << ctx.SelfID);

    if (AppData(ctx)->Counters) {
        if (AppData()->PQConfig.GetTopicsAreFirstClassCitizen()) {
            SetupStreamCounters(ctx);
        } else {
            SetupTopicCounters(ctx);
        }
    }
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


//
// Functions
//

bool ValidateResponse(const TInitializerStep& step, TEvKeyValue::TEvResponse::TPtr& ev, const TActorContext& ctx) {
    auto& response = ev->Get()->Record;
    if (response.GetStatus() != NMsgBusProxy::MSTATUS_OK) {
        LOG_ERROR_S(
                ctx, NKikimrServices::PERSQUEUE,
                "commands for topic '" << step.TopicName() << " partition " << step.PartitionId()
                << " are not processed at all, got KV error " << response.GetStatus()
        );
        return false;
    }

    for (ui32 i = 0; i < response.GetStatusResultSize(); ++i) {
        auto& res = response.GetGetStatusResult(i);
        if (res.GetStatus() != NKikimrProto::OK) {
            LOG_ERROR_S(
                    ctx, NKikimrServices::PERSQUEUE,
                    "commands for topic '" << step.TopicName() << "' partition " << step.PartitionId() <<
                    " are not processed at all, got KV error in CmdGetStatus " << res.GetStatus()
            );
            return false;
        }
    }

    return true;
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

bool DiskIsFull(TEvKeyValue::TEvResponse::TPtr& ev) {
    auto& response = ev->Get()->Record;

    bool diskIsOk = true;
    for (ui32 i = 0; i < response.GetStatusResultSize(); ++i) {
        auto& res = response.GetGetStatusResult(i);
        TStorageStatusFlags status = res.GetStatusFlags();
        diskIsOk = diskIsOk && !status.Check(NKikimrBlobStorage::StatusDiskSpaceLightYellowMove);
    }
    return !diskIsOk;
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

void RequestInfoRange(const TActorContext& ctx, const TActorId& dst, ui32 partition, const TString& key) {
    RequestRange(ctx, dst, partition, TKeyPrefix::TypeInfo, true, key, key == "");
}

void RequestDataRange(const TActorContext& ctx, const TActorId& dst, ui32 partition, const TString& key) {
    RequestRange(ctx, dst, partition, TKeyPrefix::TypeData, false, key);
}

} // namespace NKikimr::NPQ
