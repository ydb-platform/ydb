#include "keyvalue_state.h"
#include "keyvalue_data.h"
#include "keyvalue_storage_read_request.h"
#include "keyvalue_storage_request.h"
#include "keyvalue_trash_key_arbitrary.h"
#include <ydb/core/base/tablet.h>
#include <ydb/core/protos/counters_keyvalue.pb.h>
#include <ydb/core/protos/msgbus_kv.pb.h>
#include <ydb/core/protos/tablet.pb.h>
#include <ydb/core/mind/local.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>
#include <ydb/core/tablet/tablet_metrics.h>
#include <ydb/core/util/stlog.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/json/writer/json_value.h>
#include <util/string/escape.h>
#include <util/charset/utf8.h>

// Set to 1 in order for tablet to reboot instead of failing a Y_ABORT_UNLESS on database damage
#define KIKIMR_KEYVALUE_ALLOW_DAMAGE 0

namespace NKikimr {
namespace NKeyValue {

constexpr ui64 KeyValuePairSizeEstimation = 1 + 5 // Key id, length
    + 1 + 5 // Value id, length
    + 1 + 4 // ValueSize id, value
    + 1 + 8 // CreationUnixTime id, value
    + 1 + 1 // StorageChannel id, value
    + 1 + 1 // Status id, value
    ;

constexpr ui64 KeyValuePairSizeEstimationNewApi = 1 + 5 // Key id, length
    + 1 + 5 // Value id, length
    + 1 + 4 // ValueSize id, value
    + 1 + 8 // CreationUnixTime id, value
    + 1 + 4 // StorageChannel id, value
    + 1 + 1 // Status id, value
    ;

constexpr ui64 KeyInfoSizeEstimation = 1 + 5 // Key id, length
    + 1 + 4 // ValueSize id, value
    + 1 + 8 // CreationUnixTime id, value
    + 1 + 4 // StorageChannel id, value
    ;

constexpr ui64 ReadRangeRequestMetaDataSizeEstimation = 1 + 5 // pair id, length
    + 1 + 1 // Status id, value
    ;

constexpr ui64 ReadResultSizeEstimation = 1 + 1 // Status id, value
    + 1 + 5 // Value id, length OR Message id, length
    ;

constexpr ui64 ReadResultSizeEstimationNewApi = 1 + 5 // Key id, length
    + 1 + 5 // Value id, length
    + 1 + 8 // Offset id, value
    + 1 + 8 // Size id, value
    + 1 + 1 // Status id, value
    ;

constexpr ui64 ErrorMessageSizeEstimation = 128;

constexpr size_t MaxKeySize = 4000;

bool IsKeyLengthValid(const TString& key) {
    return key.length() <= MaxKeySize;
}

// Guideline:
// Check SetError calls: there must be no changes made to the DB before SetError call (!)

TKeyValueState::TKeyValueState() {
    TabletCounters = nullptr;
    Clear();
}

void TKeyValueState::Clear() {
    IsStatePresent = false;
    IsEmptyDbStart = true;
    IsDamaged = false;
    IsTabletYellowStop = false;
    IsTabletYellowMove = false;

    StoredState.Clear();
    NextLogoBlobStep = 1;
    NextLogoBlobCookie = 1;
    Index.clear();
    RefCounts.clear();
    Trash.clear();
    InFlightForStep.clear();
    CollectOperation.Reset(nullptr);
    IsCollectEventSent = false;
    ChannelDataUsage.fill(0);
    UsedChannels.reset();

    TabletId = 0;
    KeyValueActorId = TActorId();
    ExecutorGeneration = 0;

    Queue.clear();
    IntermediatesInFlight = 0;
    IntermediatesInFlightLimit = 3; // FIXME: Change to something like 10
    RoInlineIntermediatesInFlight = 0;
    DeletesPerRequestLimit = 100'000;

    PerGenerationCounter = 0;

    if (TabletCounters) {
        ClearTabletCounters();
        CountStarting();
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//
// Tablet Counters
//
void TKeyValueState::SetupTabletCounters(TAutoPtr<TTabletCountersBase> counters) {
    TabletCountersPtr = counters;
    TabletCounters = TabletCountersPtr.Get();
}

void TKeyValueState::ClearTabletCounters() {
    for (ui32 i = 0; i < TabletCounters->Simple().Size(); ++i) {
       TabletCounters->Simple()[i].Set(0);
    }
}

TAutoPtr<TTabletCountersBase> TKeyValueState::TakeTabletCounters() {
    return TabletCountersPtr;
}

TTabletCountersBase& TKeyValueState::GetTabletCounters() {
    return *TabletCounters;
}

void TKeyValueState::SetupResourceMetrics(NMetrics::TResourceMetrics* resourceMetrics) {
    ResourceMetrics = resourceMetrics;
}

void TKeyValueState::CountRequestComplete(NMsgBusProxy::EResponseStatus status,
        const TRequestStat &stat, const TActorContext &ctx)
{
    ui64 fullLatencyMs = (TAppData::TimeProvider->Now() - stat.IntermediateCreatedAt).MilliSeconds();
    if (stat.RequestType == TRequestType::WriteOnly) {
        TabletCounters->Percentile()[COUNTER_LATENCY_FULL_WO].IncrementFor(fullLatencyMs);
        TabletCounters->Simple()[COUNTER_REQ_WO_IN_FLY].Add((ui64)-1);
        if (status == NMsgBusProxy::MSTATUS_OK) {
            TabletCounters->Cumulative()[COUNTER_REQ_WO_OK].Increment(1);
        } else if (status == NMsgBusProxy::MSTATUS_TIMEOUT) {
            TabletCounters->Cumulative()[COUNTER_REQ_WO_TIMEOUT].Increment(1);
        } else {
            TabletCounters->Cumulative()[COUNTER_REQ_WO_OTHER_ERROR].Increment(1);
        }
    } else {
        if (stat.RequestType == TRequestType::ReadOnlyInline) {
            TabletCounters->Simple()[COUNTER_REQ_RO_INLINE_IN_FLY].Set(RoInlineIntermediatesInFlight);
        } else {
            TabletCounters->Simple()[COUNTER_REQ_RO_RW_IN_FLY].Set(IntermediatesInFlight);
        }
        if (stat.RequestType == TRequestType::ReadOnlyInline || stat.RequestType == TRequestType::ReadOnly) {
            if (stat.RequestType == TRequestType::ReadOnlyInline) {
                TabletCounters->Percentile()[COUNTER_LATENCY_FULL_RO_INLINE].IncrementFor(fullLatencyMs);
            } else {
                TabletCounters->Percentile()[COUNTER_LATENCY_FULL_RO].IncrementFor(fullLatencyMs);
            }
            if (status == NMsgBusProxy::MSTATUS_OK) {
                TabletCounters->Cumulative()[COUNTER_REQ_RO_OK].Increment(1);
            } else if (status == NMsgBusProxy::MSTATUS_TIMEOUT) {
                TabletCounters->Cumulative()[COUNTER_REQ_RO_TIMEOUT].Increment(1);
            } else {
                TabletCounters->Cumulative()[COUNTER_REQ_RO_OTHER_ERROR].Increment(1);
            }
        } else {
            TabletCounters->Percentile()[COUNTER_LATENCY_FULL_RW].IncrementFor(fullLatencyMs);
            if (status == NMsgBusProxy::MSTATUS_OK) {
                TabletCounters->Cumulative()[COUNTER_REQ_RW_OK].Increment(1);
            } else if (status == NMsgBusProxy::MSTATUS_TIMEOUT) {
                TabletCounters->Cumulative()[COUNTER_REQ_RW_TIMEOUT].Increment(1);
            } else {
                TabletCounters->Cumulative()[COUNTER_REQ_RW_OTHER_ERROR].Increment(1);
            }
        }
    }

    TInstant now(ctx.Now());
    ResourceMetrics->Network.Increment(stat.ReadBytes + stat.RangeReadBytes + stat.WriteBytes, now);

    constexpr ui64 MaxStatChannels = 16;
    for (const auto& pr : stat.GroupReadBytes) {
        ResourceMetrics->ReadThroughput[pr.first].Increment(pr.second, now);
        NMetrics::TChannel channel = pr.first.first;
        if (channel >= MaxStatChannels) {
            channel = MaxStatChannels - 1;
        }
        TabletCounters->Cumulative()[COUNTER_READ_BYTES_CHANNEL_0 + channel].Increment(pr.second);
    }
    for (const auto& pr : stat.GroupWrittenBytes) {
        ResourceMetrics->WriteThroughput[pr.first].Increment(pr.second, now);
        NMetrics::TChannel channel = pr.first.first;
        if (channel >= MaxStatChannels) {
            channel = MaxStatChannels - 1;
        }
        TabletCounters->Cumulative()[COUNTER_WRITE_BYTES_CHANNEL_0 + channel].Increment(pr.second);
    }
    for (const auto& pr : stat.GroupReadIops) {
        ResourceMetrics->ReadIops[pr.first].Increment(pr.second, now);
    }
    for (const auto& pr : stat.GroupWrittenIops) {
        ResourceMetrics->WriteIops[pr.first].Increment(pr.second, now);
    }

    if (status == NMsgBusProxy::MSTATUS_OK) {
        TabletCounters->Cumulative()[COUNTER_CMD_READ_BYTES_OK].Increment(stat.ReadBytes);
        TabletCounters->Cumulative()[COUNTER_CMD_READ_OK].Increment(stat.Reads);
        TabletCounters->Cumulative()[COUNTER_CMD_READ_NODATA].Increment(stat.ReadNodata);
        TabletCounters->Cumulative()[COUNTER_CMD_DATA_RANGE_READ_BYTES_OK].Increment(stat.RangeReadBytes);
        TabletCounters->Cumulative()[COUNTER_CMD_DATA_RANGE_READ_ITEMS_OK].Increment(stat.RangeReadItems);
        TabletCounters->Cumulative()[COUNTER_CMD_DATA_RANGE_READ_ITEMS_NODATA].Increment(stat.RangeReadItemsNodata);
        TabletCounters->Cumulative()[COUNTER_CMD_INDEX_RANGE_READ_OK].Increment(stat.IndexRangeRead);
        TabletCounters->Cumulative()[COUNTER_CMD_DELETE_OK].Increment(stat.Deletes);
        TabletCounters->Cumulative()[COUNTER_CMD_DELETE_BYTES_OK].Increment(stat.DeleteBytes);
        TabletCounters->Cumulative()[COUNTER_CMD_RENAME_OK].Increment(stat.Renames);
        TabletCounters->Cumulative()[COUNTER_CMD_WRITE_BYTES_OK].Increment(stat.WriteBytes);
        TabletCounters->Cumulative()[COUNTER_CMD_WRITE_OK].Increment(stat.Writes);
        TabletCounters->Cumulative()[COUNTER_CMD_COPY_RANGE_OK].Increment(stat.CopyRanges);
        TabletCounters->Cumulative()[COUNTER_CMD_GUM_OK].Increment(stat.Concats);
    } else if (status == NMsgBusProxy::MSTATUS_TIMEOUT) {
        TabletCounters->Cumulative()[COUNTER_CMD_READ_BYTES_TIMEOUT].Increment(stat.ReadBytes);
        TabletCounters->Cumulative()[COUNTER_CMD_READ_TIMEOUT].Increment(stat.Reads);
        TabletCounters->Cumulative()[COUNTER_CMD_DATA_RANGE_READ_BYTES_TIMEOUT].Increment(stat.RangeReadBytes);
        TabletCounters->Cumulative()[COUNTER_CMD_DATA_RANGE_READ_ITEMS_TIMEOUT].Increment(stat.RangeReadItems);
        TabletCounters->Cumulative()[COUNTER_CMD_INDEX_RANGE_READ_TIMEOUT].Increment(stat.IndexRangeRead);
        TabletCounters->Cumulative()[COUNTER_CMD_DELETE_TIMEOUT].Increment(stat.Deletes);
        TabletCounters->Cumulative()[COUNTER_CMD_DELETE_BYTES_TIMEOUT].Increment(stat.DeleteBytes);
        TabletCounters->Cumulative()[COUNTER_CMD_RENAME_TIMEOUT].Increment(stat.Renames);
        TabletCounters->Cumulative()[COUNTER_CMD_WRITE_BYTES_TIMEOUT].Increment(stat.WriteBytes);
        TabletCounters->Cumulative()[COUNTER_CMD_WRITE_TIMEOUT].Increment(stat.Writes);
        TabletCounters->Cumulative()[COUNTER_CMD_COPY_RANGE_TIMEOUT].Increment(stat.CopyRanges);
        TabletCounters->Cumulative()[COUNTER_CMD_GUM_TIMEOUT].Increment(stat.Concats);
    } else {
        TabletCounters->Cumulative()[COUNTER_CMD_READ_BYTES_OTHER_ERROR].Increment(stat.ReadBytes);
        TabletCounters->Cumulative()[COUNTER_CMD_READ_OTHER_ERROR].Increment(stat.Reads);
        TabletCounters->Cumulative()[COUNTER_CMD_DATA_RANGE_READ_BYTES_OTHER_ERROR].Increment(stat.RangeReadBytes);
        TabletCounters->Cumulative()[COUNTER_CMD_DATA_RANGE_READ_ITEMS_OTHER_ERROR].Increment(stat.RangeReadItems);
        TabletCounters->Cumulative()[COUNTER_CMD_INDEX_RANGE_READ_OTHER_ERROR].Increment(stat.IndexRangeRead);
        TabletCounters->Cumulative()[COUNTER_CMD_DELETE_OTHER_ERROR].Increment(stat.Deletes);
        TabletCounters->Cumulative()[COUNTER_CMD_DELETE_BYTES_OTHER_ERROR].Increment(stat.DeleteBytes);
        TabletCounters->Cumulative()[COUNTER_CMD_RENAME_OTHER_ERROR].Increment(stat.Renames);
        TabletCounters->Cumulative()[COUNTER_CMD_WRITE_BYTES_OTHER_ERROR].Increment(stat.WriteBytes);
        TabletCounters->Cumulative()[COUNTER_CMD_WRITE_OTHER_ERROR].Increment(stat.Writes);
        TabletCounters->Cumulative()[COUNTER_CMD_COPY_RANGE_OTHER_ERROR].Increment(stat.CopyRanges);
        TabletCounters->Cumulative()[COUNTER_CMD_GUM_OTHER_ERROR].Increment(stat.Concats);
    }

    for (const auto latency: stat.GetLatencies) {
        TabletCounters->Percentile()[COUNTER_LATENCY_BS_GET].IncrementFor(latency);
    }
    for (const auto latency: stat.PutLatencies) {
        TabletCounters->Percentile()[COUNTER_LATENCY_BS_PUT].IncrementFor(latency);
    }
}

void TKeyValueState::CountRequestTakeOffOrEnqueue(TRequestType::EType requestType) {
    if (requestType == TRequestType::WriteOnly) {
        TabletCounters->Simple()[COUNTER_REQ_WO_IN_FLY].Add(1);
    } else {
        if (requestType == TRequestType::ReadOnlyInline) {
            TabletCounters->Simple()[COUNTER_REQ_RO_INLINE_IN_FLY].Set(RoInlineIntermediatesInFlight);
        } else {
            TabletCounters->Simple()[COUNTER_REQ_RO_RW_IN_FLY].Set(IntermediatesInFlight);
        }
        TabletCounters->Simple()[COUNTER_REQ_RO_RW_QUEUED].Set(Queue.size());
    }
}

void TKeyValueState::CountRequestOtherError(TRequestType::EType requestType) {
    if (requestType == TRequestType::ReadOnly || requestType == TRequestType::ReadOnlyInline) {
        TabletCounters->Cumulative()[COUNTER_REQ_RO_OTHER_ERROR].Increment(1);
    } else if (requestType == TRequestType::WriteOnly) {
        TabletCounters->Cumulative()[COUNTER_REQ_WO_OTHER_ERROR].Increment(1);
    } else {
        TabletCounters->Cumulative()[COUNTER_REQ_RW_OTHER_ERROR].Increment(1);
    }
}

void TKeyValueState::CountRequestIncoming(TRequestType::EType requestType) {
    // CountRequestIncoming is called before the request type is fully determined
    // it's impossible to separate ReadOnly and ReadOnlyInline, both are actually ReadOnly here
    if (requestType == TRequestType::ReadOnly || requestType == TRequestType::ReadOnlyInline) {
        TabletCounters->Cumulative()[COUNTER_REQ_RO].Increment(1);
    } else if (requestType == TRequestType::WriteOnly) {
        TabletCounters->Cumulative()[COUNTER_REQ_WO].Increment(1);
    } else {
        TabletCounters->Cumulative()[COUNTER_REQ_RW].Increment(1);
    }
}

void TKeyValueState::CountTrashRecord(const TLogoBlobID& id) {
    CountUncommittedTrashRecord(id);
    TabletCounters->Simple()[COUNTER_RECORD_COUNT] -= 1;
    auto& bytes = TabletCounters->Simple()[COUNTER_RECORD_BYTES];
    bytes -= id.BlobSize();
    ResourceMetrics->StorageUser.Set(bytes.Get());
    Y_ABORT_UNLESS(id.Channel() < ChannelDataUsage.size());
    ChannelDataUsage[id.Channel()] -= id.BlobSize();
}

void TKeyValueState::CountWriteRecord(const TLogoBlobID& id) {
    TabletCounters->Simple()[COUNTER_RECORD_COUNT] += 1;
    TabletCounters->Simple()[COUNTER_RECORD_BYTES] += id.BlobSize();
    ResourceMetrics->StorageUser.Set(TabletCounters->Simple()[COUNTER_RECORD_BYTES].Get());
    Y_ABORT_UNLESS(id.Channel() < ChannelDataUsage.size());
    ChannelDataUsage[id.Channel()] += id.BlobSize();
}

void TKeyValueState::CountDeleteInline(ui32 sizeBytes) {
    TabletCounters->Simple()[COUNTER_RECORD_COUNT] -= 1;
    TabletCounters->Simple()[COUNTER_RECORD_BYTES] -= sizeBytes;
    ResourceMetrics->StorageUser.Set(TabletCounters->Simple()[COUNTER_RECORD_BYTES].Get());
    Y_ABORT_UNLESS(0 < ChannelDataUsage.size());
    ChannelDataUsage[0] -= sizeBytes;
}

void TKeyValueState::CountInitialTrashRecord(const TLogoBlobID& id) {
    TabletCounters->Simple()[COUNTER_TRASH_COUNT] += 1;
    TabletCounters->Simple()[COUNTER_TRASH_BYTES] += id.BlobSize();
    Y_DEBUG_ABORT_UNLESS(TabletCounters->Simple()[COUNTER_UNCOMMITTED_TRASH_BYTES].Get() +
        TabletCounters->Simple()[COUNTER_TRASH_BYTES].Get() +
        TabletCounters->Simple()[COUNTER_VIRTUAL_TRASH_BYTES].Get() == TotalTrashSize);
}

void TKeyValueState::CountUncommittedTrashRecord(const TLogoBlobID& id) {
    TabletCounters->Simple()[COUNTER_UNCOMMITTED_TRASH_COUNT] += 1;
    TabletCounters->Simple()[COUNTER_UNCOMMITTED_TRASH_BYTES] += id.BlobSize();
    Y_DEBUG_ABORT_UNLESS(TabletCounters->Simple()[COUNTER_UNCOMMITTED_TRASH_BYTES].Get() +
        TabletCounters->Simple()[COUNTER_TRASH_BYTES].Get() +
        TabletCounters->Simple()[COUNTER_VIRTUAL_TRASH_BYTES].Get() == TotalTrashSize);
}

void TKeyValueState::CountTrashCollected(const TLogoBlobID& id) {
    TabletCounters->Simple()[COUNTER_TRASH_COUNT] -= 1;
    TabletCounters->Simple()[COUNTER_TRASH_BYTES] -= id.BlobSize();
    TabletCounters->Simple()[COUNTER_VIRTUAL_TRASH_COUNT] += 1;
    TabletCounters->Simple()[COUNTER_VIRTUAL_TRASH_BYTES] += id.BlobSize();
    Y_DEBUG_ABORT_UNLESS(TabletCounters->Simple()[COUNTER_UNCOMMITTED_TRASH_BYTES].Get() +
        TabletCounters->Simple()[COUNTER_TRASH_BYTES].Get() +
        TabletCounters->Simple()[COUNTER_VIRTUAL_TRASH_BYTES].Get() == TotalTrashSize);
}

void TKeyValueState::CountTrashCommitted(const TLogoBlobID& id) {
    TabletCounters->Simple()[COUNTER_UNCOMMITTED_TRASH_COUNT] -= 1;
    TabletCounters->Simple()[COUNTER_UNCOMMITTED_TRASH_BYTES] -= id.BlobSize();
    TabletCounters->Simple()[COUNTER_TRASH_COUNT] += 1;
    TabletCounters->Simple()[COUNTER_TRASH_BYTES] += id.BlobSize();
    Y_DEBUG_ABORT_UNLESS(TabletCounters->Simple()[COUNTER_UNCOMMITTED_TRASH_BYTES].Get() +
        TabletCounters->Simple()[COUNTER_TRASH_BYTES].Get() +
        TabletCounters->Simple()[COUNTER_VIRTUAL_TRASH_BYTES].Get() == TotalTrashSize);
}

void TKeyValueState::CountTrashDeleted(const TLogoBlobID& id) {
    TabletCounters->Simple()[COUNTER_VIRTUAL_TRASH_COUNT] -= 1;
    TabletCounters->Simple()[COUNTER_VIRTUAL_TRASH_BYTES] -= id.BlobSize();
    Y_DEBUG_ABORT_UNLESS(TabletCounters->Simple()[COUNTER_UNCOMMITTED_TRASH_BYTES].Get() +
        TabletCounters->Simple()[COUNTER_TRASH_BYTES].Get() +
        TabletCounters->Simple()[COUNTER_VIRTUAL_TRASH_BYTES].Get() == TotalTrashSize);
}

void TKeyValueState::CountOverrun() {
    TabletCounters->Cumulative()[COUNTER_REQ_OVERRUN].Increment(1);
}

void TKeyValueState::CountLatencyBsOps(const TRequestStat &stat) {
    ui64 bsDuration = (TAppData::TimeProvider->Now() - stat.KeyvalueStorageRequestSentAt).MilliSeconds();
    TabletCounters->Percentile()[COUNTER_LATENCY_BS_OPS].IncrementFor(bsDuration);
}

void TKeyValueState::CountLatencyBsCollect() {
    ui64 collectDurationMs = (TAppData::TimeProvider->Now() - LastCollectStartedAt).MilliSeconds();
    TabletCounters->Percentile()[COUNTER_LATENCY_BS_COLLECT].IncrementFor(collectDurationMs);
}

void TKeyValueState::CountLatencyQueue(const TRequestStat &stat) {
    ui64 enqueuedMs = (TAppData::TimeProvider->Now() - stat.IntermediateCreatedAt).MilliSeconds();
    if (stat.RequestType == TRequestType::WriteOnly) {
        TabletCounters->Percentile()[COUNTER_LATENCY_QUEUE_WO].IncrementFor(enqueuedMs);
    } else {
        TabletCounters->Percentile()[COUNTER_LATENCY_QUEUE_RO_RW].IncrementFor(enqueuedMs);
    }
}

void TKeyValueState::CountLatencyLocalBase(const TIntermediate &intermediate) {
    ui64 localBaseTxDurationMs =
        (TAppData::TimeProvider->Now() - intermediate.Stat.LocalBaseTxCreatedAt).MilliSeconds();
    if (intermediate.Stat.RequestType == TRequestType::WriteOnly) {
        TabletCounters->Percentile()[COUNTER_LATENCY_LOCAL_BASE_WO].IncrementFor(localBaseTxDurationMs);
    } else if (intermediate.Stat.RequestType == TRequestType::ReadWrite) {
        TabletCounters->Percentile()[COUNTER_LATENCY_LOCAL_BASE_RW].IncrementFor(localBaseTxDurationMs);
    }
}

void TKeyValueState::CountStarting() {
    TabletCounters->Simple()[COUNTER_STATE_STARTING].Set(1);
    TabletCounters->Simple()[COUNTER_STATE_PROCESSING_INIT_QUEUE].Set(0);
    TabletCounters->Simple()[COUNTER_STATE_ONLINE].Set(0);
}

void TKeyValueState::CountProcessingInitQueue() {
    TabletCounters->Simple()[COUNTER_STATE_STARTING].Set(0);
    TabletCounters->Simple()[COUNTER_STATE_PROCESSING_INIT_QUEUE].Set(1);
    TabletCounters->Simple()[COUNTER_STATE_ONLINE].Set(0);
}

void TKeyValueState::CountOnline() {
    TabletCounters->Simple()[COUNTER_STATE_STARTING].Set(0);
    TabletCounters->Simple()[COUNTER_STATE_PROCESSING_INIT_QUEUE].Set(0);
    TabletCounters->Simple()[COUNTER_STATE_ONLINE].Set(1);
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//
// Initialization
//

void TKeyValueState::Terminate(const TActorContext& ctx) {
    ctx.Send(ChannelBalancerActorId, new TEvents::TEvPoisonPill);
}

void TKeyValueState::Load(const TString &key, const TString& value) {
    if (IsEmptyDbStart) {
        IsEmptyDbStart = false;
    }

    TString arbitraryPart;
    TKeyHeader header;
    bool isOk = THelpers::ExtractKeyParts(key, arbitraryPart, header);
    Y_ABORT_UNLESS(isOk);

    switch (header.ItemType) {
        case EIT_UNKNOWN: {
            Y_ABORT_UNLESS(false, "Unexpected EIT_UNKNOWN key header.");
            break;
        }
        case EIT_KEYVALUE_1:
        {
            TIndexRecord &record = Index[arbitraryPart];
            TString errorInfo;
            bool isOk = false;
            EItemType headerItemType = TIndexRecord::ReadItemType(value);
            if (headerItemType == EIT_KEYVALUE_1) {
                isOk = record.Deserialize1(value, errorInfo);
            } else {
                isOk = record.Deserialize2(value, errorInfo);
            }
            if (!isOk) {
                TStringStream str;
                str << " Tablet# " << TabletId;
                str << " KeyArbitraryPart# \"" << arbitraryPart << "\"";
                str << errorInfo;
                if (KIKIMR_KEYVALUE_ALLOW_DAMAGE) {
                    str << Endl;
                    Cerr << str.Str();
                    IsDamaged = true;
                } else {
                    Y_ABORT_UNLESS(false, "%s", str.Str().data());
                }
            }
            for (const TIndexRecord::TChainItem& item : record.Chain) {
                if (!item.IsInline()) {
                    const ui32 newRefCount = ++RefCounts[item.LogoBlobId];
                    if (newRefCount == 1) {
                        CountWriteRecord(item.LogoBlobId);
                    }
                } else {
                    CountWriteRecord(TLogoBlobID(0, 0, 0, 0, item.InlineData.size(), 0));
                }
            }
            break;
        }
        case EIT_TRASH: {
            Y_ABORT_UNLESS(value.size() == 0);
            Y_ABORT_UNLESS(arbitraryPart.size() == sizeof(TTrashKeyArbitrary));
            const TTrashKeyArbitrary *trashKey = (const TTrashKeyArbitrary *) arbitraryPart.data();
            Trash.insert(trashKey->LogoBlobId);
            TotalTrashSize += trashKey->LogoBlobId.BlobSize();
            CountInitialTrashRecord(trashKey->LogoBlobId);
            break;
        }
        case EIT_COLLECT: {
            // just ignore the record
            Y_ABORT_UNLESS(arbitraryPart.size() == 0);
            break;
        }
        case EIT_STATE: {
            Y_ABORT_UNLESS(!IsStatePresent);
            IsStatePresent = true;
            Y_ABORT_UNLESS(arbitraryPart.size() == 0);
            Y_ABORT_UNLESS(value.size() >= sizeof(TKeyValueStoredStateData));
            const TKeyValueStoredStateData *data = (const TKeyValueStoredStateData *) value.data();
            Y_ABORT_UNLESS(data->CheckChecksum());
            Y_ABORT_UNLESS(data->DataHeader.ItemType == EIT_STATE);
            StoredState = *data;
            break;
        }
        default: {
            Y_ABORT_UNLESS(false, "Unexcpected header.ItemType# %" PRIu32, (ui32)header.ItemType);
            break;
        }
    }
}

void TKeyValueState::InitExecute(ui64 tabletId, TActorId keyValueActorId, ui32 executorGeneration,
        ISimpleDb &db, const TActorContext &ctx, const TTabletStorageInfo *info) {
    Y_UNUSED(info);
    Y_ABORT_UNLESS(IsEmptyDbStart || IsStatePresent);
    TabletId = tabletId;
    KeyValueActorId = keyValueActorId;
    ExecutorGeneration = executorGeneration;
    if (IsDamaged) {
        return;
    }
    ui8 maxChannel = 0;
    for (const auto& channel : info->Channels) {
        const ui32 index = channel.Channel;
        Y_ABORT_UNLESS(index <= UsedChannels.size());
        if (index == 0 || index >= BLOB_CHANNEL) {
            UsedChannels[index] = true;
            maxChannel = Max<ui8>(maxChannel, index);
        }
    }
    ChannelBalancerActorId = ctx.Register(new TChannelBalancer(maxChannel + 1, KeyValueActorId));

    // Issue hard barriers
    using TGroupChannel = std::tuple<ui32, ui8>;
    {
        THashMap<TGroupChannel, THelpers::TGenerationStep> hardBarriers;
        for (const auto &kv : RefCounts) {
            // extract blob id and validate its channel
            const TLogoBlobID &id = kv.first;
            const ui8 channel = id.Channel();
            Y_ABORT_UNLESS(channel >= BLOB_CHANNEL);

            // create (generation, step) pair for current item and decrement it by one step as the barriers
            // are always inclusive
            const ui32 generation = id.Generation();
            const ui32 step = id.Step();
            THelpers::TGenerationStep current;
            if (step) {
                current = THelpers::TGenerationStep(generation, step - 1);
            } else if (generation) {
                current = THelpers::TGenerationStep(generation - 1, Max<ui32>());
            } else {
                Y_ABORT("incorrect BlobId: zero generation and step");
            }

            // update minimum barrier value for this channel/group
            const ui32 group = info->GroupFor(channel, generation);
            Y_ABORT_UNLESS(group != Max<ui32>());
            if (const auto [it, inserted] = hardBarriers.try_emplace(TGroupChannel(group, channel), current); !inserted) {
                it->second = Min(it->second, current);
            }
        }
        for (const auto &channel : info->Channels) {
            if (channel.Channel < BLOB_CHANNEL) {
                continue;
            }
            for (const auto &history : channel.History) { // fill in group-channel pairs without any data
                const TGroupChannel key(history.GroupID, channel.Channel);
                hardBarriers.try_emplace(key, THelpers::TGenerationStep(executorGeneration - 1, Max<ui32>()));
            }
        }
        for (const auto& [key, barrier] : hardBarriers) {
            const auto& [group, channel] = key;
            const auto& [generation, step] = barrier;
            auto ev = TEvBlobStorage::TEvCollectGarbage::CreateHardBarrier(info->TabletID, executorGeneration,
                PerGenerationCounter, channel, generation, step, TInstant::Max());
            ++PerGenerationCounter;
            ++InitialCollectsSent;
            SendToBSProxy(ctx, group, ev.Release(), (ui64)TKeyValueState::ECollectCookie::Hard);
        }
    }

    // Issue soft barriers
    // Mark fresh blobs (after previous collected GenStep) with keep flag and issue barrier at current GenStep
    // to remove all phantom blobs
    {
        THashMap<TGroupChannel, THolder<TVector<TLogoBlobID>>> keepForGroupChannel;
        const ui32 barrierGeneration = executorGeneration - 1;
        const ui32 barrierStep = Max<ui32>();
        const THelpers::TGenerationStep barrier(barrierGeneration, barrierStep);

        auto addBlobToKeep = [&] (const TLogoBlobID &id) {
            ui32 group = info->GroupFor(id);
            Y_ABORT_UNLESS(group != Max<ui32>(), "RefBlob# %s is mapped to an invalid group (-1)!",
                    id.ToString().c_str());
            TGroupChannel key(group, id.Channel());
            THolder<TVector<TLogoBlobID>> &ptr = keepForGroupChannel[key];
            if (!ptr) {
                ptr = MakeHolder<TVector<TLogoBlobID>>();
            }
            ptr->emplace_back(id);
        };

        const THelpers::TGenerationStep storedGenStep(StoredState.GetCollectGeneration(), StoredState.GetCollectStep());
        for (const auto& [id, _] : RefCounts) {
            Y_ABORT_UNLESS(id.Channel() >= BLOB_CHANNEL);
            const THelpers::TGenerationStep blobGenStep = THelpers::GenerationStep(id);
            Y_ABORT_UNLESS(blobGenStep <= barrier);
            if (storedGenStep < blobGenStep) { // otherwise Keep flag was already issued
                addBlobToKeep(id);
            }
        }

        for (const auto &channel : info->Channels) { // just issue barriers for all groups and channels
            if (channel.Channel < BLOB_CHANNEL) {
                continue;
            }
            for (const auto& history : channel.History) {
                keepForGroupChannel.try_emplace(TGroupChannel(history.GroupID, channel.Channel));
            }
        }

        for (auto& [key, keep] : keepForGroupChannel) {
            const auto& [group, channel] = key;
            auto ev = MakeHolder<TEvBlobStorage::TEvCollectGarbage>(info->TabletID, executorGeneration,
                    PerGenerationCounter, channel, true /*collect*/, barrierGeneration, barrierStep, keep.Release(),
                    nullptr /*doNotKeep*/, TInstant::Max(), true /*isMultiCollectAllowed*/, false /*hard*/);
            ++PerGenerationCounter;
            ++InitialCollectsSent;
            SendToBSProxy(ctx, group, ev.Release(), (ui64)TKeyValueState::ECollectCookie::SoftInitial);
        }
    }

    THelpers::DbEraseCollect(db, ctx);
    if (IsEmptyDbStart) {
        THelpers::DbUpdateState(StoredState, db, ctx);
    }

    // corner case, if no CollectGarbage events were sent
    if (InitialCollectsSent == 0) {
        InitWithoutCollect = true;
        RegisterInitialGCCompletionExecute(db, ctx);
    } else {
        InitWithoutCollect = false;
        IsCollectEventSent = true;
    }
}

void TKeyValueState::InitComplete(const TActorContext& ctx, const TTabletStorageInfo *info) {
    if (InitWithoutCollect) {
        RegisterInitialGCCompletionComplete(ctx, info);
    }
}

bool TKeyValueState::RegisterInitialCollectResult(const TActorContext &ctx, const TTabletStorageInfo *info) {
    LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletId
        << " InitialCollectsSent# " << InitialCollectsSent << " Marker# KV50");
    if (--InitialCollectsSent == 0) {
        SendCutHistory(ctx, info);
        return true;
    }
    return false;
}

void TKeyValueState::RegisterInitialGCCompletionExecute(ISimpleDb &db, const TActorContext &ctx) {
    Y_ABORT_UNLESS(ExecutorGeneration);
    StoredState.SetCollectGeneration(ExecutorGeneration - 1);
    StoredState.SetCollectStep(Max<ui32>());
    THelpers::DbUpdateState(StoredState, db, ctx);
}

void TKeyValueState::RegisterInitialGCCompletionComplete(const TActorContext &ctx, const TTabletStorageInfo *info) {
    IsCollectEventSent = false;
    ProcessPostponedTrims(ctx, info);
    PrepareCollectIfNeeded(ctx);
}

void TKeyValueState::SendCutHistory(const TActorContext &ctx, const TTabletStorageInfo *info) {
    LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletId
        << " SendCutHistory Marker# KV51");

    using THistoryItem = std::tuple<ui8, ui32>; // channel, fromGeneration
    THashSet<THistoryItem> uselessItems;
    for (const auto& channel : info->Channels) {
        if (channel.Channel < BLOB_CHANNEL) {
            continue;
        }
        for (size_t i = 0, n = channel.History.size() - 1; i < n; ++i) {
            const auto& history = channel.History[i];
            uselessItems.emplace(channel.Channel, history.FromGeneration);
        }
    }

    using TSeenGeneration = std::tuple<ui8, ui32>; // channel, generation
    THashSet<TSeenGeneration> seenGenerations;
    auto usedBlob = [&](const TLogoBlobID& id) {
        const ui8 channel = id.Channel();
        Y_ABORT_UNLESS(BLOB_CHANNEL <= channel && channel < info->Channels.size());
        if (const auto [_, inserted] = seenGenerations.emplace(channel, id.Generation()); inserted) {
            const auto& ch = info->Channels[channel];
            auto it = std::upper_bound(ch.History.begin(), ch.History.end(), id.Generation(),
                TTabletChannelInfo::THistoryEntry::TCmp());
            if (it == ch.History.end()) { // in latest history entry
                return;
            }
            --it;
            Y_ABORT_UNLESS(it->FromGeneration <= id.Generation());
            Y_ABORT_UNLESS(id.Generation() < std::next(it)->FromGeneration);
            uselessItems.erase(THistoryItem(channel, it->FromGeneration));
        }
    };
    for (const auto& [id, _] : RefCounts) {
        usedBlob(id);
    }
    for (const TLogoBlobID& id : Trash) {
        usedBlob(id);
    }

    for (const auto& [channel, fromGeneration] : uselessItems) {
        TAutoPtr<TEvTablet::TEvCutTabletHistory> ev(new TEvTablet::TEvCutTabletHistory);
        auto &record = ev->Record;
        record.SetTabletID(TabletId);
        record.SetChannel(channel);
        record.SetFromGeneration(fromGeneration);
        TActorId localActorId = MakeLocalID(ctx.SelfID.NodeId());
        ctx.Send(localActorId, ev.Release());
    }
}

void TKeyValueState::OnInitQueueEmpty(const TActorContext &ctx) {
    Y_UNUSED(ctx);
    CountOnline();
}

void TKeyValueState::OnStateWork(const TActorContext &ctx) {
    Y_UNUSED(ctx);
    CountProcessingInitQueue();
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//
// User Request Processing
//

void TKeyValueState::Step() {
    if (NextLogoBlobStep < Max<ui32>()) {
        NextLogoBlobCookie = 1;
        ++NextLogoBlobStep;
    } else {
        Y_ABORT("Max step reached!");
    }
}

TLogoBlobID TKeyValueState::AllocateLogoBlobId(ui32 size, ui32 storageChannelIdx, ui64 requestUid) {
    ui32 generation = ExecutorGeneration;
    TLogoBlobID id(TabletId, generation, NextLogoBlobStep, storageChannelIdx, size, NextLogoBlobCookie);
    if (NextLogoBlobCookie < TLogoBlobID::MaxCookie) {
        ++NextLogoBlobCookie;
    } else {
        Step();
    }
    Y_ABORT_UNLESS(!CollectOperation || THelpers::GenerationStep(id) >
        THelpers::TGenerationStep(CollectOperation->Header.GetCollectGeneration(), CollectOperation->Header.GetCollectStep()));
    ++InFlightForStep[id.Step()];
    ++RequestUidStepToCount[std::make_tuple(requestUid, id.Step())];
    return id;
}

TLogoBlobID TKeyValueState::AllocatePatchedLogoBlobId(ui32 size, ui32 storageChannelIdx, TLogoBlobID originalBlobId, ui64 requestUid) {
    ui32 generation = ExecutorGeneration;
    TLogoBlobID id;
    using TEvPatch = TEvBlobStorage::TEvPatch;
    do {
        id = TLogoBlobID(TabletId, generation, NextLogoBlobStep, storageChannelIdx, size, NextLogoBlobCookie);
        if (NextLogoBlobCookie < TLogoBlobID::MaxCookie) {
            NextLogoBlobCookie++;
        } else {
            Step();
        }
    } while (TEvPatch::BlobPlacementKind(id) != TEvPatch::BlobPlacementKind(originalBlobId));
    Y_ABORT_UNLESS(!CollectOperation || THelpers::GenerationStep(id) >
        THelpers::TGenerationStep(CollectOperation->Header.GetCollectGeneration(), CollectOperation->Header.GetCollectStep()));
    ++InFlightForStep[id.Step()];
    ++RequestUidStepToCount[std::make_tuple(requestUid, id.Step())];
    return id;
}

void TKeyValueState::RequestExecute(THolder<TIntermediate> &intermediate, ISimpleDb &db, const TActorContext &ctx,
        const TTabletStorageInfo *info) {
    if (IsDamaged) {
        return;
    }
    intermediate->Response.Clear();
    intermediate->Response.SetStatus(NMsgBusProxy::MSTATUS_UNKNOWN);
    if (intermediate->HasCookie) {
        intermediate->Response.SetCookie(intermediate->Cookie);
    }

    // Check the generation
    if (intermediate->HasGeneration) {
        if (intermediate->Generation != StoredState.GetUserGeneration()) {
            TStringStream str;
            str << "KeyValue# " << TabletId;
            str << " Generation mismatch! Requested# " << intermediate->Generation;
            str << " Actual# " << StoredState.GetUserGeneration();
            str << " Marker# KV17";
            LOG_INFO_S(ctx, NKikimrServices::KEYVALUE, str.Str());
            // All reads done
            intermediate->Response.SetStatus(NMsgBusProxy::MSTATUS_REJECTED);
            intermediate->Response.SetErrorReason(str.Str());
            DropRefCountsOnErrorInTx(std::move(intermediate->RefCountsIncr), db, ctx);
            return;
        }
    }

    // Process CmdIncrementGeneration()
    if (intermediate->HasIncrementGeneration) {

        bool IsOk = intermediate->Commands.size() == 0
            && intermediate->Deletes.size() == 0 && intermediate->RangeReads.size() == 0
            && intermediate->Reads.size() == 0 && intermediate->Renames.size() == 0
            && intermediate->Writes.size() == 0 && intermediate->GetStatuses.size() == 0;

        if (IsOk) {
            IncrementGeneration(intermediate, db, ctx);
            return;
        } else {
            TStringStream str;
            str << "KeyValue# " << TabletId;
            str << " CmdIncrementGeneration can't be grouped with any other Cmd!";
            str << " Commands# " << intermediate->Commands.size();
            str << " Deletes# " << intermediate->Deletes.size();
            str << " RangeReads# " << intermediate->RangeReads.size();
            str << " Reads# " << intermediate->Reads.size();
            str << " Renames# " << intermediate->Renames.size();
            str << " Writes# " << intermediate->Writes.size();
            str << " GetStatuses# " << intermediate->GetStatuses.size();
            str << " CopyRanges# " << intermediate->CopyRanges.size();
            LOG_INFO_S(ctx, NKikimrServices::KEYVALUE, str.Str());
            // All reads done
            intermediate->Response.SetStatus(NMsgBusProxy::MSTATUS_INTERNALERROR);
            intermediate->Response.SetErrorReason(str.Str());
            DropRefCountsOnErrorInTx(std::move(intermediate->RefCountsIncr), db, ctx);
            return;
        }
    }

    // TODO: Validate arguments

    ProcessCmds(intermediate, db, ctx, info);

}

void TKeyValueState::RequestComplete(THolder<TIntermediate> &intermediate, const TActorContext &ctx,
        const TTabletStorageInfo *info) {
    Reply(intermediate, ctx, info);
}

void TKeyValueState::DropRefCountsOnErrorInTx(std::deque<std::pair<TLogoBlobID, bool>>&& refCountsIncr, ISimpleDb& db,
        const TActorContext& ctx) {
    for (const auto& [logoBlobId, initial] : refCountsIncr) {
        Dereference(logoBlobId, db, ctx, initial);
    }
}

void TKeyValueState::DropRefCountsOnError(std::deque<std::pair<TLogoBlobID, bool>>& refCountsIncr, bool writesMade,
        const TActorContext& /*ctx*/) {
    auto pred = [&](const auto& item) {
        const auto& [logoBlobId, initial] = item;
        const auto it = RefCounts.find(logoBlobId);
        Y_ABORT_UNLESS(it != RefCounts.end() && it->second); // item must exist and have a refcount
        if (it->second != 1) { // just drop the reference, item kept alive
            --it->second;
        } else if (initial && !writesMade) { // this was just generated BlobId and no writes were possibly made
            RefCounts.erase(it);
        } else { // this item has to be removed inside tx by rotation to Trash -- it may have been written somehow
            return false;
        }
        return true;
    };

    refCountsIncr.erase(std::remove_if(refCountsIncr.begin(), refCountsIncr.end(), pred), refCountsIncr.end());
}

///////////////////////////////////////////////////////////////////////////////
// Request processing
//

void TKeyValueState::Reply(THolder<TIntermediate> &intermediate, const TActorContext &ctx,
        const TTabletStorageInfo *info) {
    if (!intermediate->IsReplied) {
        if (intermediate->EvType == TEvKeyValue::TEvRequest::EventType) {
            THolder<TEvKeyValue::TEvResponse> response(new TEvKeyValue::TEvResponse);
            response->Record = intermediate->Response;
            for (auto& item : intermediate->Payload) {
                response->AddPayload(std::move(item));
            }
            ResourceMetrics->Network.Increment(response->Record.ByteSize());
            ctx.Send(intermediate->RespondTo, response.Release());
        }
        if (intermediate->EvType == TEvKeyValue::TEvExecuteTransaction::EventType) {
            THolder<TEvKeyValue::TEvExecuteTransactionResponse> response(new TEvKeyValue::TEvExecuteTransactionResponse);
            response->Record = intermediate->ExecuteTransactionResponse;
            ResourceMetrics->Network.Increment(response->Record.ByteSize());
            if (intermediate->RespondTo.NodeId() != ctx.SelfID.NodeId()) {
                response->Record.set_node_id(ctx.SelfID.NodeId());
            }
            ctx.Send(intermediate->RespondTo, response.Release());
        }
        if (intermediate->EvType == TEvKeyValue::TEvGetStorageChannelStatus::EventType) {
            THolder<TEvKeyValue::TEvGetStorageChannelStatusResponse> response(new TEvKeyValue::TEvGetStorageChannelStatusResponse);
            response->Record = intermediate->GetStorageChannelStatusResponse;
            ResourceMetrics->Network.Increment(response->Record.ByteSize());
            if (intermediate->RespondTo.NodeId() != ctx.SelfID.NodeId()) {
                response->Record.set_node_id(ctx.SelfID.NodeId());
            }
            ctx.Send(intermediate->RespondTo, response.Release());
        }
        if (intermediate->EvType == TEvKeyValue::TEvAcquireLock::EventType) {
            THolder<TEvKeyValue::TEvAcquireLockResponse> response(new TEvKeyValue::TEvAcquireLockResponse);
            response->Record.set_lock_generation(StoredState.GetUserGeneration());
            response->Record.set_cookie(intermediate->Cookie);
            ResourceMetrics->Network.Increment(response->Record.ByteSize());
            if (intermediate->RespondTo.NodeId() != ctx.SelfID.NodeId()) {
                response->Record.set_node_id(ctx.SelfID.NodeId());
            }
            ctx.Send(intermediate->RespondTo, response.Release());
        }
        intermediate->IsReplied = true;
        intermediate->UpdateStat();

        CountLatencyLocalBase(*intermediate);

        OnRequestComplete(intermediate->RequestUid, intermediate->CreatedAtGeneration, intermediate->CreatedAtStep,
            ctx, info, (NMsgBusProxy::EResponseStatus)intermediate->Response.GetStatus(), intermediate->Stat);
    }
}

void TKeyValueState::ProcessCmd(TIntermediate::TRead &request,
        NKikimrClient::TKeyValueResponse::TReadResult *legacyResponse,
        NKikimrKeyValue::StorageChannel* /*response*/,
        ISimpleDb &/*db*/, const TActorContext &/*ctx*/, TRequestStat &/*stat*/, ui64 /*unixTime*/,
        TIntermediate *intermediate)
{
    NKikimrProto::EReplyStatus outStatus = request.CumulativeStatus();
    request.Status = outStatus;
    legacyResponse->SetStatus(outStatus);
    if (outStatus == NKikimrProto::OK) {
        TRope value = request.BuildRope();
        if (intermediate->UsePayloadInResponse) {
            legacyResponse->SetPayloadId(intermediate->Payload.size());
            intermediate->Payload.push_back(std::move(value));
        } else {
            const TContiguousSpan span(value.GetContiguousSpan());
            legacyResponse->SetValue(span.data(), span.size());
        }
    } else {
        legacyResponse->SetMessage(request.Message);
        if (outStatus == NKikimrProto::NODATA) {
            for (ui32 itemIdx = 0; itemIdx < request.ReadItems.size(); ++itemIdx) {
                TIntermediate::TRead::TReadItem &item = request.ReadItems[itemIdx];
                // Make sure the blob is not referenced anymore
                auto refCountIt = RefCounts.find(item.LogoBlobId);
                if (refCountIt != RefCounts.end()) {
                    TStringStream str;
                    str << "KeyValue# " << TabletId
                        << " CmdRead "
                        //<< " ReadIdx# " << i
                        << " key# " << EscapeC(request.Key)
                        << " ItemIdx# " << itemIdx
                        << " BlobId# " << item.LogoBlobId.ToString()
                        << " Status# " << NKikimrProto::EReplyStatus_Name(item.Status)
                        << " outStatus# " << NKikimrProto::EReplyStatus_Name(outStatus)
                        << " but blob has RefCount# " << refCountIt->second
                        << " ! KEYVALUE CONSISTENCY ERROR!"
                        << " Message# " << request.Message
                        << " Marker# KV46";
                    Y_ABORT_UNLESS(false, "%s", str.Str().c_str());
                }
            }
        }
    }
}

void TKeyValueState::ProcessCmd(TIntermediate::TRangeRead &request,
        NKikimrClient::TKeyValueResponse::TReadRangeResult *legacyResponse,
        NKikimrKeyValue::StorageChannel */*response*/,
        ISimpleDb &/*db*/, const TActorContext &/*ctx*/, TRequestStat &/*stat*/, ui64 /*unixTime*/,
        TIntermediate *intermediate)
{
    for (ui64 r = 0; r < request.Reads.size(); ++r) {
        auto &read = request.Reads[r];
        auto *resultKv = legacyResponse->AddPair();

        NKikimrProto::EReplyStatus outStatus = read.CumulativeStatus();
        read.Status = outStatus;
        if (outStatus != NKikimrProto::OK && outStatus != NKikimrProto::OVERRUN) {
            // LOG_ERROR_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletId << " CmdReadRange " << r
            //    << " status " << NKikimrProto::EReplyStatus_Name(outStatus)
            //    << " message " << read.Message
            //    << " key " << EscapeC(read.Key));

            if (outStatus == NKikimrProto::NODATA) {
                for (ui32 itemIdx = 0; itemIdx < read.ReadItems.size(); ++itemIdx) {
                    TIntermediate::TRead::TReadItem &item = read.ReadItems[itemIdx];
                    // Make sure the blob is not referenced anymore
                    auto refCountIt = RefCounts.find(item.LogoBlobId);
                    if (refCountIt != RefCounts.end()) {
                        TStringStream str;
                        str << "KeyValue# " << TabletId
                            << " CmdReadRange "
                            // << " RangeReadIdx# " << i
                            << " ReadIdx# " << r
                            << " ItemIdx# " << itemIdx
                            << " key# " << EscapeC(read.Key)
                            << " BlobId# " << item.LogoBlobId.ToString()
                            << " Status# " << NKikimrProto::EReplyStatus_Name(item.Status)
                            << " outStatus# " << NKikimrProto::EReplyStatus_Name(outStatus)
                            << " but blob has RefCount# " << refCountIt->second
                            << " ! KEYVALUE CONSISTENCY ERROR!"
                            << " Message# " << read.Message
                            << " Marker# KV47";
                        Y_ABORT_UNLESS(false, "%s", str.Str().c_str());
                    }
                }
            }
        }

        resultKv->SetStatus(outStatus);
        resultKv->SetKey(read.Key);
        if (request.IncludeData && (outStatus == NKikimrProto::OK || outStatus == NKikimrProto::OVERRUN)) {
            TRope value = read.BuildRope();
            if (intermediate->UsePayloadInResponse) {
                resultKv->SetPayloadId(intermediate->Payload.size());
                intermediate->Payload.push_back(std::move(value));
            } else {
                const TContiguousSpan span = value.GetContiguousSpan();
                resultKv->SetValue(span.data(), span.size());
            }
        }
        resultKv->SetValueSize(read.ValueSize);
        resultKv->SetCreationUnixTime(read.CreationUnixTime);
        resultKv->SetStorageChannel(read.StorageChannel);
    }

    legacyResponse->SetStatus(request.Status);
}


NKikimrKeyValue::StorageChannel::StatusFlag GetStatusFlag(const TStorageStatusFlags &statusFlags) {
    if (statusFlags.Check(NKikimrBlobStorage::StatusDiskSpaceOrange)) {
        return NKikimrKeyValue::StorageChannel::STATUS_FLAG_ORANGE_OUT_SPACE;
    }
    if (statusFlags.Check(NKikimrBlobStorage::StatusDiskSpaceOrange)) {
        return NKikimrKeyValue::StorageChannel::STATUS_FLAG_YELLOW_STOP;
    }
    return NKikimrKeyValue::StorageChannel::STATUS_FLAG_GREEN;
}

void TKeyValueState::ProcessCmd(TIntermediate::TWrite &request,
        NKikimrClient::TKeyValueResponse::TWriteResult *legacyResponse,
        NKikimrKeyValue::StorageChannel *response,
        ISimpleDb &db, const TActorContext &ctx, TRequestStat &/*stat*/, ui64 unixTime,
        TIntermediate* /*intermediate*/)
{
    TIndexRecord& record = Index[request.Key];
    Dereference(record, db, ctx);

    record.Chain = {};
    ui32 storage_channel = 0;
    if (request.Status == NKikimrProto::SCHEDULED) {
        TRope inlineData = request.Data;
        const size_t size = inlineData.size();
        record.Chain.push_back(TIndexRecord::TChainItem(std::move(inlineData), 0));
        CountWriteRecord(TLogoBlobID(0, 0, 0, 0, size, 0));
        request.Status = NKikimrProto::OK;
        storage_channel = InlineStorageChannelInPublicApi;
    } else {
        int channel = -1;

        ui64 offset = 0;
        for (const TLogoBlobID& logoBlobId : request.LogoBlobIds) {
            record.Chain.push_back(TIndexRecord::TChainItem(logoBlobId, offset));
            offset += logoBlobId.BlobSize();
            CountWriteRecord(logoBlobId);
            if (channel == -1) {
                channel = logoBlobId.Channel();
            } else {
                // all blobs from the same write must be within the same channel
                Y_ABORT_UNLESS(channel == (int)logoBlobId.Channel());
            }
        }
        storage_channel = channel + MainStorageChannelInPublicApi;

        ctx.Send(ChannelBalancerActorId, new TChannelBalancer::TEvReportWriteLatency(channel, request.Latency));
    }

    record.CreationUnixTime = unixTime;
    UpdateKeyValue(request.Key, record, db, ctx);

    if (legacyResponse) {
        legacyResponse->SetStatus(NKikimrProto::OK);
        legacyResponse->SetStatusFlags(request.StatusFlags.Raw);
    }
    if (response) {
        response->set_status(NKikimrKeyValue::Statuses::RSTATUS_OK);
        response->set_status_flag(GetStatusFlag(request.StatusFlags));
        response->set_storage_channel(storage_channel);
    }
}

void TKeyValueState::ProcessCmd(TIntermediate::TPatch &request,
        NKikimrClient::TKeyValueResponse::TPatchResult *legacyResponse,
        NKikimrKeyValue::StorageChannel *response,
        ISimpleDb &db, const TActorContext &ctx, TRequestStat &/*stat*/, ui64 unixTime,
        TIntermediate* /*intermediate*/)
{
    TIndexRecord& record = Index[request.PatchedKey];
    Dereference(record, db, ctx);

    record.Chain = {};

    record.Chain.push_back(TIndexRecord::TChainItem(request.PatchedBlobId, 0));
    CountWriteRecord(request.PatchedBlobId); // TODO(kruall) change to CountPatchRecord

    ui32 storage_channel = request.PatchedBlobId.Channel() + MainStorageChannelInPublicApi;
    // ctx.Send(ChannelBalancerActorId, new TChannelBalancer::TEvReportWriteLatency(channel, request.Latency));

    record.CreationUnixTime = unixTime;
    UpdateKeyValue(request.PatchedKey, record, db, ctx);

    if (legacyResponse) {
        legacyResponse->SetStatus(NKikimrProto::OK);
        legacyResponse->SetStatusFlags(request.StatusFlags.Raw);
    }
    if (response) {
        response->set_status(NKikimrKeyValue::Statuses::RSTATUS_OK);
        response->set_status_flag(GetStatusFlag(request.StatusFlags));
        response->set_storage_channel(storage_channel);
    }
}


void TKeyValueState::ProcessCmd(const TIntermediate::TDelete &request,
        NKikimrClient::TKeyValueResponse::TDeleteRangeResult *legacyResponse,
        NKikimrKeyValue::StorageChannel */*response*/,
        ISimpleDb &db, const TActorContext &ctx, TRequestStat &stat, ui64 /*unixTime*/,
        TIntermediate* /*intermediate*/)
{
    TraverseRange(request.Range, [&](TIndex::iterator it) {
        stat.Deletes++;
        stat.DeleteBytes += it->second.GetFullValueSize();
        Dereference(it->second, db, ctx);
        THelpers::DbEraseUserKey(it->first, db, ctx);
        Index.erase(it);
    });

    if (legacyResponse) {
        legacyResponse->SetStatus(NKikimrProto::OK);
    }
}

void TKeyValueState::ProcessCmd(const TIntermediate::TRename &request,
        NKikimrClient::TKeyValueResponse::TRenameResult *legacyResponse,
        NKikimrKeyValue::StorageChannel */*response*/,
        ISimpleDb &db, const TActorContext &ctx, TRequestStat &/*stat*/, ui64 unixTime,
        TIntermediate* /*intermediate*/)
{
    auto oldIter = Index.find(request.OldKey);
    Y_ABORT_UNLESS(oldIter != Index.end());
    TIndexRecord& source = oldIter->second;

    TIndexRecord& dest = Index[request.NewKey];
    Dereference(dest, db, ctx);
    dest.Chain = std::move(source.Chain);
    dest.CreationUnixTime = unixTime;

    THelpers::DbEraseUserKey(oldIter->first, db, ctx);
    Index.erase(oldIter);

    UpdateKeyValue(request.NewKey, dest, db, ctx);

    if (legacyResponse) {
        legacyResponse->SetStatus(NKikimrProto::OK);
    }
}

void TKeyValueState::ProcessCmd(const TIntermediate::TCopyRange &request,
        NKikimrClient::TKeyValueResponse::TCopyRangeResult *legacyResponse,
        NKikimrKeyValue::StorageChannel */*response*/,
        ISimpleDb &db, const TActorContext &ctx, TRequestStat &/*stat*/, ui64 /*unixTime*/,
        TIntermediate *intermediate)
{
    TVector<TIndex::iterator> itemsToClone;

    TraverseRange(request.Range, [&](TIndex::iterator it) {
        if (it->first.StartsWith(request.PrefixToRemove)) {
            itemsToClone.push_back(it);
        }
    });

    for (TIndex::iterator it : itemsToClone) {
        const TIndexRecord& sourceRecord = it->second;
        ui32 inlineSize = 0;
        for (const TIndexRecord::TChainItem& item : sourceRecord.Chain) {
            if (item.IsInline()) {
                inlineSize += item.GetSize();
            } else {
                ++RefCounts[item.LogoBlobId];
                intermediate->RefCountsIncr.emplace_back(item.LogoBlobId, false);
            }
        }
        if (inlineSize) {
            CountWriteRecord(TLogoBlobID(0, 0, 0, 0, inlineSize, 0));
        }

        TString newKey = request.PrefixToAdd + it->first.substr(request.PrefixToRemove.size());
        TIndexRecord& record = Index[newKey];
        Dereference(record, db, ctx);
        record.Chain = sourceRecord.Chain;
        record.CreationUnixTime = sourceRecord.CreationUnixTime;
        UpdateKeyValue(newKey, record, db, ctx);
    }

    if (legacyResponse) {
        legacyResponse->SetStatus(NKikimrProto::OK);
    }
}

void TKeyValueState::ProcessCmd(const TIntermediate::TConcat &request,
        NKikimrClient::TKeyValueResponse::TConcatResult *legacyResponse,
        NKikimrKeyValue::StorageChannel* /*response*/,
        ISimpleDb &db, const TActorContext &ctx, TRequestStat& /*stat*/, ui64 unixTime,
        TIntermediate *intermediate)
{
    TVector<TIndexRecord::TChainItem> chain;
    ui64 offset = 0;

    for (const TString& key : request.InputKeys) {
        auto it = Index.find(key);
        Y_ABORT_UNLESS(it != Index.end());
        TIndexRecord& input = it->second;

        ui32 inlineSize = 0;
        for (TIndexRecord::TChainItem& chainItem : input.Chain) {
            if (chainItem.IsInline()) {
                chain.push_back(TIndexRecord::TChainItem(TRope(chainItem.InlineData), offset));
                inlineSize += chainItem.GetSize();
            } else {
                const TLogoBlobID& id = chainItem.LogoBlobId;
                chain.push_back(TIndexRecord::TChainItem(id, offset));
                ++RefCounts[id];
                intermediate->RefCountsIncr.emplace_back(id, false);
            }
            offset += chainItem.GetSize();
        }
        if (inlineSize) {
            CountWriteRecord(TLogoBlobID(0, 0, 0, 0, inlineSize, 0));
        }

        if (!request.KeepInputs) {
            Dereference(input, db, ctx);
            THelpers::DbEraseUserKey(it->first, db, ctx);
            Index.erase(it);
        }
    }

    TIndexRecord& record = Index[request.OutputKey];
    Dereference(record, db, ctx);
    record.Chain = std::move(chain);
    record.CreationUnixTime = unixTime;
    UpdateKeyValue(request.OutputKey, record, db, ctx);

    if (legacyResponse) {
        legacyResponse->SetStatus(NKikimrProto::OK);
    }
}

void TKeyValueState::CmdRead(THolder<TIntermediate> &intermediate, ISimpleDb &db, const TActorContext &ctx) {
    for (ui64 i = 0; i < intermediate->Reads.size(); ++i) {
        auto &request = intermediate->Reads[i];
        auto *response = intermediate->Response.AddReadResult();
        ProcessCmd(request, response, nullptr, db, ctx, intermediate->Stat, 0, intermediate.Get());
    }
    if (intermediate->ReadCommand && std::holds_alternative<TIntermediate::TRead>(*intermediate->ReadCommand)) {
        auto &request = std::get<TIntermediate::TRead>(*intermediate->ReadCommand);
        auto *response = intermediate->Response.AddReadResult();
        ProcessCmd(request, response, nullptr, db, ctx, intermediate->Stat, 0, intermediate.Get());
    }
}

void TKeyValueState::CmdReadRange(THolder<TIntermediate> &intermediate, ISimpleDb &db, const TActorContext &ctx) {
    Y_UNUSED(ctx);
    Y_UNUSED(db);
    for (ui64 i = 0; i < intermediate->RangeReads.size(); ++i) {
        auto &rangeRead = intermediate->RangeReads[i];
        auto *rangeReadResult = intermediate->Response.AddReadRangeResult();
        ProcessCmd(rangeRead, rangeReadResult, nullptr, db, ctx, intermediate->Stat, 0, intermediate.Get());
    }
    if (intermediate->ReadCommand && std::holds_alternative<TIntermediate::TRangeRead>(*intermediate->ReadCommand)) {
        auto &request = std::get<TIntermediate::TRangeRead>(*intermediate->ReadCommand);
        auto *response = intermediate->Response.AddReadRangeResult();
        ProcessCmd(request, response, nullptr, db, ctx, intermediate->Stat, 0, intermediate.Get());
    }
}

void TKeyValueState::CmdRename(THolder<TIntermediate> &intermediate, ISimpleDb &db, const TActorContext &ctx) {
    ui64 unixTime = TAppData::TimeProvider->Now().Seconds();
    for (ui32 i = 0; i < intermediate->Renames.size(); ++i) {
        auto& request = intermediate->Renames[i];
        auto *response = intermediate->Response.AddRenameResult();
        ProcessCmd(request, response, nullptr, db, ctx, intermediate->Stat, unixTime, intermediate.Get());
    }
}

void TKeyValueState::CmdDelete(THolder<TIntermediate> &intermediate, ISimpleDb &db, const TActorContext &ctx) {
    for (ui32 i = 0; i < intermediate->Deletes.size(); ++i) {
        auto& request = intermediate->Deletes[i];
        auto *response = intermediate->Response.AddDeleteRangeResult();
        ProcessCmd(request, response, nullptr, db, ctx, intermediate->Stat, 0, intermediate.Get());
    }
}

void TKeyValueState::CmdWrite(THolder<TIntermediate> &intermediate, ISimpleDb &db, const TActorContext &ctx) {
    ui64 unixTime = TAppData::TimeProvider->Now().Seconds();
    for (ui32 i = 0; i < intermediate->Writes.size(); ++i) {
        auto& request = intermediate->Writes[i];
        auto *response = intermediate->Response.AddWriteResult();
        ProcessCmd(request, response, nullptr, db, ctx, intermediate->Stat, unixTime, intermediate.Get());
    }
    ResourceMetrics->TryUpdate(ctx);
}

void TKeyValueState::CmdGetStatus(THolder<TIntermediate> &intermediate, ISimpleDb &db, const TActorContext &ctx) {
    Y_UNUSED(db);
    Y_UNUSED(ctx);
    for (ui32 i = 0; i < intermediate->GetStatuses.size(); ++i) {
        auto& request = intermediate->GetStatuses[i];
        if (intermediate->EvType == TEvKeyValue::TEvRequest::EventType) {
            auto& response = *intermediate->Response.AddGetStatusResult();

            response.SetStatus(request.Status);
            response.SetStorageChannel(request.StorageChannel);
            response.SetStatusFlags(request.StatusFlags.Raw);
        } else if ((intermediate->EvType == TEvKeyValue::TEvGetStorageChannelStatus::EventType)) {
            auto response = intermediate->GetStorageChannelStatusResponse.add_storage_channel();

            if (request.Status == NKikimrProto::OK) {
                response->set_status(NKikimrKeyValue::Statuses::RSTATUS_OK);
            } else if (request.Status == NKikimrProto::TIMEOUT) {
                response->set_status(NKikimrKeyValue::Statuses::RSTATUS_TIMEOUT);
            } else {
                response->set_status(NKikimrKeyValue::Statuses::RSTATUS_ERROR);
            }

            if (request.StorageChannel == NKikimrClient::TKeyValueRequest::INLINE) {
                response->set_storage_channel(1);
            } else {
                response->set_storage_channel(request.StorageChannel - BLOB_CHANNEL + MainStorageChannelInPublicApi);
            }

            response->set_status_flag(GetStatusFlag(request.StatusFlags));
        }
    }
    intermediate->GetStorageChannelStatusResponse.set_status(NKikimrKeyValue::Statuses::RSTATUS_OK);
}

void TKeyValueState::CmdCopyRange(THolder<TIntermediate>& intermediate, ISimpleDb& db, const TActorContext& ctx) {
    for (const auto& request : intermediate->CopyRanges) {
        auto *response = intermediate->Response.AddCopyRangeResult();
        ProcessCmd(request, response, nullptr, db, ctx, intermediate->Stat, 0, intermediate.Get());
    }
}

void TKeyValueState::CmdConcat(THolder<TIntermediate>& intermediate, ISimpleDb& db, const TActorContext& ctx) {
    ui64 unixTime = TAppData::TimeProvider->Now().Seconds();
    for (const auto& request : intermediate->Concats) {
        auto *response = intermediate->Response.AddConcatResult();
        ProcessCmd(request, response, nullptr, db, ctx, intermediate->Stat, unixTime, intermediate.Get());
    }
}

void TKeyValueState::CmdTrimLeakedBlobs(THolder<TIntermediate>& intermediate, ISimpleDb& db, const TActorContext& ctx) {
    if (intermediate->TrimLeakedBlobs) {
        auto& response = *intermediate->Response.MutableTrimLeakedBlobsResult();
        response.SetStatus(NKikimrProto::OK);
        ui32 numItems = 0;
        ui32 numUntrimmed = 0;
        for (const TLogoBlobID& id : intermediate->TrimLeakedBlobs->FoundBlobs) {
            auto it = RefCounts.find(id);
            if (it != RefCounts.end()) {
                Y_ABORT_UNLESS(it->second != 0);
            } else if (!Trash.count(id)) { // we found a candidate for trash
                if (numItems < intermediate->TrimLeakedBlobs->MaxItemsToTrim) {
                    LOG_WARN_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletId << " trimming " << id);
                    Trash.insert(id);
                    TotalTrashSize += id.BlobSize();
                    CountUncommittedTrashRecord(id);
                    THelpers::DbUpdateTrash(id, db, ctx);
                    ++numItems;
                } else {
                    ++numUntrimmed;
                }
            }
        }
        response.SetNumItemsTrimmed(numItems);
        response.SetNumItemsLeft(numUntrimmed);
    }
}

void TKeyValueState::CmdSetExecutorFastLogPolicy(THolder<TIntermediate> &intermediate, ISimpleDb &/*db*/,
        const TActorContext &/*ctx*/) {
    if (intermediate->SetExecutorFastLogPolicy) {
        auto& response = *intermediate->Response.MutableSetExecutorFastLogPolicyResult();
        response.SetStatus(NKikimrProto::OK);
    }
}

void TKeyValueState::CmdCmds(THolder<TIntermediate> &intermediate, ISimpleDb &db, const TActorContext &ctx) {
    ui64 unixTime = TAppData::TimeProvider->Now().Seconds();
    bool wasWrite = false;
    auto getChannel = [&](auto &cmd) -> NKikimrKeyValue::StorageChannel* {
        using Type = std::decay_t<decltype(cmd)>;
        if constexpr (std::is_same_v<Type, TIntermediate::TWrite>) {
            if (intermediate->EvType != TEvKeyValue::TEvExecuteTransaction::EventType) {
                return nullptr;
            }
            ui32 storageChannel = MainStorageChannelInPublicApi;
            if (cmd.Status == NKikimrProto::SCHEDULED) {
                storageChannel = InlineStorageChannelInPublicApi;
            }
            if (cmd.LogoBlobIds.size()) {
                storageChannel = cmd.LogoBlobIds.front().Channel() - BLOB_CHANNEL + MainStorageChannelInPublicApi;
            }
            auto it = intermediate->Channels.find(storageChannel);
            if (it == intermediate->Channels.end()) {
                auto channel = intermediate->ExecuteTransactionResponse.add_storage_channel();
                intermediate->Channels.emplace(storageChannel, channel);
                return channel;
            }
            return it->second;
        }
        return nullptr;
    };
    auto getLegacyResponse = [&](auto &cmd) {
        using Type = std::decay_t<decltype(cmd)>;
        if constexpr (std::is_same_v<Type, TIntermediate::TWrite>) {
            wasWrite = true;
            return intermediate->Response.AddWriteResult();
        }
        if constexpr (std::is_same_v<Type, TIntermediate::TPatch>) {
            wasWrite = true;
            return intermediate->Response.AddPatchResult();
        }
        if constexpr (std::is_same_v<Type, TIntermediate::TDelete>) {
            return intermediate->Response.AddDeleteRangeResult();
        }
        if constexpr (std::is_same_v<Type, TIntermediate::TRename>) {
            return intermediate->Response.AddRenameResult();
        }
        if constexpr (std::is_same_v<Type, TIntermediate::TCopyRange>) {
            return intermediate->Response.AddCopyRangeResult();
        }
        if constexpr (std::is_same_v<Type, TIntermediate::TConcat>) {
            return intermediate->Response.AddConcatResult();
        }
    };
    auto process = [&](auto &cmd) {
        ProcessCmd(cmd, getLegacyResponse(cmd), getChannel(cmd), db, ctx, intermediate->Stat, unixTime, intermediate.Get());
    };
    for (auto &cmd : intermediate->Commands) {
        std::visit(process, cmd);
    }
    if (wasWrite) {
        ResourceMetrics->TryUpdate(ctx);
    }
}

TKeyValueState::TCheckResult TKeyValueState::CheckCmd(const TIntermediate::TCopyRange &cmd, TKeySet& keys,
        ui32 index) const
{
    TVector<TString> nkeys;
    auto range = GetRange(cmd.Range, keys);
    for (auto it = range.first; it != range.second; ++it) {
        if (it->StartsWith(cmd.PrefixToRemove)) {
            auto newKey = cmd.PrefixToAdd + it->substr(cmd.PrefixToRemove.size());
            if (!IsKeyLengthValid(newKey)) {
                TStringStream str;
                str << "KeyValue# " << TabletId
                    << " NewKey# " << EscapeC(newKey) << " in CmdCopyRange(" << index << ") has length " << newKey.length() << " but max is " << MaxKeySize
                    << " Marker# KV24";
                return {NKikimrKeyValue::Statuses::RSTATUS_INTERNAL_ERROR, str.Str()};
            }
            nkeys.push_back(newKey);
        }
    }
    keys.insert(nkeys.begin(), nkeys.end());
    return {};
}

TKeyValueState::TCheckResult TKeyValueState::CheckCmd(const TIntermediate::TRename &cmd, TKeySet& keys,
        ui32 index) const
{
    auto it = keys.find(cmd.OldKey);
    if (it == keys.end()) {
        TStringStream str;
        str << "KeyValue# " << TabletId
            << " OldKey# " << EscapeC(cmd.OldKey) << " does not exist in CmdRename(" << index << ")"
            << " Marker# KV18";
        return {NKikimrKeyValue::Statuses::RSTATUS_NOT_FOUND, str.Str()};
    }
    if (!IsKeyLengthValid(cmd.NewKey)) {
        TStringStream str;
        str << "KeyValue# " << TabletId
            << " NewKey# " << EscapeC(cmd.NewKey) << " in CmdRename(" << index << ") has length " << cmd.NewKey.length() << " but max is " << MaxKeySize
            << " Marker# KV23";
        return {NKikimrKeyValue::Statuses::RSTATUS_INTERNAL_ERROR, str.Str()};
    }

    keys.erase(it);
    keys.insert(cmd.NewKey);
    return {};
}

TKeyValueState::TCheckResult TKeyValueState::CheckCmd(const TIntermediate::TConcat &cmd, TKeySet& keys,
        ui32 index) const
{
    for (const TString& key : cmd.InputKeys) {
        auto it = keys.find(key);
        if (it == keys.end()) {
            TStringStream str;
            str << "KeyValue# " << TabletId
                << " InputKey# " << EscapeC(key) << " does not exist in CmdConcat(" << index << ")"
                << " Marker# KV19";
            return {NKikimrKeyValue::Statuses::RSTATUS_NOT_FOUND, str.Str()};
        }
        if (!cmd.KeepInputs) {
            keys.erase(it);
        }
    }

    if (!IsKeyLengthValid(cmd.OutputKey)) {
        TStringStream str;
        str << "KeyValue# " << TabletId
            << " OutputKey length in CmdConcat(" << index << ") is " << cmd.OutputKey.length() << " but max is " << MaxKeySize
            << " Marker# KV25";
        return {NKikimrKeyValue::Statuses::RSTATUS_INTERNAL_ERROR, str.Str()};
    }

    keys.insert(cmd.OutputKey);
    return {};
}

TKeyValueState::TCheckResult TKeyValueState::CheckCmd(const TIntermediate::TDelete &cmd, TKeySet& keys,
        ui32 /*index*/) const
{
    auto r = GetRange(cmd.Range, keys);
    keys.erase(r.first, r.second);
    return {};
}

TKeyValueState::TCheckResult TKeyValueState::CheckCmd(const TIntermediate::TWrite &cmd, TKeySet& keys,
        ui32 index) const
{
    if (!IsKeyLengthValid(cmd.Key)) {
        TStringStream str;
        str << "KeyValue# " << TabletId
            << " Key length in CmdWrite(" << index << ") is " << cmd.Key.length() << " but max is " << MaxKeySize
            << " Marker# KV26";
        return {NKikimrKeyValue::Statuses::RSTATUS_INTERNAL_ERROR, str.Str()};
    }
    keys.insert(cmd.Key);
    return {};
}

TKeyValueState::TCheckResult TKeyValueState::CheckCmd(const TIntermediate::TPatch &cmd, TKeySet& keys, ui32 index) const
{
    auto it = keys.find(cmd.OriginalKey);
    if (it == keys.end()) {
        TStringStream str;
        str << "KeyValue# " << TabletId
            << " InputKey# " << EscapeC(cmd.OriginalKey) << " does not exist in CmdPatch(" << index << ")"
            << " Marker# KV28";
        return {NKikimrKeyValue::Statuses::RSTATUS_NOT_FOUND, str.Str()};
    }

    if (!IsKeyLengthValid(cmd.PatchedKey)) {
        TStringStream str;
        str << "KeyValue# " << TabletId
            << " Key length in CmdPatch(" << index << ") is " << cmd.PatchedKey.length() << " but max is " << MaxKeySize
            << " Marker# KV27";
        return {NKikimrKeyValue::Statuses::RSTATUS_INTERNAL_ERROR, str.Str()};
    }
    keys.insert(cmd.PatchedKey);
    return {};
}


TKeyValueState::TCheckResult TKeyValueState::CheckCmd(const TIntermediate::TGetStatus &/*cmd*/, TKeySet& /*keys*/, ui32 /*index*/) const {
    return {};
}

template<class Cmd>
bool TKeyValueState::CheckCmds(THolder<TIntermediate>& intermediate, const TDeque<Cmd>& cmds, const TActorContext& ctx,
        TKeySet& keys, const TTabletStorageInfo* info) {

    ui32 index = 0;
    for (const auto& cmd : cmds) {
        const auto &[status, msg] = CheckCmd(cmd, keys, index++);
        if (NKikimrKeyValue::Statuses::RSTATUS_OK != status) {
            ReplyError(ctx, msg, NMsgBusProxy::MSTATUS_ERROR, status, intermediate, info);
            return false;
        }
    }

    return true;
}

bool TKeyValueState::CheckCmds(THolder<TIntermediate>& intermediate, const TActorContext& ctx, TKeySet& keys,
        const TTabletStorageInfo* info)
{
    ui32 renameIndex = 0;
    ui32 concatIndex = 0;

    auto nextIdx = [&](auto &cmd) -> ui32 {
        using Type = std::decay_t<decltype(cmd)>;
        if constexpr (std::is_same_v<Type, TIntermediate::TRename>) {
            return renameIndex++;
        }
        if constexpr (std::is_same_v<Type, TIntermediate::TConcat>) {
            return concatIndex++;
        }
        return 0;
    };
    auto visitor = [&](auto &cmd) {
        return CheckCmd(cmd, keys, nextIdx(cmd));
    };

    for (const auto& cmd : intermediate->Commands) {
        const auto &[status, msg] = std::visit(visitor, cmd);
        if (NKikimrKeyValue::Statuses::RSTATUS_OK != status) {
            ReplyError(ctx, msg, NMsgBusProxy::MSTATUS_ERROR, status, intermediate, info);
            return false;
        }
    }
    return true;
}

void TKeyValueState::ProcessCmds(THolder<TIntermediate> &intermediate, ISimpleDb &db, const TActorContext &ctx,
        const TTabletStorageInfo *info) {
    LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletId << " TTxRequest ProcessCmds");

    TKeySet keys(Index);

    bool success = true;

    if (intermediate->HasGeneration && intermediate->Generation != StoredState.GetUserGeneration()) {
        TStringStream str;
        str << "KeyValue# " << TabletId
            << " Generation changed during command execution, aborted"
            << " Marker# KV04";
        ReplyError(ctx, str.Str(), NMsgBusProxy::MSTATUS_REJECTED, NKikimrKeyValue::Statuses::RSTATUS_WRONG_LOCK_GENERATION, intermediate, info);
        success = false;
    }

    success = success && CheckCmds(intermediate, intermediate->CopyRanges, ctx, keys, info);
    success = success && CheckCmds(intermediate, intermediate->Renames, ctx, keys, info);
    success = success && CheckCmds(intermediate, intermediate->Concats, ctx, keys, info);
    success = success && CheckCmds(intermediate, intermediate->Deletes, ctx, keys, info);
    success = success && CheckCmds(intermediate, intermediate->Writes, ctx, keys, info);
    success = success && CheckCmds(intermediate, intermediate->Patches, ctx, keys, info);
    success = success && CheckCmds(intermediate, ctx, keys, info);
    success = success && CheckCmds(intermediate, intermediate->GetStatuses, ctx, keys, info);
    if (!success) {
        DropRefCountsOnErrorInTx(std::exchange(intermediate->RefCountsIncr, {}), db, ctx);
    } else {
        // Read + validate
        CmdRead(intermediate, db, ctx);
        CmdReadRange(intermediate, db, ctx);
        CmdCopyRange(intermediate, db, ctx);
        CmdRename(intermediate, db, ctx);

        // All reads done
        CmdConcat(intermediate, db, ctx);
        CmdDelete(intermediate, db, ctx);
        CmdWrite(intermediate, db, ctx);
        CmdGetStatus(intermediate, db, ctx);
        CmdCmds(intermediate, db, ctx);

        // Blob trimming
        CmdTrimLeakedBlobs(intermediate, db, ctx);

        CmdSetExecutorFastLogPolicy(intermediate, db, ctx);
    }

    // Safe to change the internal state and prepare the response
    intermediate->Response.SetStatus(NMsgBusProxy::MSTATUS_OK);

    if (intermediate->Stat.RequestType == TRequestType::ReadOnly ||
            intermediate->Stat.RequestType == TRequestType::ReadOnlyInline) {
        Reply(intermediate, ctx, info);
    }
}

bool TKeyValueState::IncrementGeneration(THolder<TIntermediate> &intermediate, ISimpleDb &db,
        const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletId << " TTxRequest IncrementGeneration");

    ui64 nextGeneration = StoredState.GetUserGeneration() + 1;
    Y_ABORT_UNLESS(nextGeneration > StoredState.GetUserGeneration());
    StoredState.SetUserGeneration(nextGeneration);

    THelpers::DbUpdateState(StoredState, db, ctx);

    intermediate->Response.MutableIncrementGenerationResult()->SetGeneration(nextGeneration);
    intermediate->Response.SetStatus(NMsgBusProxy::MSTATUS_OK);

    return true;
}

void TKeyValueState::Dereference(const TIndexRecord& record, ISimpleDb& db, const TActorContext& ctx) {
    ui32 inlineSize = 0;
    for (const TIndexRecord::TChainItem& item : record.Chain) {
        if (item.IsInline()) {
            inlineSize += item.GetSize();
        } else {
            Dereference(item.LogoBlobId, db, ctx, false);
        }
    }
    if (inlineSize) {
        CountDeleteInline(inlineSize);
    }
}

void TKeyValueState::Dereference(const TLogoBlobID& id, ISimpleDb& db, const TActorContext& ctx, bool initial) {
    auto it = RefCounts.find(id);
    Y_ABORT_UNLESS(it != RefCounts.end());
    --it->second;
    if (!it->second) {
        RefCounts.erase(it);
        db.AddTrash(id);
        TotalTrashSize += id.BlobSize();
        THelpers::DbUpdateTrash(id, db, ctx);
        if (initial) {
            CountUncommittedTrashRecord(id);
        } else {
            CountTrashRecord(id);
        }
    }
}

void TKeyValueState::PushTrashBeingCommitted(TVector<TLogoBlobID>& trashBeingCommitted, const TActorContext& ctx) {
    Trash.insert(trashBeingCommitted.begin(), trashBeingCommitted.end());
    for (const TLogoBlobID& id : trashBeingCommitted) {
        CountTrashCommitted(id);
    }
    PrepareCollectIfNeeded(ctx);
}

void TKeyValueState::UpdateKeyValue(const TString& key, const TIndexRecord& record, ISimpleDb& db,
        const TActorContext& ctx) {
    TString value = record.Serialize();
    THelpers::DbUpdateUserKeyValue(key, value, db, ctx);
}

void TKeyValueState::OnPeriodicRefresh(const TActorContext &ctx) {
    Y_UNUSED(ctx);
    TInstant now = TAppData::TimeProvider->Now();
    TInstant oldestInstant = now;
    for (const auto &requestInputTime : RequestInputTime) {
        oldestInstant = Min(oldestInstant, requestInputTime.second);
    }
    TDuration maxDuration = now - oldestInstant;
    TabletCounters->Simple()[COUNTER_REQ_AGE_MS].Set(maxDuration.MilliSeconds());
}

void TKeyValueState::OnUpdateWeights(TChannelBalancer::TEvUpdateWeights::TPtr ev) {
    WeightManager = std::move(ev->Get()->WeightManager);
}

void TKeyValueState::OnRequestComplete(ui64 requestUid, ui64 generation, ui64 step, const TActorContext &ctx,
        const TTabletStorageInfo *info, NMsgBusProxy::EResponseStatus status, const TRequestStat &stat) {
    LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletId
        << " OnRequestComplete uid# " << requestUid << " generation# " << generation
        << " step# " << step << " ChannelGeneration# " << StoredState.GetChannelGeneration()
        << " ChannelStep# " << StoredState.GetChannelStep());

    CountLatencyBsOps(stat);

    RequestInputTime.erase(requestUid);

    if (stat.RequestType != TRequestType::WriteOnly) {
        if (stat.RequestType == TRequestType::ReadOnlyInline) {
            RoInlineIntermediatesInFlight--;
        } else {
            IntermediatesInFlight--;
        }
    }

    CountRequestComplete(status, stat, ctx);
    ResourceMetrics->TryUpdate(ctx);

    if (Queue.size() && IntermediatesInFlight < IntermediatesInFlightLimit) {
        TRequestType::EType requestType = Queue.front()->Stat.RequestType;

        CountLatencyQueue(Queue.front()->Stat);

        ProcessPostponedIntermediate(ctx, std::move(Queue.front()), info);
        Queue.pop_front();
        ++IntermediatesInFlight;

        CountRequestTakeOffOrEnqueue(requestType);
    }

    CmdTrimLeakedBlobsUids.erase(requestUid);
    CancelInFlight(requestUid);
    PrepareCollectIfNeeded(ctx);
}

void TKeyValueState::CancelInFlight(ui64 requestUid) {
    for (auto it = RequestUidStepToCount.lower_bound(std::make_tuple(requestUid, 0)); it != RequestUidStepToCount.end() &&
            std::get<0>(it->first) == requestUid; it = RequestUidStepToCount.erase(it)) {
        const auto& [requestUid, step] = it->first;
        const ui32 drop = it->second;
        auto stepIt = InFlightForStep.find(step);
        Y_ABORT_UNLESS(stepIt != InFlightForStep.end() && drop <= stepIt->second);
        if (drop < stepIt->second) {
            stepIt->second -= drop;
        } else {
            InFlightForStep.erase(stepIt);
        }
    }
}

bool TKeyValueState::CheckDeadline(const TActorContext &ctx, NKikimrClient::TKeyValueRequest &kvRequest,
        THolder<TIntermediate> &intermediate) {
    if (kvRequest.HasDeadlineInstantMs()) {
        TInstant now = TAppData::TimeProvider->Now();
        intermediate->Deadline = TInstant::MicroSeconds(kvRequest.GetDeadlineInstantMs() * 1000ull);

        if (intermediate->Deadline <= now) {
            TStringStream str;
            str << "KeyValue# " << TabletId;
            str << " Deadline reached before processing the request!";
            str << " DeadlineInstantMs# " << (ui64)kvRequest.GetDeadlineInstantMs();
            str << " < Now# " << (ui64)now.MilliSeconds();
            ReplyError(ctx, str.Str(), NMsgBusProxy::MSTATUS_TIMEOUT, NKikimrKeyValue::Statuses::RSTATUS_TIMEOUT, intermediate);
            return true;
        }
    }
    return false;
}

bool TKeyValueState::CheckGeneration(const TActorContext &ctx, NKikimrClient::TKeyValueRequest &kvRequest,
        THolder<TIntermediate> &intermediate) {
    if (kvRequest.HasGeneration()) {
        intermediate->HasGeneration = true;
        intermediate->Generation = kvRequest.GetGeneration();
        if (kvRequest.GetGeneration() != StoredState.GetUserGeneration()) {
            TStringStream str;
            str << "KeyValue# " << TabletId;
            str << " Generation mismatch! Requested# " << kvRequest.GetGeneration();
            str << " Actual# " << StoredState.GetUserGeneration();
            str << " Marker# KV01";
            ReplyError(ctx, str.Str(), NMsgBusProxy::MSTATUS_REJECTED, NKikimrKeyValue::Statuses::RSTATUS_WRONG_LOCK_GENERATION, intermediate);
            return true;
        }
    } else {
        intermediate->HasGeneration = false;
    }
    return false;
}


template <typename TypeWithPriority>
void SetPriority(NKikimrBlobStorage::EGetHandleClass *outHandleClass, ui8 priority) {
    *outHandleClass = NKikimrBlobStorage::FastRead;
    if constexpr (std::is_same_v<TypeWithPriority, NKikimrKeyValue::Priorities>) {
        switch (priority) {
            case TypeWithPriority::PRIORITY_UNSPECIFIED:
            case TypeWithPriority::PRIORITY_REALTIME:
                *outHandleClass = NKikimrBlobStorage::FastRead;
                break;
            case TypeWithPriority::PRIORITY_BACKGROUND:
                *outHandleClass = NKikimrBlobStorage::AsyncRead;
                break;
        }
    } else {
        switch (priority) {
            case TypeWithPriority::REALTIME:
                *outHandleClass = NKikimrBlobStorage::FastRead;
                break;
            case TypeWithPriority::BACKGROUND:
                *outHandleClass = NKikimrBlobStorage::AsyncRead;
                break;
        }
    }
}

template <typename TypeWithPriority, bool WithOverrun = false, ui64 SpecificReadResultSizeEstimation=ReadResultSizeEstimation>
bool PrepareOneRead(const TString &key, TIndexRecord &indexRecord, ui64 offset, ui64 size, ui8 priority,
        ui64 cmdLimitBytes, THolder<TIntermediate> &intermediate, TIntermediate::TRead &response, bool &outIsInlineOnly)
{
    for (ui64 idx = 0; idx < indexRecord.Chain.size(); ++idx) {
        if (!indexRecord.Chain[idx].IsInline()) {
            outIsInlineOnly = false;
            break;
        }
    }

    if (!size) {
        size = std::numeric_limits<decltype(size)>::max();
    }
    ui64 fullValueSize = indexRecord.GetFullValueSize();
    offset = std::min(offset, fullValueSize);
    size = std::min(size, fullValueSize - offset);
    ui64 metaDataSize = key.size() + SpecificReadResultSizeEstimation;
    ui64 recSize = std::max(size, ErrorMessageSizeEstimation) + metaDataSize;

    response.RequestedSize = size;
    bool isOverRun = false;

    if (intermediate->IsTruncated
            || intermediate->TotalSize + recSize > intermediate->TotalSizeLimit
            || (cmdLimitBytes && intermediate->TotalSize + recSize > cmdLimitBytes)) {
        response.Status = NKikimrProto::OVERRUN;
        if (!WithOverrun
                || std::min(intermediate->TotalSizeLimit, cmdLimitBytes) < intermediate->TotalSize + metaDataSize)
        {
            return true;
        }
        if (cmdLimitBytes) {
            size = std::min(intermediate->TotalSizeLimit, cmdLimitBytes) - intermediate->TotalSize - metaDataSize;
        } else {
            size = intermediate->TotalSizeLimit - intermediate->TotalSize - metaDataSize;
        }
        isOverRun = true;
    }

    response.ValueSize = size;
    response.CreationUnixTime = indexRecord.CreationUnixTime;
    response.Key = key;

    SetPriority<TypeWithPriority>(&response.HandleClass, priority);

    if (size) {
        const ui32 numReads = indexRecord.GetReadItems(offset, size, response);
        intermediate->TotalSize += recSize;
        intermediate->TotalReadsScheduled += numReads;
    } else if (response.Status != NKikimrProto::OVERRUN) {
        response.Status = NKikimrProto::OK;
    }
    return isOverRun;
}

template <typename TypeWithPriority, ui64 SpecificKeyValuePairSizeEstimation>
bool PrepareOneReadFromRangeReadWithoutData(const TString &key, TIndexRecord &indexRecord, ui8 priority,
        THolder<TIntermediate> &intermediate, TIntermediate::TRangeRead &response,
        ui64 &cmdSizeBytes, ui64 cmdLimitBytes, bool *outIsInlineOnly)
{
    if (intermediate->IsTruncated) {
        return false;
    }

    NKikimrClient::TKeyValueRequest::EStorageChannel storageChannel =
        NKikimrClient::TKeyValueRequest::MAIN;
    if (indexRecord.Chain.size()) {
        if (indexRecord.Chain[0].IsInline()) {
            storageChannel = NKikimrClient::TKeyValueRequest::INLINE;
        } else {
            *outIsInlineOnly = false;
            ui32 storageChannelIdx = indexRecord.Chain[0].LogoBlobId.Channel();
            ui32 storageChannelOffset = storageChannelIdx - BLOB_CHANNEL;
            storageChannel = (NKikimrClient::TKeyValueRequest::EStorageChannel)storageChannelOffset;
        }
    }

    ui64 metadataSize = key.size() + SpecificKeyValuePairSizeEstimation;
    if (intermediate->TotalSize + metadataSize > intermediate->TotalSizeLimit
            || cmdSizeBytes + metadataSize > cmdLimitBytes) {
        STLOG(NLog::PRI_TRACE, NKikimrServices::KEYVALUE, KV330, "Went beyond limits",
                (intermediate->TotalSize + metadataSize, intermediate->TotalSize + metadataSize),
                (intermediate->TotalSizeLimit, intermediate->TotalSizeLimit),
                (cmdSizeBytes + metadataSize, cmdSizeBytes + metadataSize),
                (cmdLimitBytes, cmdLimitBytes));
        return true;
    }
    response.Reads.emplace_back(key, indexRecord.GetFullValueSize(), indexRecord.CreationUnixTime,
            storageChannel);
    intermediate->TotalSize += metadataSize;
    SetPriority<TypeWithPriority>(&response.HandleClass, priority);

    cmdSizeBytes += metadataSize;
    return false;
}

struct TSeqInfo {
    ui32 Reads = 0;
    ui32 RunLen = 0;
    ui32 Generation = 0;
    ui32 Step = 0;
    ui32 Cookie = 0;
};

template <typename TypeWithPriority, ui64 SpecificKeyValuePairSizeEstimation>
bool PrepareOneReadFromRangeReadWithData(const TString &key, TIndexRecord &indexRecord, ui8 priority,
        THolder<TIntermediate> &intermediate, TIntermediate::TRangeRead &response,
        ui64 &cmdSizeBytes, ui64 cmdLimitBytes, TSeqInfo &seq, bool *outIsInlineOnly)
{
    if (intermediate->IsTruncated) {
        return false;
    }

    NKikimrClient::TKeyValueRequest::EStorageChannel storageChannel =
        NKikimrClient::TKeyValueRequest::MAIN;
    if (indexRecord.Chain.size()) {
        if (indexRecord.Chain[0].IsInline()) {
            storageChannel = NKikimrClient::TKeyValueRequest::INLINE;
        } else {
            *outIsInlineOnly = false;
            ui32 storageChannelIdx = indexRecord.Chain[0].LogoBlobId.Channel();
            ui32 storageChannelOffset = storageChannelIdx - BLOB_CHANNEL;
            storageChannel = (NKikimrClient::TKeyValueRequest::EStorageChannel)storageChannelOffset;
        }
    }

    bool isSeq = false;
    bool isInline = false;
    if (indexRecord.Chain.size() == 1) {
        if (indexRecord.Chain.front().IsInline()) {
            isSeq = true;
            isInline = true;
        } else {
            const TLogoBlobID& id = indexRecord.Chain.front().LogoBlobId;
            isSeq = id.Generation() == seq.Generation
                && id.Step() == seq.Step
                && id.Cookie() == seq.Cookie;
            seq.Generation = id.Generation();
            seq.Step = id.Step();
            seq.Cookie = id.Cookie() + 1;
        }
    }
    if (isSeq) {
        seq.Reads++;
        if (seq.Reads > intermediate->SequentialReadLimit && !isInline) {
            isSeq = false;
        } else {
            ++seq.RunLen;
        }
    }
    if (!isSeq) {
        seq.RunLen = 1;
    }

    ui64 valueSize = indexRecord.GetFullValueSize();
    ui64 metadataSize = key.size() + SpecificKeyValuePairSizeEstimation;
    if (intermediate->TotalSize + valueSize + metadataSize > intermediate->TotalSizeLimit
            || cmdSizeBytes + valueSize + metadataSize > cmdLimitBytes
            || (seq.RunLen == 1 && intermediate->TotalReadsScheduled >= intermediate->TotalReadsLimit)) {
        return true;
    }

    TIntermediate::TRead read(key, valueSize, indexRecord.CreationUnixTime, storageChannel);
    const ui32 numReads = indexRecord.GetReadItems(0, valueSize, read);
    SetPriority<TypeWithPriority>(&response.HandleClass, priority);
    SetPriority<TypeWithPriority>(&read.HandleClass, priority);

    response.Reads.push_back(std::move(read));

    intermediate->TotalSize += valueSize + metadataSize;
    intermediate->TotalReadsScheduled += numReads;

    cmdSizeBytes += valueSize + metadataSize;
    return false;
}

bool TKeyValueState::PrepareCmdRead(const TActorContext &ctx, NKikimrClient::TKeyValueRequest &kvRequest,
        THolder<TIntermediate> &intermediate, bool &outIsInlineOnly) {
    outIsInlineOnly = true;
    intermediate->Reads.resize(kvRequest.CmdReadSize());
    for (ui32 i = 0; i < kvRequest.CmdReadSize(); ++i) {
        auto &request = kvRequest.GetCmdRead(i);
        auto &response = intermediate->Reads[i];

        if (!request.HasKey()) {
            TStringStream str;
            str << "KeyValue# " << TabletId;
            str << " Missing Key in CmdRead(" << (ui32)i << ") Marker# KV02";
            ReplyError(ctx, str.Str(), NMsgBusProxy::MSTATUS_INTERNALERROR, NKikimrKeyValue::Statuses::RSTATUS_INTERNAL_ERROR, intermediate);
            return true;
        }

        ui64 offset = request.HasOffset() ? request.GetOffset() : 0;
        ui64 size = request.HasSize() ? request.GetSize() : 0;
        NKikimrClient::TKeyValueRequest::EPriority priority = NKikimrClient::TKeyValueRequest::REALTIME;
        if (request.HasPriority()) {
            priority = request.GetPriority();
        }

        auto it = Index.find(request.GetKey());
        if (it == Index.end()) {
            response.Status = NKikimrProto::NODATA;
            response.Message = "No such key Marker# KV48";
        } else {
            bool isOverrun = PrepareOneRead<NKikimrClient::TKeyValueRequest>(it->first, it->second, offset, size,
                    priority, 0, intermediate, response, outIsInlineOnly);
            if (isOverrun) {
                if (!intermediate->IsTruncated) {
                    CountOverrun();
                    intermediate->IsTruncated = true;
                }
            }
        }
    }
    return false;
}

template <typename TypeWithPriority, bool CheckUTF8 = false,
        ui64 MetaDataSizeWithData = KeyValuePairSizeEstimation,
        ui64 MetaDataSizeWithoutData = KeyValuePairSizeEstimation>
void ProcessOneCmdReadRange(TKeyValueState *self, const TKeyRange &range, ui64 cmdLimitBytes, bool includeData,
        ui8 priority, TIntermediate::TRangeRead &response, THolder<TIntermediate> &intermediate, bool *outIsInlineOnly)
{
    ui64 cmdSizeBytes = 0;
    TSeqInfo seq;
    seq.RunLen = 1;

    self->TraverseRange(range, [&](TKeyValueState::TIndex::iterator it) {
        if (intermediate->IsTruncated) {
            return;
        }

        auto &[key, indexRecord] = *it;

        if (CheckUTF8 && !IsUtf(key)) {
            TIntermediate::TRead read;
            read.CreationUnixTime = indexRecord.CreationUnixTime;
            EscapeC(key, read.Key);
            read.Status = NKikimrProto::ERROR;
            read.Message = "Key isn't UTF8";
            response.Reads.push_back(std::move(read));
            return;
        }

        bool isOverRun = false;
        if (includeData) {
            isOverRun = PrepareOneReadFromRangeReadWithData<NKikimrClient::TKeyValueRequest, MetaDataSizeWithData>(
                    key, indexRecord, priority, intermediate, response,
                    cmdSizeBytes, cmdLimitBytes, seq, outIsInlineOnly);
        } else {
            isOverRun = PrepareOneReadFromRangeReadWithoutData<NKikimrClient::TKeyValueRequest, MetaDataSizeWithoutData>(
                    key, indexRecord, priority, intermediate, response, cmdSizeBytes,
                    cmdLimitBytes, outIsInlineOnly);
        }
        if (isOverRun) {
            self->CountOverrun();
            intermediate->IsTruncated = true;
        }
    });

    if (intermediate->IsTruncated) {
        response.Status = NKikimrProto::OVERRUN;
    } else if (response.Reads.size() == 0) {
        response.Status = NKikimrProto::NODATA;
    }
}

bool TKeyValueState::PrepareCmdReadRange(const TActorContext &ctx, NKikimrClient::TKeyValueRequest &kvRequest,
        THolder<TIntermediate> &intermediate, bool &inOutIsInlineOnly) {
    intermediate->RangeReads.resize(kvRequest.CmdReadRangeSize());
    for (ui32 i = 0; i < kvRequest.CmdReadRangeSize(); ++i) {
        auto &request = kvRequest.GetCmdReadRange(i);
        auto &response = intermediate->RangeReads[i];

        if (!request.HasRange()) {
            TStringStream str;
            str << "KeyValue# " << TabletId;
            str << " Missing Range in CmdReadRange(" << i << ") Marker# KV03";
            ReplyError(ctx, str.Str(), NMsgBusProxy::MSTATUS_INTERNALERROR, NKikimrKeyValue::Statuses::RSTATUS_INTERNAL_ERROR, intermediate);
            return true;
        }

        TKeyRange range;
        if (!ConvertRange(request.GetRange(), &range, ctx, intermediate, "CmdReadRange", i)) {
            return true;
        }

        ui64 cmdLimitBytes = request.HasLimitBytes() ? request.GetLimitBytes() : Max<ui64>();
        bool includeData = request.HasIncludeData() && request.GetIncludeData();
        ui8 priority = request.HasPriority() ? request.GetPriority() : Max<ui8>();
        response.IncludeData = includeData;
        response.LimitBytes = cmdLimitBytes;

        ProcessOneCmdReadRange<NKikimrClient::TKeyValueRequest>(this, range, cmdLimitBytes, includeData, priority,
                response, intermediate, &inOutIsInlineOnly);
    }
    return false;
}

bool TKeyValueState::PrepareCmdRename(const TActorContext &ctx, NKikimrClient::TKeyValueRequest &kvRequest,
        THolder<TIntermediate> &intermediate) {
    for (ui32 i = 0; i < kvRequest.CmdRenameSize(); ++i) {
        auto& request = kvRequest.GetCmdRename(i);
        intermediate->Commands.emplace_back(TIntermediate::TRename());
        auto& interm = std::get<TIntermediate::TRename>(intermediate->Commands.back());

        if (!request.HasOldKey()) {
            TStringStream str;
            str << "KeyValue# " << TabletId;
            str << " Missing OldKey in CmdRename(" << i << ") Marker# KV06";
            ReplyError(ctx, str.Str(), NMsgBusProxy::MSTATUS_INTERNALERROR, NKikimrKeyValue::Statuses::RSTATUS_INTERNAL_ERROR, intermediate);
            return true;
        }
        if (!request.HasNewKey()) {
            TStringStream str;
            str << "KeyValue# " << TabletId;
            str << " Missing NewKey in CmdRename(" << i << ") Marker# KV07";
            ReplyError(ctx, str.Str(), NMsgBusProxy::MSTATUS_INTERNALERROR, NKikimrKeyValue::Statuses::RSTATUS_INTERNAL_ERROR, intermediate);
            return true;
        }

        interm.OldKey = request.GetOldKey();
        interm.NewKey = request.GetNewKey();
    }
    return false;
}

bool TKeyValueState::PrepareCmdDelete(const TActorContext &ctx, NKikimrClient::TKeyValueRequest &kvRequest,
        THolder<TIntermediate> &intermediate) {
    ui64 nToDelete = 0;
    for (ui32 i = 0; i < kvRequest.CmdDeleteRangeSize(); ++i) {
        auto& request = kvRequest.GetCmdDeleteRange(i);
        intermediate->Commands.emplace_back(TIntermediate::TDelete());
        auto& interm = std::get<TIntermediate::TDelete>(intermediate->Commands.back());

        if (!request.HasRange()) {
            TStringStream str;
            str << "KeyValue# " << TabletId;
            str << " Missing Range in CmdDelete(" << i << ") Marker# KV08";
            ReplyError(ctx, str.Str(), NMsgBusProxy::MSTATUS_INTERNALERROR, NKikimrKeyValue::Statuses::RSTATUS_INTERNAL_ERROR, intermediate);
            return true;
        }
        if (!ConvertRange(request.GetRange(), &interm.Range, ctx, intermediate, "CmdDelete", i)) {
            return true;
        }
        TraverseRange(interm.Range, [&](TIndex::iterator it) {
                Y_UNUSED(it);
                nToDelete++;
            });
        // The use of >, not >= is important here.
        if (nToDelete > DeletesPerRequestLimit && !AppData(ctx)->AllowHugeKeyValueDeletes) {
            TStringStream str;
            str << "KeyValue# " << TabletId;
            str << " Can't delete Range, in CmdDelete(" << i << "), total limit of deletions per request ("
                << DeletesPerRequestLimit << ") reached, Marker# KV32";
            ReplyError(ctx, str.Str(), NMsgBusProxy::MSTATUS_INTERNALERROR, NKikimrKeyValue::Statuses::RSTATUS_INTERNAL_ERROR, intermediate);
            return true;
        }
    }
    return false;
}

void TKeyValueState::SplitIntoBlobs(TIntermediate::TWrite &cmd, bool isInline, ui32 storageChannelIdx) {
    if (isInline) {
        cmd.Status = NKikimrProto::SCHEDULED;
        cmd.StatusFlags = TStorageStatusFlags(ui32(NKikimrBlobStorage::StatusIsValid));
        if (GetIsTabletYellowMove()) {
            cmd.StatusFlags.Merge(ui32(NKikimrBlobStorage::StatusDiskSpaceLightYellowMove));
        }
        if (GetIsTabletYellowStop()) {
            cmd.StatusFlags.Merge(ui32(NKikimrBlobStorage::StatusDiskSpaceYellowStop));
        }
    } else {
        cmd.Status = NKikimrProto::UNKNOWN;
        ui64 sizeRemain = cmd.Data.size();
        while (sizeRemain) {
            ui32 blobSize = Min<ui64>(sizeRemain, 8 << 20);
            cmd.LogoBlobIds.push_back(TLogoBlobID(0, 0, 0, storageChannelIdx, blobSize, 0));
            sizeRemain -= blobSize;
        }
    }
}

bool TKeyValueState::PrepareCmdWrite(const TActorContext &ctx, NKikimrClient::TKeyValueRequest &kvRequest,
        TEvKeyValue::TEvRequest& ev, THolder<TIntermediate> &intermediate, const TTabletStorageInfo *info) {
    intermediate->WriteIndices.reserve(kvRequest.CmdWriteSize());
    for (ui32 i = 0; i < kvRequest.CmdWriteSize(); ++i) {
        auto& request = kvRequest.GetCmdWrite(i);
        intermediate->WriteIndices.push_back(intermediate->Commands.size());
        auto& cmd = intermediate->Commands.emplace_back(TIntermediate::TWrite());
        auto& interm = std::get<TIntermediate::TWrite>(cmd);

        if (!request.HasKey()) {
            TStringStream str;
            str << "KeyValue# " << TabletId;
            str << " Missing Key in CmdWrite(" << i << ") Marker# KV09";
            ReplyError(ctx, str.Str(), NMsgBusProxy::MSTATUS_INTERNALERROR, NKikimrKeyValue::Statuses::RSTATUS_INTERNAL_ERROR, intermediate);
            return true;
        }
        if (request.GetDataCase() == NKikimrClient::TKeyValueRequest::TCmdWrite::DATA_NOT_SET) {
            TStringStream str;
            str << "KeyValue# " << TabletId;
            str << " Missing Value/PayloadId in CmdWrite(" << i << ") Marker# KV10";
            ReplyError(ctx, str.Str(), NMsgBusProxy::MSTATUS_INTERNALERROR, NKikimrKeyValue::Statuses::RSTATUS_INTERNAL_ERROR, intermediate);
            return true;
        }

        bool isInline = false;
        ui32 storageChannelIdx = BLOB_CHANNEL;
        if (request.HasStorageChannel()) {
            auto storageChannel = request.GetStorageChannel();
            ui32 storageChannelOffset = (ui32)storageChannel;
            if (storageChannelOffset == NKikimrClient::TKeyValueRequest::INLINE) {
                isInline = true;
            } else {
                storageChannelIdx = storageChannelOffset + BLOB_CHANNEL;
                ui32 endChannel = info->Channels.size();
                if (storageChannelIdx >= endChannel) {
                    storageChannelIdx = BLOB_CHANNEL;
                    LOG_INFO_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletId
                            << " CmdWrite StorageChannel# " << storageChannelOffset
                            << " does not exist, using MAIN");
                }
            }
        } else if (request.AutoselectChannelSize() && WeightManager) {
            std::bitset<256> enabled;
            for (const auto& channel : request.GetAutoselectChannel()) {
                if (channel != NKikimrClient::TKeyValueRequest::INLINE) {
                    const ui32 index = channel + BLOB_CHANNEL;
                    if (index < info->Channels.size()) {
                        Y_ABORT_UNLESS(index < enabled.size());
                        enabled.set(index);
                    }
                }
            }
            // TODO(alexvru): trim enabled channels by used space
            if (enabled.any()) {
                int index = WeightManager->Pick(enabled);
                if (index != -1) {
                    storageChannelIdx = index;
                }
            }
        }

        interm.Key = request.GetKey();

        switch (request.GetDataCase()) {
            case NKikimrClient::TKeyValueRequest::TCmdWrite::kValue:
                interm.Data = TRope(request.GetValue());
                break;

            case NKikimrClient::TKeyValueRequest::TCmdWrite::kPayloadId:
                interm.Data = ev.GetPayload(request.GetPayloadId());
                break;

            case NKikimrClient::TKeyValueRequest::TCmdWrite::DATA_NOT_SET:
                Y_UNREACHABLE();
        }

        interm.Tactic = TEvBlobStorage::TEvPut::TacticDefault;
        switch (request.GetTactic()) {
            case NKikimrClient::TKeyValueRequest::MIN_LATENCY:
                interm.Tactic = TEvBlobStorage::TEvPut::TacticMinLatency;
            break;
            case NKikimrClient::TKeyValueRequest::MAX_THROUGHPUT:
                interm.Tactic = TEvBlobStorage::TEvPut::TacticMaxThroughput;
            break;
        }
        interm.HandleClass = NKikimrBlobStorage::UserData;
        if (request.HasPriority()) {
            switch (request.GetPriority()) {
                case NKikimrClient::TKeyValueRequest::REALTIME:
                    interm.HandleClass = NKikimrBlobStorage::UserData;
                    break;
                case NKikimrClient::TKeyValueRequest::BACKGROUND:
                    interm.HandleClass = NKikimrBlobStorage::AsyncBlob;
                    break;
            }
        }
        SplitIntoBlobs(interm, isInline, storageChannelIdx);
    }
    return false;
}

bool TKeyValueState::PrepareCmdPatch(const TActorContext &ctx, NKikimrClient::TKeyValueRequest &kvRequest,
        TEvKeyValue::TEvRequest& ev, THolder<TIntermediate> &intermediate, const TTabletStorageInfo *info) {

    intermediate->PatchIndices.reserve(kvRequest.CmdPatchSize());
    for (ui32 patchIdx = 0; patchIdx < kvRequest.CmdPatchSize(); ++patchIdx) {
        auto& request = kvRequest.GetCmdPatch(patchIdx);
        intermediate->PatchIndices.push_back(intermediate->Commands.size());
        auto& cmd = intermediate->Commands.emplace_back(TIntermediate::TPatch());
        auto& interm = std::get<TIntermediate::TPatch>(cmd);

        if (!request.HasOriginalKey()) {
            TStringStream str;
            str << "KeyValue# " << TabletId;
            str << " Missing OriginalKey in CmdPatch(" << patchIdx << ") Marker# KV35";
            ReplyError(ctx, str.Str(), NMsgBusProxy::MSTATUS_INTERNALERROR, NKikimrKeyValue::Statuses::RSTATUS_INTERNAL_ERROR, intermediate);
            return true;
        }
        interm.OriginalKey = request.GetOriginalKey();
        if (!request.HasPatchedKey()) {
            TStringStream str;
            str << "KeyValue# " << TabletId;
            str << " Missing PatchedKey in CmdPatch(" << patchIdx << ") Marker# KV37";
            ReplyError(ctx, str.Str(), NMsgBusProxy::MSTATUS_INTERNALERROR, NKikimrKeyValue::Statuses::RSTATUS_INTERNAL_ERROR, intermediate);
            return true;
        }
        interm.PatchedKey = request.GetPatchedKey();

        auto it = Index.find(request.GetOriginalKey());
        if (it == Index.end()) {
            TStringStream str;
            str << "KeyValue# " << TabletId;
            str << " Missing Value/PayloadId in CmdPatch(" << patchIdx << ") Marker# KV39";
            ReplyError(ctx, str.Str(), NMsgBusProxy::MSTATUS_INTERNALERROR, NKikimrKeyValue::Statuses::RSTATUS_INTERNAL_ERROR, intermediate);
            return true;
        }

        auto &[key, indexRecord] = *it;
        if (indexRecord.Chain.size() > 1) {
            TStringStream str;
            str << "KeyValue# " << TabletId;
            str << " Patching work only with one-blob values instead of many-blob values in CmdPatch(" << patchIdx << ") Marker# KV62";
            ReplyError(ctx, str.Str(), NMsgBusProxy::MSTATUS_INTERNALERROR, NKikimrKeyValue::Statuses::RSTATUS_INTERNAL_ERROR, intermediate);
            return true;
        }

        if (indexRecord.Chain.size() == 0 || indexRecord.Chain[0].IsInline()) {
            TStringStream str;
            str << "KeyValue# " << TabletId;
            str << " Patching only with one-blob values instead of none-blob values in CmdPatch(" << patchIdx << ") Marker# KV63";
            ReplyError(ctx, str.Str(), NMsgBusProxy::MSTATUS_INTERNALERROR, NKikimrKeyValue::Statuses::RSTATUS_INTERNAL_ERROR, intermediate);
            return true;
        }

        interm.Diffs.resize(request.DiffsSize());
        for (ui32 diffIdx = 0; diffIdx < request.DiffsSize(); ++diffIdx) {
            auto &diff = request.GetDiffs(diffIdx);
            switch (diff.GetDataCase()) {
                case NKikimrClient::TKeyValueRequest::TCmdPatch::TDiff::kValue:
                    interm.Diffs[diffIdx].Buffer = TRope(diff.GetValue());
                    break;

                case NKikimrClient::TKeyValueRequest::TCmdPatch::TDiff::kPayloadId:
                    interm.Diffs[diffIdx].Buffer = ev.GetPayload(diff.GetPayloadId());
                    break;

                case NKikimrClient::TKeyValueRequest::TCmdPatch::TDiff::DATA_NOT_SET: {
                    TStringStream str;
                    str << "KeyValue# " << TabletId;
                    str << " Missing Value/PayloadId in CmdPatch(" << patchIdx << ") Diff(" << diffIdx << ") Marker# KV38";
                    ReplyError(ctx, str.Str(), NMsgBusProxy::MSTATUS_INTERNALERROR, NKikimrKeyValue::Statuses::RSTATUS_INTERNAL_ERROR, intermediate);
                    return true;
                }
            }
            interm.Diffs[diffIdx].Offset = diff.GetOffset(); 
        }

        ui32 storageChannelIdx = BLOB_CHANNEL;
        if (request.HasStorageChannel()) {
            auto storageChannel = request.GetStorageChannel();
            ui32 storageChannelOffset = (ui32)storageChannel;
            
            if (storageChannelOffset == NKikimrClient::TKeyValueRequest::INLINE) {
                TStringStream str;
                str << "KeyValue# " << TabletId;
                str << " Patching blob can't be store in inline channel CmdPatch(" << patchIdx << ") Marker# KV91";
                ReplyError(ctx, str.Str(), NMsgBusProxy::MSTATUS_INTERNALERROR, NKikimrKeyValue::Statuses::RSTATUS_INTERNAL_ERROR, intermediate);
                return true;
            }

            storageChannelIdx = storageChannelOffset + BLOB_CHANNEL;
            ui32 endChannel = info->Channels.size();
            if (storageChannelIdx >= endChannel) {
                storageChannelIdx = BLOB_CHANNEL;
                LOG_INFO_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletId
                        << " CmdPatch StorageChannel# " << storageChannelOffset
                        << " does not exist, using MAIN");
            }
        }

        interm.OriginalBlobId = indexRecord.Chain[0].LogoBlobId;
        interm.PatchedBlobId = TLogoBlobID(0, 0, 0, storageChannelIdx, interm.OriginalBlobId.BlobSize(), 0);
    }
    return false;
}


TKeyValueState::TPrepareResult TKeyValueState::InitGetStatusCommand(TIntermediate::TGetStatus &cmd,
        NKikimrClient::TKeyValueRequest::EStorageChannel storageChannel, const TTabletStorageInfo *info)
{
    TString msg;
    if (storageChannel == NKikimrClient::TKeyValueRequest::INLINE) {
        cmd.StorageChannel = storageChannel;
        cmd.GroupId = 0;
        cmd.Status = NKikimrProto::OK;
        cmd.StatusFlags = TStorageStatusFlags(ui32(NKikimrBlobStorage::StatusIsValid));
        if (GetIsTabletYellowMove()) {
            cmd.StatusFlags.Merge(ui32(NKikimrBlobStorage::StatusDiskSpaceLightYellowMove));
        }
        if (GetIsTabletYellowStop()) {
            cmd.StatusFlags.Merge(ui32(NKikimrBlobStorage::StatusDiskSpaceYellowStop));
        }
    } else {
        ui32 storageChannelOffset = (ui32)storageChannel;
        ui32 storageChannelIdx = storageChannelOffset + BLOB_CHANNEL;
        ui32 endChannel = info->Channels.size();
        if (storageChannelIdx >= endChannel) {
            storageChannelIdx = BLOB_CHANNEL;
            msg = TStringBuilder() << "KeyValue# " << TabletId
                    << " CmdGetStatus StorageChannel# " << storageChannelOffset
                    << " does not exist, using MAIN";
        }

        cmd.StorageChannel = storageChannel;
        cmd.GroupId = info->GroupFor(storageChannelIdx, ExecutorGeneration);
        cmd.Status = NKikimrProto::UNKNOWN;
    }
    return {false, msg};
}

bool TKeyValueState::PrepareCmdGetStatus(const TActorContext &ctx, NKikimrClient::TKeyValueRequest &kvRequest,
        THolder<TIntermediate> &intermediate, const TTabletStorageInfo *info) {
    intermediate->GetStatuses.resize(kvRequest.CmdGetStatusSize());
    for (ui32 i = 0; i < kvRequest.CmdGetStatusSize(); ++i) {
        auto& request = kvRequest.GetCmdGetStatus(i);
        auto& interm = intermediate->GetStatuses[i];

        NKikimrClient::TKeyValueRequest::EStorageChannel storageChannel =
            NKikimrClient::TKeyValueRequest::MAIN;

        if (request.HasStorageChannel()) {
            storageChannel = request.GetStorageChannel();
        }
        TPrepareResult result = InitGetStatusCommand(interm, storageChannel, info);
        if (result.ErrorMsg && !result.WithError) {
            LOG_INFO_S(ctx, NKikimrServices::KEYVALUE, result.ErrorMsg  << " Marker# KV76");
        }
    }
    return false;
}


bool TKeyValueState::PrepareCmdCopyRange(const TActorContext& ctx, NKikimrClient::TKeyValueRequest& kvRequest,
        THolder<TIntermediate>& intermediate) {
    for (ui32 i = 0; i < kvRequest.CmdCopyRangeSize(); ++i) {
        auto& request = kvRequest.GetCmdCopyRange(i);
        intermediate->Commands.emplace_back(TIntermediate::TCopyRange());
        auto& interm = std::get<TIntermediate::TCopyRange>(intermediate->Commands.back());

        if ((!request.HasPrefixToAdd() || request.GetPrefixToAdd().empty()) &&
                (!request.HasPrefixToRemove() || request.GetPrefixToRemove().empty())) {
            TStringStream str;
            str << "KeyValue# " << TabletId
                << " Missing or empty both PrefixToAdd and PrefixToRemove in CmdCopyRange(" << i << ") Marker# KV11";
            ReplyError(ctx, str.Str(), NMsgBusProxy::MSTATUS_INTERNALERROR, NKikimrKeyValue::Statuses::RSTATUS_INTERNAL_ERROR, intermediate);
            return true;
        }
        if (!request.HasRange()) {
            interm.Range = TKeyRange::WholeDatabase();
        } else if (!ConvertRange(request.GetRange(), &interm.Range, ctx, intermediate, "CmdCopyRange", i)) {
            return false;
        }
        interm.PrefixToAdd = request.HasPrefixToAdd() ? request.GetPrefixToAdd() : TString();
        interm.PrefixToRemove = request.HasPrefixToRemove() ? request.GetPrefixToRemove() : TString();
    }
    return false;
}

bool TKeyValueState::PrepareCmdConcat(const TActorContext& ctx, NKikimrClient::TKeyValueRequest& kvRequest,
        THolder<TIntermediate>& intermediate) {
    for (ui32 i = 0; i < kvRequest.CmdConcatSize(); ++i) {
        auto& request = kvRequest.GetCmdConcat(i);
        intermediate->Commands.emplace_back(TIntermediate::TConcat());
        auto& interm = std::get<TIntermediate::TConcat>(intermediate->Commands.back());

        if (!request.HasOutputKey()) {
            TStringStream str;
            str << "KeyValue# " << TabletId << " Missing OutputKey in CmdConcat(" << i << ") Marker# KV12";
            ReplyError(ctx, str.Str(), NMsgBusProxy::MSTATUS_INTERNALERROR, NKikimrKeyValue::Statuses::RSTATUS_INTERNAL_ERROR, intermediate);
            return true;
        }

        const auto& inputKeys = request.GetInputKeys();
        interm.InputKeys = {inputKeys.begin(), inputKeys.end()};
        interm.OutputKey = request.GetOutputKey();
        interm.KeepInputs = request.HasKeepInputs() && request.GetKeepInputs();
    }
    return false;
}

bool TKeyValueState::PrepareCmdTrimLeakedBlobs(const TActorContext& /*ctx*/,
        NKikimrClient::TKeyValueRequest& kvRequest, THolder<TIntermediate>& intermediate,
        const TTabletStorageInfo *info) {
    if (kvRequest.HasCmdTrimLeakedBlobs()) {
        const auto& request = kvRequest.GetCmdTrimLeakedBlobs();
        TIntermediate::TTrimLeakedBlobs interm;
        interm.MaxItemsToTrim = request.GetMaxItemsToTrim();
        for (const auto& channel : info->Channels) {
            if (channel.Channel >= BLOB_CHANNEL) {
                for (const auto& history : channel.History) {
                    interm.ChannelGroupMap.emplace(channel.Channel, history.GroupID);
                }
            }
        }
        intermediate->TrimLeakedBlobs = std::move(interm);
    }
    return false;
}

bool TKeyValueState::PrepareCmdSetExecutorFastLogPolicy(const TActorContext & /*ctx*/,
        NKikimrClient::TKeyValueRequest &kvRequest, THolder<TIntermediate> &intermediate,
        const TTabletStorageInfo * /*info*/) {
    if (kvRequest.HasCmdSetExecutorFastLogPolicy()) {
        const auto& request = kvRequest.GetCmdSetExecutorFastLogPolicy();
        TIntermediate::TSetExecutorFastLogPolicy interm;
        interm.IsAllowed = request.GetIsAllowed();
        intermediate->SetExecutorFastLogPolicy = interm;
    }
    return false;
}

using TPrepareResult = TKeyValueState::TPrepareResult;

TPrepareResult TKeyValueState::PrepareOneCmd(const TCommand::Rename &request, THolder<TIntermediate> &intermediate) {
    intermediate->Commands.emplace_back(TIntermediate::TRename());
    auto &cmd = std::get<TIntermediate::TRename>(intermediate->Commands.back());
    cmd.OldKey = request.old_key();
    cmd.NewKey = request.new_key();
    return {};
}

TPrepareResult TKeyValueState::PrepareOneCmd(const TCommand::Concat &request, THolder<TIntermediate> &intermediate) {
    intermediate->Commands.emplace_back(TIntermediate::TConcat());
    auto &cmd = std::get<TIntermediate::TConcat>(intermediate->Commands.back());
    auto inputKeys = request.input_keys();
    cmd.InputKeys.insert(cmd.InputKeys.end(), inputKeys.begin(), inputKeys.end());

    cmd.OutputKey = request.output_key();
    cmd.KeepInputs = request.keep_inputs();
    return {};
}

TPrepareResult TKeyValueState::PrepareOneCmd(const TCommand::CopyRange &request, THolder<TIntermediate> &intermediate) {
    intermediate->Commands.emplace_back(TIntermediate::TCopyRange());
    auto &cmd = std::get<TIntermediate::TCopyRange>(intermediate->Commands.back());
    auto convResult = ConvertRange(request.range(), &cmd.Range, "CopyRange");
    if (convResult.WithError) {
        return {true, convResult.ErrorMsg};
    }
    cmd.PrefixToAdd = request.prefix_to_add();
    cmd.PrefixToRemove = request.prefix_to_remove();
    return {};
}

TPrepareResult TKeyValueState::PrepareOneCmd(const TCommand::Write &request, THolder<TIntermediate> &intermediate,
        const TTabletStorageInfo *info)
{
    intermediate->Commands.emplace_back(TIntermediate::TWrite());
    auto &cmd = std::get<TIntermediate::TWrite>(intermediate->Commands.back());
    cmd.Key = request.key();
    cmd.Data = TRope(request.value());
    switch (request.tactic()) {
    case TCommand::Write::TACTIC_MIN_LATENCY:
        cmd.Tactic = TEvBlobStorage::TEvPut::TacticMinLatency;
        break;
    case TCommand::Write::TACTIC_MAX_THROUGHPUT:
        cmd.Tactic = TEvBlobStorage::TEvPut::TacticMaxThroughput;
        break;
    default:
        cmd.Tactic = TEvBlobStorage::TEvPut::TacticDefault;
        break;
    }

    cmd.HandleClass = NKikimrBlobStorage::UserData;
    if (request.priority() == NKikimrKeyValue::Priorities::PRIORITY_BACKGROUND) {
        cmd.HandleClass = NKikimrBlobStorage::AsyncBlob;
    }

    bool isInline = false;
    ui32 storageChannelIdx = BLOB_CHANNEL;
    ui32 storageChannel = request.storage_channel();
    if (!storageChannel) {
        storageChannel = MainStorageChannelInPublicApi;
    }
    ui32 storageChannelOffset = storageChannel - MainStorageChannelInPublicApi;

    if (storageChannel == InlineStorageChannelInPublicApi) {
        isInline = true;
    } else {
        storageChannelIdx = storageChannelOffset + BLOB_CHANNEL;
        ui32 endChannel = info->Channels.size();
        if (storageChannelIdx >= endChannel) {
            storageChannelIdx = BLOB_CHANNEL;
        }
    }
    SplitIntoBlobs(cmd, isInline, storageChannelIdx);
    return {};
}

TPrepareResult TKeyValueState::PrepareOneCmd(const TCommand::DeleteRange &request, THolder<TIntermediate> &intermediate,
        const TActorContext &ctx)
{
    intermediate->Commands.emplace_back(TIntermediate::TDelete());
    auto &cmd = std::get<TIntermediate::TDelete>(intermediate->Commands.back());
    auto convResult = ConvertRange(request.range(), &cmd.Range, "DeleteRange");
    if (convResult.WithError) {
        return {true, convResult.ErrorMsg};
    }
    ui32 nToDelete = 0;
    TraverseRange(cmd.Range, [&](TIndex::iterator it) {
        Y_UNUSED(it);
        nToDelete++;
    });
    // The use of >, not >= is important here.
    if (nToDelete > DeletesPerRequestLimit && !AppData(ctx)->AllowHugeKeyValueDeletes) {
        TStringBuilder str;
        str << "KeyValue# " << TabletId;
        str << " Can't delete Range, in DeleteRange, total limit of deletions per request ("
            << DeletesPerRequestLimit << ") reached, Marker# KV90";
        TString msg = str;
        ReplyError<TEvKeyValue::TEvExecuteTransactionResponse>(ctx, msg,
                NKikimrKeyValue::Statuses::RSTATUS_INTERNAL_ERROR, intermediate, nullptr);
        return {true, msg};
    }
    return {};
}

TPrepareResult TKeyValueState::PrepareOneCmd(const TCommand &request, THolder<TIntermediate> &intermediate,
        const TTabletStorageInfo *info, const TActorContext &ctx)
{
    switch (request.action_case()) {
    case NKikimrKeyValue::ExecuteTransactionRequest::Command::ACTION_NOT_SET:
        return {true, "Command not specified Marker# KV68"};
    case NKikimrKeyValue::ExecuteTransactionRequest::Command::kDeleteRange:
        return PrepareOneCmd(request.delete_range(), intermediate, ctx);
    case NKikimrKeyValue::ExecuteTransactionRequest::Command::kRename:
        return PrepareOneCmd(request.rename(), intermediate);
    case NKikimrKeyValue::ExecuteTransactionRequest::Command::kCopyRange:
        return PrepareOneCmd(request.copy_range(), intermediate);
    case NKikimrKeyValue::ExecuteTransactionRequest::Command::kConcat:
        return PrepareOneCmd(request.concat(), intermediate);
    case NKikimrKeyValue::ExecuteTransactionRequest::Command::kWrite:
        return PrepareOneCmd(request.write(), intermediate, info);
    }
}

TPrepareResult TKeyValueState::PrepareCommands(NKikimrKeyValue::ExecuteTransactionRequest &kvRequest,
    THolder<TIntermediate> &intermediate, const TTabletStorageInfo *info, const TActorContext &ctx)
{
    for (i32 idx = 0; idx < kvRequest.commands_size(); ++idx) {
        auto &cmd = kvRequest.commands(idx);
        TPrepareResult result = PrepareOneCmd(cmd, intermediate, info, ctx);
        if (cmd.has_write()) {
            intermediate->WriteIndices.push_back(idx);
        }
        if (result.WithError) {
            return result;
        }
    }
    if (intermediate->EvType == TEvKeyValue::TEvExecuteTransaction::EventType) {
        intermediate->ExecuteTransactionResponse.set_status(NKikimrKeyValue::Statuses::RSTATUS_OK);
    }
    return {};
}

NKikimrKeyValue::Statuses::ReplyStatus ConvertStatus(NMsgBusProxy::EResponseStatus status) {
    switch (status) {
    case NMsgBusProxy::MSTATUS_ERROR:
        return NKikimrKeyValue::Statuses::RSTATUS_ERROR;
    case NMsgBusProxy::MSTATUS_TIMEOUT:
        return NKikimrKeyValue::Statuses::RSTATUS_TIMEOUT;
    case NMsgBusProxy::MSTATUS_REJECTED:
        return NKikimrKeyValue::Statuses::RSTATUS_WRONG_LOCK_GENERATION;
    case NMsgBusProxy::MSTATUS_INTERNALERROR:
        return NKikimrKeyValue::Statuses::RSTATUS_INTERNAL_ERROR;
    default:
        return NKikimrKeyValue::Statuses::RSTATUS_INTERNAL_ERROR;
    };
}

void TKeyValueState::ReplyError(const TActorContext &ctx, TString errorDescription,
        NMsgBusProxy::EResponseStatus oldStatus, NKikimrKeyValue::Statuses::ReplyStatus newStatus,
        THolder<TIntermediate> &intermediate, const TTabletStorageInfo *info) {
    LOG_INFO_S(ctx, NKikimrServices::KEYVALUE, errorDescription);
    Y_ABORT_UNLESS(!intermediate->IsReplied);

    if (intermediate->EvType == TEvKeyValue::TEvRequest::EventType) {
        THolder<TEvKeyValue::TEvResponse> response(new TEvKeyValue::TEvResponse);
        if (intermediate->HasCookie) {
            response->Record.SetCookie(intermediate->Cookie);
        }
        response->Record.SetErrorReason(errorDescription);
        response->Record.SetStatus(oldStatus);
        ResourceMetrics->Network.Increment(response->Record.ByteSize());
        intermediate->IsReplied = true;
        ctx.Send(intermediate->RespondTo, response.Release());
    }
    if (intermediate->EvType == TEvKeyValue::TEvExecuteTransaction::EventType) {
        ReplyError<TEvKeyValue::TEvExecuteTransactionResponse>(ctx, errorDescription,
                newStatus, intermediate, info);
        return;
    }
    if (intermediate->EvType == TEvKeyValue::TEvGetStorageChannelStatus::EventType) {
        ReplyError<TEvKeyValue::TEvGetStorageChannelStatusResponse>(ctx, errorDescription,
                newStatus, intermediate, info);
        return;
    }
    if (intermediate->EvType == TEvKeyValue::TEvRead::EventType) {
        ReplyError<TEvKeyValue::TEvReadResponse>(ctx, errorDescription,
                newStatus, intermediate, info);
        return;
    }
    if (intermediate->EvType == TEvKeyValue::TEvReadRange::EventType) {
        ReplyError<TEvKeyValue::TEvReadRangeResponse>(ctx, errorDescription,
                newStatus, intermediate, info);
        return;
    }

    if (info) {
        intermediate->UpdateStat();
        OnRequestComplete(intermediate->RequestUid, intermediate->CreatedAtGeneration, intermediate->CreatedAtStep,
                ctx, info, oldStatus, intermediate->Stat);
    } else { //metrics change report in OnRequestComplete is not done
        ResourceMetrics->TryUpdate(ctx);
        RequestInputTime.erase(intermediate->RequestUid);
    }
}

bool TKeyValueState::PrepareReadRequest(const TActorContext &ctx, TEvKeyValue::TEvRead::TPtr &ev,
        THolder<TIntermediate> &intermediate, TRequestType::EType *outRequestType)
{
    LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletId << " PrepareReadRequest Marker# KV53");

    NKikimrKeyValue::ReadRequest &request = ev->Get()->Record;
    StoredState.SetChannelGeneration(ExecutorGeneration);
    StoredState.SetChannelStep(NextLogoBlobStep - 1);

    intermediate.Reset(new TIntermediate(ev->Sender, ctx.SelfID,
            StoredState.GetChannelGeneration(), StoredState.GetChannelStep(), TRequestType::ReadOnly, std::move(ev->TraceId)));

    intermediate->HasCookie = true;
    intermediate->Cookie = request.cookie();

    intermediate->RequestUid = NextRequestUid;
    ++NextRequestUid;
    RequestInputTime[intermediate->RequestUid] = TAppData::TimeProvider->Now();
    intermediate->EvType = TEvKeyValue::TEvRead::EventType;

    bool isInlineOnly = true;
    intermediate->ReadCommand = TIntermediate::TRead();
    auto &response = std::get<TIntermediate::TRead>(*intermediate->ReadCommand);
    response.Key = request.key();

    if (CheckDeadline(ctx, ev->Get(), intermediate)) {
        return false;
    }

    if (CheckGeneration(ctx, ev->Get(), intermediate)) {
        return false;
    }

    auto it = Index.find(request.key());
    if (it == Index.end()) {
        response.Status = NKikimrProto::NODATA;
        response.Message = "No such key Marker# KV55";
        ReplyError<TEvKeyValue::TEvReadResponse>(ctx, response.Message,
                NKikimrKeyValue::Statuses::RSTATUS_NOT_FOUND, intermediate);
        return false;
    }
    bool isOverRun = PrepareOneRead<NKikimrKeyValue::Priorities, true, ReadResultSizeEstimationNewApi>(
            it->first, it->second, request.offset(), request.size(), request.priority(), request.limit_bytes(),
            intermediate, response, isInlineOnly);

    if (isInlineOnly) {
        *outRequestType = TRequestType::ReadOnlyInline;
        intermediate->Stat.RequestType = *outRequestType;
    }
    intermediate->IsTruncated = isOverRun;
    return true;
}

bool TKeyValueState::PrepareReadRangeRequest(const TActorContext &ctx, TEvKeyValue::TEvReadRange::TPtr &ev,
        THolder<TIntermediate> &intermediate, TRequestType::EType *outRequestType)
{
    LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletId << " PrepareReadRangeRequest Marker# KV57");

    NKikimrKeyValue::ReadRangeRequest &request = ev->Get()->Record;
    StoredState.SetChannelGeneration(ExecutorGeneration);
    StoredState.SetChannelStep(NextLogoBlobStep - 1);

    TRequestType::EType requestType = TRequestType::ReadOnly;
    intermediate.Reset(new TIntermediate(ev->Sender, ctx.SelfID,
        StoredState.GetChannelGeneration(), StoredState.GetChannelStep(), requestType, std::move(ev->TraceId)));

    intermediate->HasCookie = true;
    intermediate->Cookie = request.cookie();

    intermediate->RequestUid = NextRequestUid;
    ++NextRequestUid;
    RequestInputTime[intermediate->RequestUid] = TAppData::TimeProvider->Now();
    intermediate->EvType = TEvKeyValue::TEvReadRange::EventType;
    intermediate->TotalSize = ReadRangeRequestMetaDataSizeEstimation;

    intermediate->ReadCommand = TIntermediate::TRangeRead();
    auto &response = std::get<TIntermediate::TRangeRead>(*intermediate->ReadCommand);

    if (CheckDeadline(ctx, ev->Get(), intermediate)) {
        response.Status = NKikimrProto::ERROR;
        return false;
    }

    if (CheckGeneration(ctx, ev->Get(), intermediate)) {
        response.Status = NKikimrProto::ERROR;
        return false;
    }

    TKeyRange range;
    auto convResult = ConvertRange(request.range(), &range, "ReadRange");
    if (convResult.WithError) {
        response.Status = NKikimrProto::ERROR;
        return false;
    }
    response.Status = NKikimrProto::OK;

    ui64 cmdLimitBytes = request.limit_bytes();
    if (!cmdLimitBytes) {
        cmdLimitBytes = Max<ui64>();
    }
    bool includeData = request.include_data();
    ui8 priority = request.priority();
    response.LimitBytes = cmdLimitBytes;
    response.IncludeData = includeData;

    bool isInlineOnly = true;
    auto processOneCmdReadRange = ProcessOneCmdReadRange<NKikimrKeyValue::Priorities, true,
            KeyValuePairSizeEstimationNewApi, KeyInfoSizeEstimation>;
    processOneCmdReadRange(this, range, cmdLimitBytes, includeData, priority, response, intermediate, &isInlineOnly);

    if (isInlineOnly) {
        *outRequestType = TRequestType::ReadOnlyInline;
        intermediate->Stat.RequestType = *outRequestType;
    }
    return true;
}


bool TKeyValueState::PrepareExecuteTransactionRequest(const TActorContext &ctx,
        TEvKeyValue::TEvExecuteTransaction::TPtr &ev, THolder<TIntermediate> &intermediate,
        const TTabletStorageInfo *info)
{
    LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletId
            << " PrepareExecuteTransactionRequest Marker# KV72");

    NKikimrKeyValue::ExecuteTransactionRequest &request = ev->Get()->Record;
    StoredState.SetChannelGeneration(ExecutorGeneration);
    StoredState.SetChannelStep(NextLogoBlobStep - 1);

    TRequestType::EType requestType = TRequestType::WriteOnly;
    intermediate.Reset(new TIntermediate(ev->Sender, ctx.SelfID,
        StoredState.GetChannelGeneration(), StoredState.GetChannelStep(), requestType, std::move(ev->TraceId)));

    intermediate->HasCookie = true;
    intermediate->Cookie = request.cookie();
    intermediate->EvType = TEvKeyValue::TEvExecuteTransaction::EventType;
    intermediate->ExecuteTransactionResponse.set_cookie(request.cookie());

    intermediate->RequestUid = NextRequestUid;
    ++NextRequestUid;
    RequestInputTime[intermediate->RequestUid] = TAppData::TimeProvider->Now();

    if (CheckDeadline(ctx, ev->Get(), intermediate)) {
        return false;
    }

    if (CheckGeneration(ctx, ev->Get(), intermediate)) {
        return false;
    }

    TPrepareResult result = PrepareCommands(request, intermediate, info, ctx);

    if (result.WithError) {
        DropRefCountsOnError(intermediate->RefCountsIncr, false, ctx);
        Y_ABORT_UNLESS(intermediate->RefCountsIncr.empty());

        LOG_ERROR_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletId
                << " PrepareExecuteTransactionRequest return flase, Marker# KV73"
                << " Submsg# " << result.ErrorMsg);
        return false;
    }

    return true;
}


TKeyValueState::TPrepareResult TKeyValueState::PrepareOneGetStatus(TIntermediate::TGetStatus &cmd,
        ui64 publicStorageChannel, const TTabletStorageInfo *info)
{
    NKikimrClient::TKeyValueRequest::EStorageChannel storageChannel = NKikimrClient::TKeyValueRequest::MAIN;
    if (publicStorageChannel == 1) {
        storageChannel = NKikimrClient::TKeyValueRequest::INLINE;
    } else if (publicStorageChannel) {
        ui32 storageChannelIdx = BLOB_CHANNEL + publicStorageChannel - MainStorageChannelInPublicApi;
        storageChannel = NKikimrClient::TKeyValueRequest::EStorageChannel(storageChannelIdx);
    }
    return InitGetStatusCommand(cmd, storageChannel, info);;
}


bool TKeyValueState::PrepareGetStorageChannelStatusRequest(const TActorContext &ctx, TEvKeyValue::TEvGetStorageChannelStatus::TPtr &ev,
        THolder<TIntermediate> &intermediate, const TTabletStorageInfo *info)
{
    LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletId << " PrepareGetStorageChannelStatusRequest Marker# KV78");

    NKikimrKeyValue::GetStorageChannelStatusRequest &request = ev->Get()->Record;
    StoredState.SetChannelGeneration(ExecutorGeneration);
    StoredState.SetChannelStep(NextLogoBlobStep - 1);

    TRequestType::EType requestType = TRequestType::ReadOnly;
    intermediate.Reset(new TIntermediate(ev->Sender, ctx.SelfID,
        StoredState.GetChannelGeneration(), StoredState.GetChannelStep(), requestType, std::move(ev->TraceId)));

    intermediate->RequestUid = NextRequestUid;
    ++NextRequestUid;
    RequestInputTime[intermediate->RequestUid] = TAppData::TimeProvider->Now();
    intermediate->EvType = TEvKeyValue::TEvGetStorageChannelStatus::EventType;

    if (CheckDeadline(ctx, ev->Get(), intermediate)) {
        return false;
    }

    if (CheckGeneration(ctx, ev->Get(), intermediate)) {
        return false;
    }

    intermediate->GetStatuses.resize(request.storage_channel_size());
    for (i32 idx = 0; idx < request.storage_channel_size(); ++idx) {
        TPrepareResult result = PrepareOneGetStatus(intermediate->GetStatuses[idx], request.storage_channel(idx), info);
        if (result.ErrorMsg && !result.WithError) {
            LOG_INFO_S(ctx, NKikimrServices::KEYVALUE, result.ErrorMsg  << " Marker# KV77");
        }
    }
    return true;
}

bool TKeyValueState::PrepareAcquireLockRequest(const TActorContext &ctx, TEvKeyValue::TEvAcquireLock::TPtr &ev,
        THolder<TIntermediate> &intermediate)
{
    LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletId << " PrepareAcquireLockRequest Marker# KV79");

    StoredState.SetChannelGeneration(ExecutorGeneration);
    StoredState.SetChannelStep(NextLogoBlobStep - 1);

    TRequestType::EType requestType = TRequestType::ReadOnlyInline;
    intermediate.Reset(new TIntermediate(ev->Sender, ctx.SelfID,
        StoredState.GetChannelGeneration(), StoredState.GetChannelStep(), requestType, std::move(ev->TraceId)));

    intermediate->RequestUid = NextRequestUid;
    ++NextRequestUid;
    RequestInputTime[intermediate->RequestUid] = TAppData::TimeProvider->Now();
    intermediate->EvType = TEvKeyValue::TEvAcquireLock::EventType;
    intermediate->HasIncrementGeneration = true;
    return true;
}

void RegisterReadRequestActor(const TActorContext &ctx, THolder<TIntermediate> &&intermediate,
        const TTabletStorageInfo *info, ui32 tabletGeneration)
{
    ctx.RegisterWithSameMailbox(CreateKeyValueStorageReadRequest(std::move(intermediate), info, tabletGeneration));
}

void TKeyValueState::RegisterRequestActor(const TActorContext &ctx, THolder<TIntermediate> &&intermediate,
        const TTabletStorageInfo *info, ui32 tabletGeneration)
{
    auto fixWrite = [&](TIntermediate::TWrite& write) {
        for (auto& logoBlobId : write.LogoBlobIds) {
            Y_ABORT_UNLESS(logoBlobId.TabletID() == 0);
            logoBlobId = AllocateLogoBlobId(logoBlobId.BlobSize(), logoBlobId.Channel(), intermediate->RequestUid);
            ui32 newRefCount = ++RefCounts[logoBlobId];
            Y_ABORT_UNLESS(newRefCount == 1);
            intermediate->RefCountsIncr.emplace_back(logoBlobId, true);
        }
    };

    auto fixPatch = [&](TIntermediate::TPatch& patch) {
        Y_ABORT_UNLESS(patch.PatchedBlobId.TabletID() == 0);
        patch.PatchedBlobId = AllocatePatchedLogoBlobId(patch.PatchedBlobId.BlobSize(), patch.PatchedBlobId.Channel(), patch.OriginalBlobId, intermediate->RequestUid);
        ui32 newRefCount = ++RefCounts[patch.PatchedBlobId];
        Y_ABORT_UNLESS(newRefCount == 1);
        intermediate->RefCountsIncr.emplace_back(patch.PatchedBlobId, true);

        LOG_INFO_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletId
            << " PatchedKey# " << patch.PatchedKey << " BlobId# " << patch.PatchedBlobId);
    };

    for (auto& write : intermediate->Writes) {
        fixWrite(write);
    }
    for (auto& patch : intermediate->Patches) {
        fixPatch(patch);
    }
    for (auto& cmd : intermediate->Commands) {
        if (auto *write = std::get_if<TIntermediate::TWrite>(&cmd)) {
            fixWrite(*write);
        }
        if (auto *patch = std::get_if<TIntermediate::TPatch>(&cmd)) {
            fixPatch(*patch);
        }
    }

    ctx.RegisterWithSameMailbox(CreateKeyValueStorageRequest(std::move(intermediate), info, tabletGeneration));
}

void TKeyValueState::ProcessPostponedIntermediate(const TActorContext& ctx, THolder<TIntermediate> &&intermediate,
            const TTabletStorageInfo *info)
{
    switch(intermediate->EvType) {
    case TEvKeyValue::TEvRequest::EventType:
        return RegisterRequestActor(ctx, std::move(intermediate), info, ExecutorGeneration);
    case TEvKeyValue::TEvRead::EventType:
    case TEvKeyValue::TEvReadRange::EventType:
        return RegisterReadRequestActor(ctx, std::move(intermediate), info, ExecutorGeneration);
    default:
        Y_FAIL_S("Unexpected event type# " << intermediate->EvType);
    }
}

void TKeyValueState::ProcessPostponedTrims(const TActorContext& ctx, const TTabletStorageInfo *info) {
    if (!IsCollectEventSent) {
        for (auto& interm : CmdTrimLeakedBlobsPostponed) {
            CmdTrimLeakedBlobsUids.insert(interm->RequestUid);
            RegisterRequestActor(ctx, std::move(interm), info, ExecutorGeneration);
        }
        CmdTrimLeakedBlobsPostponed.clear();
    }
}

void TKeyValueState::OnEvReadRequest(TEvKeyValue::TEvRead::TPtr &ev, const TActorContext &ctx,
        const TTabletStorageInfo *info)
{
    THolder<TIntermediate> intermediate;

    ResourceMetrics->Network.Increment(ev->Get()->Record.ByteSize());
    ResourceMetrics->TryUpdate(ctx);

    TRequestType::EType requestType = TRequestType::ReadOnly;
    CountRequestIncoming(requestType);

    if (PrepareReadRequest(ctx, ev, intermediate, &requestType)) {
        if (requestType == TRequestType::ReadOnlyInline) {
            LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletId
                << " Create storage inline read request, Marker# KV49");
            RegisterReadRequestActor(ctx, std::move(intermediate), info, ExecutorGeneration);
            ++RoInlineIntermediatesInFlight;
        } else {
            if (IntermediatesInFlight < IntermediatesInFlightLimit) {
                LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletId
                    << " Create storage read request, Marker# KV54");
                RegisterReadRequestActor(ctx, std::move(intermediate), info, ExecutorGeneration);
                ++IntermediatesInFlight;
            } else {
                LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletId
                    << " Enqueue storage read request, Marker# KV56");
                PostponeIntermediate<TEvKeyValue::TEvRead>(std::move(intermediate));
            }
        }
        CountRequestTakeOffOrEnqueue(requestType);
    } else {
        intermediate->UpdateStat();
        CountRequestOtherError(requestType);
    }
}

void TKeyValueState::OnEvReadRangeRequest(TEvKeyValue::TEvReadRange::TPtr &ev, const TActorContext &ctx,
        const TTabletStorageInfo *info)
{
    THolder<TIntermediate> intermediate;

    ResourceMetrics->Network.Increment(ev->Get()->Record.ByteSize());
    ResourceMetrics->TryUpdate(ctx);

    TRequestType::EType requestType = TRequestType::ReadOnly;
    CountRequestIncoming(requestType);

    if (PrepareReadRangeRequest(ctx, ev, intermediate, &requestType)) {
        if (requestType == TRequestType::ReadOnlyInline) {
            LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletId
                << " Create storage inline read range request, Marker# KV58");
            RegisterReadRequestActor(ctx, std::move(intermediate), info, ExecutorGeneration);
            ++RoInlineIntermediatesInFlight;
        } else {
            if (IntermediatesInFlight < IntermediatesInFlightLimit) {
                LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletId
                    << " Create storage read range request, Marker# KV66");
                RegisterReadRequestActor(ctx, std::move(intermediate), info, ExecutorGeneration);
                ++IntermediatesInFlight;
            } else {
                LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletId
                    << " Enqueue storage read range request, Marker# KV59");
                PostponeIntermediate<TEvKeyValue::TEvReadRange>(std::move(intermediate));
            }
        }
        CountRequestTakeOffOrEnqueue(requestType);
    } else {
        intermediate->UpdateStat();
        CountRequestOtherError(requestType);
    }
    CountRequestTakeOffOrEnqueue(requestType);
}

void TKeyValueState::OnEvExecuteTransaction(TEvKeyValue::TEvExecuteTransaction::TPtr &ev, const TActorContext &ctx,
        const TTabletStorageInfo *info)
{
    THolder<TIntermediate> intermediate;

    ResourceMetrics->Network.Increment(ev->Get()->Record.ByteSize());
    ResourceMetrics->TryUpdate(ctx);

    TRequestType::EType requestType = TRequestType::WriteOnly;
    CountRequestIncoming(requestType);

    if (PrepareExecuteTransactionRequest(ctx, ev, intermediate, info)) {
        LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletId
            << " Create storage request for WO, Marker# KV67");
        RegisterRequestActor(ctx, std::move(intermediate), info, ExecutorGeneration);

        CountRequestTakeOffOrEnqueue(requestType);
    } else {
        intermediate->UpdateStat();
        CountRequestOtherError(requestType);
        CancelInFlight(intermediate->RequestUid);
    }
}

void TKeyValueState::OnEvGetStorageChannelStatus(TEvKeyValue::TEvGetStorageChannelStatus::TPtr &ev, const TActorContext &ctx,
        const TTabletStorageInfo *info)
{
    THolder<TIntermediate> intermediate;

    ResourceMetrics->Network.Increment(ev->Get()->Record.ByteSize());
    ResourceMetrics->TryUpdate(ctx);

    TRequestType::EType requestType = TRequestType::ReadOnlyInline;
    CountRequestIncoming(requestType);

    if (PrepareGetStorageChannelStatusRequest(ctx, ev, intermediate, info)) {
        LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletId
            << " Create GetStorageChannelStatus request, Marker# KV75");
        RegisterRequestActor(ctx, std::move(intermediate), info, ExecutorGeneration);
        ++RoInlineIntermediatesInFlight;
        CountRequestTakeOffOrEnqueue(requestType);
    } else {
        intermediate->UpdateStat();
        CountRequestOtherError(requestType);
    }
}

void TKeyValueState::OnEvAcquireLock(TEvKeyValue::TEvAcquireLock::TPtr &ev, const TActorContext &ctx,
        const TTabletStorageInfo *info)
{
    THolder<TIntermediate> intermediate;

    ResourceMetrics->Network.Increment(ev->Get()->Record.ByteSize());
    ResourceMetrics->TryUpdate(ctx);

    TRequestType::EType requestType = TRequestType::ReadOnlyInline;

    CountRequestIncoming(requestType);
    if (PrepareAcquireLockRequest(ctx, ev, intermediate)) {
        LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletId
            << " Create AcquireLock request, Marker# KV80");
        RegisterRequestActor(ctx, std::move(intermediate), info, ExecutorGeneration);
        ++RoInlineIntermediatesInFlight;
        CountRequestTakeOffOrEnqueue(requestType);
    } else {
        intermediate->UpdateStat();
        CountRequestOtherError(requestType);
    }
}

void TKeyValueState::OnEvIntermediate(TIntermediate &intermediate, const TActorContext &ctx) {
    Y_UNUSED(ctx);
    CountLatencyBsOps(intermediate.Stat);
    intermediate.Stat.LocalBaseTxCreatedAt = TAppData::TimeProvider->Now();
}

void TKeyValueState::OnEvRequest(TEvKeyValue::TEvRequest::TPtr &ev, const TActorContext &ctx,
        const TTabletStorageInfo *info) {
    THolder<TIntermediate> intermediate;
    NKikimrClient::TKeyValueRequest &request = ev->Get()->Record;

    ResourceMetrics->Network.Increment(request.ByteSize());
    ResourceMetrics->TryUpdate(ctx);

    bool hasWrites = request.CmdWriteSize() || request.CmdDeleteRangeSize() || request.CmdRenameSize()
                  || request.CmdCopyRangeSize() || request.CmdConcatSize() || request.HasCmdSetExecutorFastLogPolicy();

    bool hasReads = request.CmdReadSize() || request.CmdReadRangeSize();

    bool hasTrim = request.HasCmdTrimLeakedBlobs();

    TRequestType::EType requestType;
    if (hasWrites) {
        if (hasReads) {
            requestType = TRequestType::ReadWrite;
        } else {
            requestType = TRequestType::WriteOnly;
        }
    } else {
        requestType = TRequestType::ReadOnly;
    }

    CountRequestIncoming(requestType);


    if (PrepareIntermediate(ev, intermediate, requestType, ctx, info)) {
        // Spawn KeyValueStorageRequest actor on the same thread
        if (hasTrim && IsCollectEventSent) {
            CmdTrimLeakedBlobsPostponed.push_back(std::move(intermediate));
        } else {
            if (hasTrim) {
                CmdTrimLeakedBlobsUids.insert(intermediate->RequestUid);
            }
            if (requestType == TRequestType::WriteOnly) {
                LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletId
                    << " Create storage request for WO, Marker# KV42");
                RegisterRequestActor(ctx, std::move(intermediate), info, ExecutorGeneration);
            } else if (requestType == TRequestType::ReadOnlyInline) {
                LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletId
                    << " Create storage request for RO_INLINE, Marker# KV45");
                RegisterRequestActor(ctx, std::move(intermediate), info, ExecutorGeneration);
                ++RoInlineIntermediatesInFlight;
            } else {
                if (IntermediatesInFlight < IntermediatesInFlightLimit) {
                    LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletId
                        << " Create storage request for RO/RW, Marker# KV43");
                    RegisterRequestActor(ctx, std::move(intermediate), info, ExecutorGeneration);
                    ++IntermediatesInFlight;
                } else {
                    LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletId
                        << " Enqueue storage request for RO/RW, Marker# KV44");
                    PostponeIntermediate<TEvKeyValue::TEvRequest>(std::move(intermediate));
                }
            }
        }

        CountRequestTakeOffOrEnqueue(requestType);
    } else {
        intermediate->UpdateStat();
        CountRequestOtherError(requestType);
        CancelInFlight(intermediate->RequestUid);
    }
}

bool TKeyValueState::PrepareIntermediate(TEvKeyValue::TEvRequest::TPtr &ev, THolder<TIntermediate> &intermediate,
        TRequestType::EType &inOutRequestType, const TActorContext &ctx, const TTabletStorageInfo *info) {
    LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletId << " PrepareIntermediate Marker# KV40");
    NKikimrClient::TKeyValueRequest &request = ev->Get()->Record;

    StoredState.SetChannelGeneration(ExecutorGeneration);
    StoredState.SetChannelStep(NextLogoBlobStep - 1);

    intermediate.Reset(new TIntermediate(ev->Sender, ctx.SelfID,
        StoredState.GetChannelGeneration(), StoredState.GetChannelStep(), inOutRequestType, std::move(ev->TraceId)));
    intermediate->RequestUid = NextRequestUid;
    ++NextRequestUid;
    RequestInputTime[intermediate->RequestUid] = TAppData::TimeProvider->Now();
    intermediate->EvType = TEvKeyValue::TEvRequest::EventType;

    intermediate->HasCookie = request.HasCookie();
    if (request.HasCookie()) {
        intermediate->Cookie = request.GetCookie();
    }
    intermediate->HasIncrementGeneration = request.HasCmdIncrementGeneration();

    intermediate->UsePayloadInResponse = request.GetUsePayloadInResponse();

    if (CheckDeadline(ctx, request, intermediate)) {
        return false;
    }

    if (CheckGeneration(ctx, request, intermediate)) {
        return false;
    }

    bool error = false;

    bool isInlineOnly = true;
    error = error || PrepareCmdRead(ctx, request, intermediate, isInlineOnly);
    error = error || PrepareCmdReadRange(ctx, request, intermediate, isInlineOnly);
    if (!error && isInlineOnly && inOutRequestType == TRequestType::ReadOnly) {
        inOutRequestType = TRequestType::ReadOnlyInline;
        intermediate->Stat.RequestType = inOutRequestType;
    }

    ui32 cmdCount = request.CmdWriteSize() + request.CmdDeleteRangeSize() + request.CmdRenameSize()
            + request.CmdCopyRangeSize() + request.CmdConcatSize();
    intermediate->Commands.reserve(cmdCount);

    error = error || PrepareCmdCopyRange(ctx, request, intermediate);
    error = error || PrepareCmdRename(ctx, request, intermediate);
    error = error || PrepareCmdConcat(ctx, request, intermediate);
    error = error || PrepareCmdDelete(ctx, request, intermediate);
    error = error || PrepareCmdWrite(ctx, request, *ev->Get(), intermediate, info);
    error = error || PrepareCmdPatch(ctx, request, *ev->Get(), intermediate, info);
    error = error || PrepareCmdGetStatus(ctx, request, intermediate, info);
    error = error || PrepareCmdTrimLeakedBlobs(ctx, request, intermediate, info);
    error = error || PrepareCmdSetExecutorFastLogPolicy(ctx, request, intermediate, info);

    intermediate->WriteCount = request.CmdWriteSize();
    intermediate->DeleteCount = request.CmdDeleteRangeSize();
    intermediate->RenameCount = request.CmdRenameSize();
    intermediate->CopyRangeCount = request.CmdCopyRangeSize();
    intermediate->ConcatCount = request.CmdConcatSize();

    if (error) {
        DropRefCountsOnError(intermediate->RefCountsIncr, false, ctx);
        Y_ABORT_UNLESS(intermediate->RefCountsIncr.empty());

        LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletId
                << " PrepareIntermediate return flase, Marker# KV41");
        return false;
    }

    return true;
}

void TKeyValueState::UpdateResourceMetrics(const TActorContext& ctx) {
    ResourceMetrics->TryUpdate(ctx);
}

bool TKeyValueState::ConvertRange(const NKikimrClient::TKeyValueRequest::TKeyRange& from, TKeyRange *to,
                                  const TActorContext& ctx, THolder<TIntermediate>& intermediate, const char *cmd,
                                  ui32 index) {
    to->HasFrom = from.HasFrom();
    if (to->HasFrom) {
        to->KeyFrom = from.GetFrom();
        to->IncludeFrom = from.HasIncludeFrom() && from.GetIncludeFrom();
    } else if (from.HasIncludeFrom()) {
        TStringStream str;
        str << "KeyValue# " << TabletId
            << " Range.IncludeFrom unexpectedly set without Range.From in " << cmd << "(" << index << ")"
            << " Marker# KV13";
        ReplyError(ctx, str.Str(), NMsgBusProxy::MSTATUS_ERROR, NKikimrKeyValue::Statuses::RSTATUS_ERROR, intermediate);
        return false;
    }

    to->HasTo = from.HasTo();
    if (to->HasTo) {
        to->KeyTo = from.GetTo();
        to->IncludeTo = from.HasIncludeTo() && from.GetIncludeTo();
    } else if (from.HasIncludeTo()) {
        TStringStream str;
        str << "KeyValue# " << TabletId
            << " Range.IncludeTo unexpectedly set without Range.To in " << cmd << "(" << index << ")"
            << " Marker# KV14";
        ReplyError(ctx, str.Str(), NMsgBusProxy::MSTATUS_ERROR, NKikimrKeyValue::Statuses::RSTATUS_ERROR, intermediate);
        return false;
    }

    if (to->HasFrom && to->HasTo) {
        if (!to->IncludeFrom && !to->IncludeTo && to->KeyFrom >= to->KeyTo) {
            TStringStream str;
            str << "KeyValue# " << TabletId
                << " Range.KeyFrom >= Range.KeyTo in " << cmd << "(" << index << ")"
                << " Marker# KV15";
            ReplyError(ctx, str.Str(), NMsgBusProxy::MSTATUS_ERROR, NKikimrKeyValue::Statuses::RSTATUS_ERROR, intermediate);
            return false;
        } else if (to->KeyFrom > to->KeyTo) {
            TStringStream str;
            str << "KeyValue# " << TabletId
                << " Range.KeyFrom > Range.KeyTo in " << cmd << "(" << index << ")"
                << " Marker# KV16";
            ReplyError(ctx, str.Str(), NMsgBusProxy::MSTATUS_ERROR, NKikimrKeyValue::Statuses::RSTATUS_ERROR, intermediate);
            return false;
        }
    }

    return true;
}

TString TKeyValueState::Dump() const {
    TStringStream ss;
    ss << "=== INDEX ===\n";
    for (auto& x : Index) {
        const TString& k = x.first;
        const TIndexRecord& v = x.second;
        ss << k << "=== ctime:" << v.CreationUnixTime;
        for (const TIndexRecord::TChainItem& y : v.Chain) {
            ss << " -> " << y.LogoBlobId << ":" << y.Offset;
        }
        ss << "\n";
    }
    ss << "=== END ===\n";
    return ss.Str();
}

void TKeyValueState::VerifyEqualIndex(const TKeyValueState& state) const {
    auto i2 = state.Index.cbegin(), e2 = state.Index.cend();
    int i = 0;
    for (auto i1 = Index.cbegin(), e1 = Index.cend(); i1 != e1; ++i, ++i1, ++i2) {
        Y_ABORT_UNLESS(i2 != e2, "index length differs. Dump:\n%s\n%s\n", Dump().data(), state.Dump().data());
        const TString& k1 = i1->first;
        const TString& k2 = i2->first;
        Y_ABORT_UNLESS(k1 == k2, "index key #%d differs. Dump:\n%s\n%s\n", i, Dump().data(), state.Dump().data());
        const TIndexRecord& v1 = i1->second;
        const TIndexRecord& v2 = i2->second;
        Y_ABORT_UNLESS(v1 == v2, "index value #%d differs. Dump:\n%s\n%s\n", i, Dump().data(), state.Dump().data());
    }
    Y_ABORT_UNLESS(i2 == e2, "index length differs. Dump:\n%s\n%s\n", Dump().data(), state.Dump().data());
}

void TKeyValueState::RenderHTMLPage(IOutputStream &out) const {
    HTML(out) {
        TAG(TH2) {out << "KeyValue Tablet";}
        UL_CLASS("nav nav-tabs") {
            LI_CLASS("active") {
                out << "<a href=\"#database\" data-toggle=\"tab\">Database</a>";
            }
            LI() {
                out << "<a href=\"#refcounts\" data-toggle=\"tab\">RefCounts</a>";
            }
            LI() {
                out << "<a href=\"#trash\" data-toggle=\"tab\">Trash</a>";
            }
            LI() {
                out << "<a href=\"#channelstat\" data-toggle=\"tab\">Channel Stat</a>";
            }
        }
        DIV_CLASS("tab-content") {
            DIV_CLASS_ID("tab-pane fade in active", "database") {
                TABLE_SORTABLE_CLASS("table") {
                    TABLEHEAD() {
                        TABLER() {
                            TABLEH() {out << "Idx";}
                            TABLEH() {out << "Key";}
                            TABLEH() {out << "Value Size";}
                            TABLEH() {out << "Creation UnixTime";}
                            TABLEH() {out << "Storage Channel";}
                            TABLEH() {out << "LogoBlobIds";}
                        }
                    }
                    TABLEBODY() {
                        ui64 idx = 1;
                        for (auto it = Index.begin(); it != Index.end(); ++it) {
                            TABLER() {
                                TABLED() {out << idx;}
                                ++idx;
                                TABLED() {out << EscapeC(it->first);}
                                TABLED() {out << it->second.GetFullValueSize();}
                                TABLED() {out << it->second.CreationUnixTime;}
                                TABLED() {
                                    NKikimrClient::TKeyValueRequest::EStorageChannel storageChannel =
                                        NKikimrClient::TKeyValueRequest::MAIN;
                                    if (it->second.Chain.size()) {
                                        if (it->second.Chain[0].IsInline()) {
                                            storageChannel = NKikimrClient::TKeyValueRequest::INLINE;
                                        } else {
                                            ui32 storageChannelIdx = it->second.Chain[0].LogoBlobId.Channel();
                                            ui32 storageChannelOffset = storageChannelIdx - BLOB_CHANNEL;
                                            storageChannel = (NKikimrClient::TKeyValueRequest::EStorageChannel)
                                                storageChannelOffset;
                                        }
                                    }
                                    out << NKikimrClient::TKeyValueRequest::EStorageChannel_Name(
                                        storageChannel);
                                }

                                TABLED() {
                                    for (ui32 i = 0; i < it->second.Chain.size(); ++i) {
                                        if (i > 0) {
                                            out << "<br/>";
                                        }
                                        if (it->second.Chain[i].IsInline()) {
                                            out << "[INLINE:" << it->second.Chain[i].InlineData.size() << "]";
                                        } else {
                                            out << it->second.Chain[i].LogoBlobId.ToString();
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            DIV_CLASS_ID("tab-pane fade", "refcounts") {
                TABLE_SORTABLE_CLASS("table") {
                    TABLEHEAD() {
                        TABLER() {
                            TABLEH() {out << "Idx";}
                            TABLEH() {out << "LogoBlobId";}
                            TABLEH() {out << "RefCount";}
                        }
                    }
                    TABLEBODY() {
                        ui32 idx = 1;
                        for (const auto& kv : RefCounts) {
                            TABLER() {
                                TABLED() {out << idx;}
                                TABLED() {out << kv.first.ToString();}
                                TABLED() {out << kv.second;}
                            }
                            ++idx;
                        }
                    }
                }
            }
            DIV_CLASS_ID("tab-pane fade", "trash") {
                TABLE_SORTABLE_CLASS("table") {
                    TABLEHEAD() {
                        TABLER() {
                            TABLEH() {out << "Idx";}
                            TABLEH() {out << "LogoBlobId";}
                        }
                    }
                    TABLEBODY() {
                        ui64 idx = 1;
                        for (auto it = Trash.begin(); it != Trash.end(); ++it) {
                            TABLER() {
                                TABLED() {out << idx;}
                                ++idx;
                                TABLED() {out << *it;}
                            }
                        }
                    }
                }
            }
            DIV_CLASS_ID("tab-pane fade", "channelstat") {
                TABLE_SORTABLE_CLASS("table") {
                    TABLEHEAD() {
                        TABLER() {
                            TABLEH() {out << "Channel";}
                            TABLEH() {out << "Total values size";}
                        }
                    }
                    TABLEBODY() {
                        for (size_t i = 0; i < ChannelDataUsage.size(); ++i) {
                            if (UsedChannels[i]) {
                                TABLER() {
                                    TABLED() {
                                        if (i) {
                                            out << i;
                                        } else {
                                            out << "inline";
                                        }
                                    }
                                    ui64 size = ChannelDataUsage[i];
                                    out << "<td title=" << size << ">";
                                    TString value;
                                    for (; size >= 1000; size /= 1000) {
                                        value = Sprintf(" %03" PRIu64, size % 1000) + value;
                                    }
                                    value = Sprintf("%" PRIu64, size) + value;
                                    out << value;
                                    out << "</td>";
                                }
                            }
                        }
                    }
                }
            }

        }
    }
}

void TKeyValueState::MonChannelStat(NJson::TJsonValue& out) const {
    for (size_t i = 0; i < ChannelDataUsage.size(); ++i) {
        if (UsedChannels[i]) {
            const TString key = i ? Sprintf("%zu", i) : "inline";
            out[key] = ChannelDataUsage[i];
        }
    }
}

} // NKeyValue
} // NKikimr
