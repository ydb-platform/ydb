#include "keyvalue_intermediate.h"
#include <ydb/core/base/appdata.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <library/cpp/time_provider/time_provider.h>

namespace NKikimr {
namespace NKeyValue {


TIntermediate::TRead::TRead()
    : Offset(0)
    , Size(0)
    , ValueSize(0)
    , CreationUnixTime(0)
    , StorageChannel(NKikimrClient::TKeyValueRequest::MAIN)
    , HandleClass(NKikimrBlobStorage::AsyncRead)
    , Status(NKikimrProto::UNKNOWN)
{}

TIntermediate::TRead::TRead(const TString &key, ui32 valueSize, ui64 creationUnixTime,
        NKikimrClient::TKeyValueRequest::EStorageChannel storageChannel)
    : Key(key)
    , Offset(0)
    , Size(valueSize)
    , ValueSize(valueSize)
    , CreationUnixTime(creationUnixTime)
    , StorageChannel(storageChannel)
    , HandleClass(NKikimrBlobStorage::AsyncRead)
    , Status(NKikimrProto::UNKNOWN)
{}

NKikimrProto::EReplyStatus TIntermediate::TRead::ItemsStatus() const {
    for (const TReadItem& item : ReadItems) {
        if (item.Status != NKikimrProto::OK) {
            return NKikimrProto::ERROR;
        }
    }
    return NKikimrProto::OK;
}

NKikimrProto::EReplyStatus TIntermediate::TRead::CumulativeStatus() const {
    NKikimrProto::EReplyStatus itemsStatus = ItemsStatus();
    if (itemsStatus == NKikimrProto::OK) {
        if (Status == NKikimrProto::UNKNOWN) {
            return NKikimrProto::OK;
        } else {
            return Status;
        }
    } else {
        if (Status == NKikimrProto::OVERRUN || Status == NKikimrProto::OK || Status == NKikimrProto::UNKNOWN) {
            return itemsStatus;
        } else {
            return Status;
        }
    }
}

TRope TIntermediate::TRead::BuildRope() {
    TRope rope = Value ? Value.GetMonolith() : TRope();
    Y_ABORT_UNLESS(!Value || rope.size() == ValueSize);
    return rope;
}

TIntermediate::TIntermediate(TActorId respondTo, TActorId keyValueActorId, ui64 channelGeneration, ui64 channelStep,
        TRequestType::EType requestType, NWilson::TTraceId traceId)
    : Cookie(0)
    , Generation(0)
    , Deadline(TInstant::Max())
    , HasCookie(false)
    , HasGeneration(false)
    , HasIncrementGeneration(false)
    , RespondTo(respondTo)
    , KeyValueActorId(keyValueActorId)
    , TotalSize(0)
    , TotalSizeLimit(25ull << 20)
    , TotalReadsScheduled(0)
    , TotalReadsLimit(5)
    , SequentialReadLimit(100)
    , IsTruncated(false)
    , CreatedAtGeneration(channelGeneration)
    , CreatedAtStep(channelStep)
    , IsReplied(false)
    , Span(TWilsonTablet::TabletTopLevel, std::move(traceId), "KeyValue.Intermediate", NWilson::EFlags::AUTO_END)
{
    Stat.IntermediateCreatedAt = TAppData::TimeProvider->Now();
    Stat.RequestType = requestType;
}

void TIntermediate::UpdateStat() {
    auto checkRead = [&] (const auto &read) {
        if (read.Status == NKikimrProto::NODATA) {
            Stat.ReadNodata++;
        } else if (read.Status == NKikimrProto::OK) {
            Stat.Reads++;
            Stat.ReadBytes += read.ValueSize;
        }
    };
    auto checkRangeRead = [&] (const auto &range) {
        if (range.IncludeData) {
            for (const auto &read: range.Reads) {
                if (read.Status == NKikimrProto::NODATA) {
                    Stat.RangeReadItemsNodata++;
                } else if (read.Status == NKikimrProto::OK) {
                    Stat.RangeReadItems++;
                    Stat.RangeReadBytes += read.ValueSize;
                }
            }
        } else {
            Stat.IndexRangeRead++;
        }
    };

    if (ReadCommand) {
        auto checkReadCommand = [&] (auto &cmd) {
            using Type = std::decay_t<decltype(cmd)>;
            if constexpr (std::is_same_v<Type, TIntermediate::TRead>) {
                checkRead(cmd);
            }
            if constexpr (std::is_same_v<Type, TIntermediate::TRangeRead>) {
                checkRangeRead(cmd);
            }
        };
        std::visit(checkReadCommand, *ReadCommand);
    }

    for (const auto &read: Reads) {
        checkRead(read);
    }
    for (const auto &range: RangeReads) {
        checkRangeRead(range);
    }
    for (const auto &write: Writes) {
        if (write.Status == NKikimrProto::OK) {
            Stat.WriteBytes += write.Data.size();
        }
    }

    for (const auto &cmd : Commands) {
        if (!std::holds_alternative<TIntermediate::TWrite>(cmd)) {
            continue;
        }
        const auto &write = std::get<TIntermediate::TWrite>(cmd);
        if (write.Status == NKikimrProto::OK) {
            Stat.WriteBytes += write.Data.size();
        }
    }

    Stat.Writes = WriteCount;
    Stat.GetStatuses = GetStatuses.size();
    Stat.Renames = RenameCount;
    Stat.CopyRanges = CopyRangeCount;
    Stat.Concats = ConcatCount;
}

} // NKeyValue
} // NKikimr
