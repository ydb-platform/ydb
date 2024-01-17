#include "row_stream.h"

#include <library/cpp/yt/memory/ref.h>
#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/error.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

std::tuple<TSharedRef, TMutableRef> SerializeRowStreamBlockEnvelope(
    i64 payloadSize,
    const NApi::NRpcProxy::NProto::TRowsetDescriptor& descriptor,
    const NApi::NRpcProxy::NProto::TRowsetStatistics* statistics)
{
    /*
        Block with statistics:
            count: i32 = 2
            length-of-part-1: i64
            part-1:
                count: i32 = 2
                length-of-part-1-1: i64
                part-1-1: descriptor
                length-of-part-1-2: i64
                part-1-2: payload
            length-of-part-2: i64
            part-2: statistics

        Block without statistics:
            count: i32 = 2
            length-of-part-1: i64
            part-1: descriptor
            length-of-part-2: i64
            part-2: payload
    */

    i64 totalSize = 0;
    totalSize += sizeof (i32); // partCount
    totalSize += sizeof (i64) * 2; // partLength * 2
    totalSize += descriptor.ByteSizeLong(); // descriptor
    totalSize += payloadSize;
    if (statistics) {
        totalSize += sizeof (i32); // partCount
        totalSize += sizeof (i64) * 2; // partLength * 2
        totalSize += statistics->ByteSize();
    }

    struct TSerializedRowStreamBlockTag { };
    auto block = TSharedMutableRef::Allocate<TSerializedRowStreamBlockTag>(totalSize, {.InitializeStorage = false});

    char* current = block.Begin();

    auto writeInt32 = [&] (i32 value) {
        *reinterpret_cast<i32*>(current) = value;
        current += sizeof (i32);
    };

    auto writeInt64 = [&] (i64 value) {
        *reinterpret_cast<i64*>(current) = value;
        current += sizeof (i64);
    };

    TMutableRef payloadRef;
    auto skipPayload = [&] {
        payloadRef = TMutableRef(current, current + payloadSize);
        current += payloadSize;
    };

    auto writeProto = [&] (const auto& proto) {
        current = reinterpret_cast<char*>(proto.SerializeWithCachedSizesToArray(reinterpret_cast<ui8*>(current)));
    };

    if (statistics) {
        writeInt32(2); // partCount
        writeInt64(sizeof (i32) + 2 * sizeof (i64) + descriptor.ByteSizeLong() + payloadSize); // partLength
    }

    writeInt32(2); // partCount
    writeInt64(descriptor.ByteSizeLong()); // partLength
    writeProto(descriptor);
    writeInt64(payloadSize); // partLength
    skipPayload();

    if (statistics) {
        writeInt64(statistics->ByteSize()); // partLength
        writeProto(*statistics);
    }

    YT_VERIFY(current == block.End());

    return std::tuple(std::move(block), payloadRef);
}

TSharedRef DeserializeRowStreamBlockEnvelope(
    const TSharedRef& block,
    NApi::NRpcProxy::NProto::TRowsetDescriptor* descriptor,
    NApi::NRpcProxy::NProto::TRowsetStatistics* statistics)
{
    TSharedRef rowsRef;
    if (statistics) {
        auto parts = UnpackRefs(block);
        if (parts.size() != 2) {
            THROW_ERROR_EXCEPTION(
                "Error deserializing row stream: expected %v packed refs, got %v",
                2,
                parts.size());
        }

        rowsRef = parts[0];
        const auto& statisticsRef = parts[1];
        if (!TryDeserializeProto(statistics, statisticsRef)) {
            THROW_ERROR_EXCEPTION("Error deserializing rowset statistics");
        }
    } else {
        rowsRef = block;
    }

    auto parts = UnpackRefs(rowsRef);
    if (parts.size() != 2) {
        THROW_ERROR_EXCEPTION(
            "Error deserializing row stream: expected %v packed refs, got %v",
            2,
            parts.size());
    }

    const auto& descriptorRef = parts[0];
    const auto& payloadRef = parts[1];

    if (!TryDeserializeProto(descriptor, descriptorRef)) {
        THROW_ERROR_EXCEPTION("Error deserializing rowset descriptor");
    }

    return payloadRef;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
