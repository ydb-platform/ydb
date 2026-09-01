#include "block_field_serializer.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/dirty_map.pb.h>

#include <util/generic/strbuf.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui8 RunLengthContinuation = 0xff;

constexpr size_t GetBitMaskSize(ui64 blockCount)
{
    return (blockCount + 7) / 8;
}

size_t GetRunLengthEncodingSegmentThreshold(ui64 blockCount)
{
    // Each segment takes two bytes for its skip and fill lengths. The sum of
    // all lengths is at most blockCount, so at most blockCount / 255
    // additional continuation bytes are needed.
    return (GetBitMaskSize(blockCount) - blockCount / RunLengthContinuation) /
           2;
}

constexpr size_t MaxBitMaskSize = GetBitMaskSize(MaxVChunkBlockCount);

////////////////////////////////////////////////////////////////////////////////

void AppendRunLength(ui64 length, TString* output)
{
    while (length >= RunLengthContinuation) {
        output->push_back(static_cast<char>(RunLengthContinuation));
        length -= RunLengthContinuation;
    }
    output->push_back(static_cast<char>(length));
}

ui64 ReadRunLength(TStringBuf input, size_t* offset)
{
    ui64 length = 0;
    for (;;) {
        Y_ABORT_UNLESS(*offset < input.size());

        const ui8 part = static_cast<ui8>(input[(*offset)++]);
        length += part;
        Y_ABORT_UNLESS(length <= MaxVChunkBlockCount);

        if (part != RunLengthContinuation) {
            return length;
        }
    }
}

TString MakeRunLengthEncoding(const TBlockRangeField& field, ui64 blockCount)
{
    TString result;
    result.reserve(
        field.GetSegmentCount() * 2 +
        field.GetBlockCount() / RunLengthContinuation);

    ui64 position = 0;
    field.Enumerate(
        [&](TBlockRange64 item)
        {
            Y_ABORT_UNLESS(item.End < blockCount);

            AppendRunLength(item.Start - position, &result);
            AppendRunLength(item.Size(), &result);
            position = item.End + 1;

            return TBlockRangeField::EEnumerateContinuation::Continue;
        });

    return result;
}

TString MakeBitMask(const TBlockRangeField& field, ui64 blockCount)
{
    TString result;

    field.Enumerate(
        [&](TBlockRange64 item)
        {
            Y_ABORT_UNLESS(item.End < blockCount);

            const size_t byteCount = item.End / 8 + 1;
            if (result.size() < byteCount) {
                result.resize(byteCount, 0);
            }

            for (ui64 blockIndex = item.Start; blockIndex <= item.End;
                 ++blockIndex) {
                const size_t byteIndex = blockIndex / 8;
                result[byteIndex] = static_cast<char>(
                    static_cast<ui8>(result[byteIndex]) |
                    (1 << (blockIndex % 8)));
            }

            return TBlockRangeField::EEnumerateContinuation::Continue;
        });

    return result;
}

void LoadRunLengthEncoding(TStringBuf input, TBlockRangeField* field)
{
    ui64 position = 0;
    size_t offset = 0;

    while (offset < input.size()) {
        const ui64 skipLength = ReadRunLength(input, &offset);
        const ui64 fillLength = ReadRunLength(input, &offset);
        Y_ABORT_UNLESS(fillLength > 0);
        Y_ABORT_UNLESS(position + skipLength <= MaxVChunkBlockCount);

        position += skipLength;
        Y_ABORT_UNLESS(fillLength <= MaxVChunkBlockCount - position);
        field->Add(TBlockRange64::WithLength(position, fillLength));
        position += fillLength;
    }
}

void LoadBitMask(TStringBuf input, TBlockRangeField* field)
{
    Y_ABORT_UNLESS(input.size() <= MaxBitMaskSize);

    const auto isSet = [&](size_t blockIndex)
    {
        return static_cast<ui8>(input[blockIndex / 8]) &
               (1 << (blockIndex % 8));
    };

    const size_t blockCount = input.size() * 8;
    size_t blockIndex = 0;
    while (blockIndex < blockCount) {
        while (blockIndex < blockCount && !isSet(blockIndex)) {
            ++blockIndex;
        }
        const size_t rangeStart = blockIndex;
        while (blockIndex < blockCount && isSet(blockIndex)) {
            ++blockIndex;
        }
        if (rangeStart != blockIndex) {
            field->Add(
                TBlockRange64::WithLength(rangeStart, blockIndex - rangeStart));
        }
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void SaveBlockField(
    const TBlockRangeField& field,
    ui64 blockCount,
    TBlockFieldProto* proto)
{
    Y_ABORT_UNLESS(blockCount <= MaxVChunkBlockCount);

    proto->Clear();
    if (field.Empty()) {
        return;
    }

    if (field.GetSegmentCount() <=
        GetRunLengthEncodingSegmentThreshold(blockCount))
    {
        proto->SetRunLengthEncoding(MakeRunLengthEncoding(field, blockCount));
    } else {
        proto->SetBitMask(MakeBitMask(field, blockCount));
    }
}

void LoadBlockField(const TBlockFieldProto& proto, TBlockRangeField* field)
{
    field->Clear();
    switch (proto.GetEncodingCase()) {
        case TBlockFieldProto::kRunLengthEncoding:
            LoadRunLengthEncoding(proto.GetRunLengthEncoding(), field);
            break;
        case TBlockFieldProto::kBitMask:
            LoadBitMask(proto.GetBitMask(), field);
            break;
        case TBlockFieldProto::ENCODING_NOT_SET:
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
