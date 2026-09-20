#include "block_range_field_impl.h"

#include <util/string/builder.h>

namespace NYdb::NBS::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui8 RunLengthContinuation = 0xff;

////////////////////////////////////////////////////////////////////////////////

void AppendRunLength(ui64 length, TString* output)
{
    while (length >= RunLengthContinuation) {
        output->push_back(static_cast<char>(RunLengthContinuation));
        length -= RunLengthContinuation;
    }
    output->push_back(static_cast<char>(length));
}

bool ReadRunLength(TStringBuf input, size_t* offset, ui64* length)
{
    *length = 0;
    for (;;) {
        if (*offset >= input.size()) {
            return false;
        }

        const ui8 part = static_cast<ui8>(input[(*offset)++]);
        *length += part;

        if (part != RunLengthContinuation) {
            return true;
        }
    }
}

}   // namespace

// static
bool TNodeBasedBlockRangeField::DeserializeFromRLE(
    const TString& input,
    ui16 maxBlockCount,
    TEnumerateFunc func)
{
    ui64 position = 0;
    size_t offset = 0;

    while (offset < input.size()) {
        ui64 skipLength = 0;
        ui64 fillLength = 0;
        if (!ReadRunLength(input, &offset, &skipLength) ||
            !ReadRunLength(input, &offset, &fillLength) || !fillLength ||
            skipLength > maxBlockCount - position)
        {
            return false;
        }

        position += skipLength;
        if (fillLength > maxBlockCount - position) {
            return false;
        }
        if (func(TBlockRange16::WithLength(position, fillLength)) ==
            EEnumerateContinuation::Stop)
        {
            return true;
        }
        position += fillLength;
    }
    return true;
}

std::optional<TBlockRange16> TNodeBasedBlockRangeField::GetFirstRange() const
{
    if (Empty()) {
        return std::nullopt;
    }
    TBlockRange16 result;
    Enumerate(
        [&](const TRange& r)
        {
            result = r;
            return EEnumerateContinuation::Stop;
        });
    return result;
}

TString TNodeBasedBlockRangeField::Print() const
{
    TStringBuilder sb;
    Enumerate(
        [&](const TRange& r)
        {
            sb << r.Print();
            return EEnumerateContinuation::Continue;
        });
    return sb;
}

TString TNodeBasedBlockRangeField::Save() const
{
    TString result;
    result.reserve(GetSegmentCount() * 2 + 64);

    ui64 position = 0;
    Enumerate(
        [&](TBlockRange16 item)
        {
            AppendRunLength(item.Start - position, &result);
            AppendRunLength(item.Size(), &result);
            position = item.End + 1;

            return TNodeBasedBlockRangeField::EEnumerateContinuation::Continue;
        });
    return result;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
