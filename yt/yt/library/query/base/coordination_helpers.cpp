#include "coordination_helpers.h"

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

TRow WidenKeySuccessor(TRow key, size_t prefix, const TRowBufferPtr& rowBuffer, bool captureValues)
{
    auto wideKey = rowBuffer->AllocateUnversioned(prefix + 1);

    for (ui32 index = 0; index < prefix; ++index) {
        wideKey[index] = key[index];
        if (captureValues) {
            wideKey[index] = rowBuffer->CaptureValue(wideKey[index]);
        }
    }

    wideKey[prefix] = MakeUnversionedSentinelValue(EValueType::Max);

    return wideKey;
}

TRow WidenKeySuccessor(TRow key, const TRowBufferPtr& rowBuffer, bool captureValues)
{
    return WidenKeySuccessor(key, key.GetCount(), rowBuffer, captureValues);
}

size_t GetSignificantWidth(TRow row)
{
    auto valueIt = row.Begin();
    while (valueIt != row.End() && !IsSentinelType(valueIt->Type)) {
        ++valueIt;
    }
    return std::distance(row.Begin(), valueIt);
}

size_t Step(size_t current, size_t source, size_t target)
{
    YT_VERIFY(target <= source);
    YT_VERIFY(current <= source);

    // Original expression: ((c * t / s + 1) * s + t - 1) / t - c;
    auto result = (source - 1 - current * target % source ) / target + 1;
    YT_VERIFY(current + result <= source);
    YT_VERIFY(result > 0);
    return result;
}

void TRangeFormatter::operator()(TStringBuilderBase* builder, TRowRange source) const
{
    builder->AppendFormat("[%v .. %v]",
        source.first,
        source.second);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
