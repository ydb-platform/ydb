#include "mkql_string_util.h"

namespace NKikimr {
namespace NMiniKQL {

namespace {

ui32 CheckedSum(ui32 one, ui32 two) {
    if (ui64(one) + ui64(two) > ui64(std::numeric_limits<ui32>::max()))
        ythrow yexception() << "Impossible to concat too large strings " << one << " and " << two << " bytes!";
    return one + two;
}

}

NUdf::TUnboxedValuePod AppendString(const NUdf::TUnboxedValuePod value, const NUdf::TStringRef ref)
{
    if (!ref.Size())
        return value;

    const auto& valueRef = value.AsStringRef();
    if (!valueRef.Size())
        return MakeString(ref);

    const auto newSize = CheckedSum(valueRef.Size(), ref.Size());
    if (newSize <= NUdf::TUnboxedValuePod::InternalBufferSize) {
        auto result = NUdf::TUnboxedValuePod::Embedded(newSize);
        const auto buf = result.AsStringRef().Data();
        std::memcpy(buf, valueRef.Data(), valueRef.Size());
        std::memcpy(buf + valueRef.Size(), ref.Data(), ref.Size());
        return result;
    } else {
        if (value.IsString()) {
            auto str = value.AsStringValue();
            const ui32 offset = ref.Data() - str.Data();
            if (str.Size() == valueRef.Size() + offset) {
                if (str.TryExpandOn(ref.Size())) {
                    std::memcpy(str.Data() + offset + valueRef.Size(), ref.Data(), ref.Size());
                    return NUdf::TUnboxedValuePod(std::move(str), newSize, offset);
                }
            }
        }

        auto data = NUdf::TStringValue::AllocateData(newSize, newSize + newSize / 2);
        NUdf::TStringValue str(data);
        data->UnRef();
        std::memcpy(str.Data(), valueRef.Data(), valueRef.Size());
        std::memcpy(str.Data() + valueRef.Size(), ref.Data(), ref.Size());
        return NUdf::TUnboxedValuePod(std::move(str));
    }
}

NUdf::TUnboxedValuePod PrependString(const NUdf::TStringRef ref, const NUdf::TUnboxedValuePod value)
{
    if (!ref.Size())
        return value;

    const auto& valueRef = value.AsStringRef();
    if (!valueRef.Size())
        return MakeString(ref);

    const auto newSize = CheckedSum(valueRef.Size(), ref.Size());
    if (newSize <= NUdf::TUnboxedValuePod::InternalBufferSize) {
        auto result = NUdf::TUnboxedValuePod::Embedded(newSize);
        const auto buf = result.AsStringRef().Data();
        std::memcpy(buf, ref.Data(), ref.Size());
        std::memcpy(buf + ref.Size(), valueRef.Data(), valueRef.Size());
        return result;
    } else {
        auto data = NUdf::TStringValue::AllocateData(newSize, newSize + newSize / 2);
        NUdf::TStringValue str(data);
        data->UnRef();
        std::memcpy(str.Data(), ref.Data(), ref.Size());
        std::memcpy(str.Data() + ref.Size(), valueRef.Data(), valueRef.Size());
        value.DeleteUnreferenced();
        return NUdf::TUnboxedValuePod(std::move(str));
    }
}

NUdf::TUnboxedValuePod ConcatStrings(const NUdf::TUnboxedValuePod first, const NUdf::TUnboxedValuePod second)
{
    const auto& leftRef = first.AsStringRef();
    if (!leftRef.Size())
        return second;

    const auto& rightRef = second.AsStringRef();
    if (!rightRef.Size())
        return first;

    const auto newSize = CheckedSum(leftRef.Size(), rightRef.Size());
    if (newSize <= NUdf::TUnboxedValuePod::InternalBufferSize) {
        auto result = NUdf::TUnboxedValuePod::Embedded(newSize);
        const auto buf = result.AsStringRef().Data();
        std::memcpy(buf, leftRef.Data(), leftRef.Size());
        std::memcpy(buf + leftRef.Size(), rightRef.Data(), rightRef.Size());
        return result;
    } else {
        if (first.IsString()) {
            auto str = first.AsStringValue();
            const ui32 offset = leftRef.Data() - str.Data();
            if (str.Size() == leftRef.Size() + offset) {
                if (str.TryExpandOn(rightRef.Size())) {
                    std::memcpy(str.Data() + offset + leftRef.Size(), rightRef.Data(), rightRef.Size());
                    second.DeleteUnreferenced();
                    return NUdf::TUnboxedValuePod(std::move(str), newSize, offset);
                }
            }
        }

        auto data = NUdf::TStringValue::AllocateData(newSize, newSize + newSize / 2);
        NUdf::TStringValue str(data);
        data->UnRef();
        std::memcpy(str.Data(), leftRef.Data(), leftRef.Size());
        std::memcpy(str.Data() + leftRef.Size(), rightRef.Data(), rightRef.Size());
        second.DeleteUnreferenced();
        return NUdf::TUnboxedValuePod(std::move(str));
    }
}

NUdf::TUnboxedValuePod SubString(const NUdf::TUnboxedValuePod value, ui32 offset, ui32 size)
{
    const auto& ref = value.AsStringRef();
    if (size == 0U || ref.Size() <= offset) {
        value.DeleteUnreferenced();
        return NUdf::TUnboxedValuePod::Zero();
    }

    if (offset == 0U && ref.Size() <= size)
        return value;

    if (const auto newSize = std::min(ref.Size() - offset, size); newSize <= NUdf::TUnboxedValuePod::InternalBufferSize) {
        auto result = NUdf::TUnboxedValuePod::Embedded(newSize);
        std::memcpy(result.AsStringRef().Data(), ref.Data() + offset, newSize);
        value.DeleteUnreferenced();
        return result;
    } else {
        auto old = value.AsStringValue();
        if (const auto newOffset = ui32(ref.Data() - old.Data()) + offset; NUdf::TUnboxedValuePod::OffsetLimit > newOffset)
            return NUdf::TUnboxedValuePod(std::move(old), newSize, newOffset);

        auto data = NUdf::TStringValue::AllocateData(newSize, newSize + (newSize >> 1U));
        NUdf::TStringValue str(data);
        data->UnRef();
        std::memcpy(str.Data(), ref.Data() + offset, newSize);
        return NUdf::TUnboxedValuePod(std::move(str));
    }
}

NUdf::TUnboxedValuePod MakeString(const NUdf::TStringRef ref)
{
    if (ref.Size() <= NUdf::TUnboxedValuePod::InternalBufferSize)
        return NUdf::TUnboxedValuePod::Embedded(ref);

    NUdf::TStringValue str(ref.Size());
    std::memcpy(str.Data(), ref.Data(), ref.Size());
    return NUdf::TUnboxedValuePod(std::move(str));
}

NUdf::TUnboxedValuePod MakeStringNotFilled(ui32 size, ui32 pad)
{
    const auto fullSize = size + pad;
    if (fullSize <= NUdf::TUnboxedValuePod::InternalBufferSize)
        return NUdf::TUnboxedValuePod::Embedded(size);

    return NUdf::TUnboxedValuePod(NUdf::TStringValue(fullSize), size);
}

}
}
