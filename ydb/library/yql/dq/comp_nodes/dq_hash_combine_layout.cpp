#include "dq_hash_combine_layout.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/public/udf/udf_type_ops.h>

#include <util/system/unaligned_mem.h>
#include <util/system/yassert.h>

#include <cstring>
#include <type_traits>

namespace NKikimr::NMiniKQL {
namespace {

using NUdf::EDataSlot;
using NUdf::TUnboxedValue;
using NUdf::TUnboxedValuePod;

constexpr size_t StringHeaderSize = sizeof(*TUnboxedValuePod{}.AsRawStringValue());

TDqHashCombineTupleLayout::EStorage GetStorage(EDataSlot slot) {
    switch (slot) {
        case EDataSlot::Uint64:
        case EDataSlot::Int64:
        case EDataSlot::Double:
        case EDataSlot::Timestamp:
        case EDataSlot::Interval:
        case EDataSlot::Datetime64:
        case EDataSlot::Timestamp64:
        case EDataSlot::Interval64:
            return TDqHashCombineTupleLayout::EStorage::Native64;
        case EDataSlot::Uint32:
        case EDataSlot::Int32:
        case EDataSlot::Float:
        case EDataSlot::Datetime:
        case EDataSlot::Date32:
            return TDqHashCombineTupleLayout::EStorage::Native32;
        case EDataSlot::Uint16:
        case EDataSlot::Int16:
        case EDataSlot::Date:
            return TDqHashCombineTupleLayout::EStorage::Native16;
        default:
            return TDqHashCombineTupleLayout::EStorage::Unboxed;
    }
}

template <typename TEstimate>
std::optional<size_t> EstimateCompositeSize(TType* type, TEstimate&& estimate) {
    if (!type->IsTuple() && !type->IsStruct()) {
        return {};
    }
    const auto* tuple = type->IsTuple() ? AS_TYPE(TTupleType, type) : nullptr;
    const auto* structure = type->IsStruct() ? AS_TYPE(TStructType, type) : nullptr;
    const ui32 count = tuple ? tuple->GetElementsCount() : structure->GetMembersCount();
    // Tuple/struct contents are generally boxed into a TDirectArrayHolderInplace instance
    size_t result = sizeof(TUnboxedValuePod) + sizeof(TDirectArrayHolderInplace);
    for (ui32 i = 0; i < count; ++i) {
        const auto itemSize = estimate(i, tuple ? tuple->GetElementType(i) : structure->GetMemberType(i));
        if (!itemSize) {
            return {};
        }
        result += *itemSize;
    }
    return result;
}

std::optional<size_t> GetStaticUvSizeBound(TType* type) {
    if (type->IsOptional()) {
        return GetStaticUvSizeBound(AS_TYPE(TOptionalType, type)->GetItemType());
    }
    if (type->IsTagged()) {
        return GetStaticUvSizeBound(AS_TYPE(TTaggedType, type)->GetBaseType());
    }
    if (type->IsData()) {
        const auto slot = AS_TYPE(TDataType, type)->GetDataSlot();
        if (!slot) {
            return {};
        }
        switch (*slot) {
            case EDataSlot::Uuid:
                return sizeof(TUnboxedValuePod) + StringHeaderSize + NUdf::UUID_SIZE;
            case EDataSlot::DyNumber:
            case EDataSlot::Json:
            case EDataSlot::JsonDocument:
            case EDataSlot::Yson:
            case EDataSlot::Utf8:
            case EDataSlot::String:
                return {};
            default:
                return sizeof(TUnboxedValuePod);
        }
    }
    return EstimateCompositeSize(type, [](ui32, TType* itemType) { return GetStaticUvSizeBound(itemType); });
}

} // namespace

std::optional<size_t> TDqHashCombineTupleLayout::EstimateValueMemorySize(const TUnboxedValuePod& value, TType* type) {
    if (!value.HasValue() || value.IsEmbedded() || value.IsInvalid()) {
        return sizeof(TUnboxedValuePod);
    }
    if (value.IsString()) {
        return sizeof(TUnboxedValuePod) + StringHeaderSize + value.AsStringRef().Size();
    }
    if (!value.IsBoxed()) {
        return {};
    }
    while (type->IsOptional() || type->IsTagged()) {
        type = type->IsOptional() ? AS_TYPE(TOptionalType, type)->GetItemType() : AS_TYPE(TTaggedType, type)->GetBaseType();
    }
    if (!type->IsTuple() && !type->IsStruct()) {
        return {};
    }

    const ui32 count = type->IsTuple() ? AS_TYPE(TTupleType, type)->GetElementsCount() :
        AS_TYPE(TStructType, type)->GetMembersCount();
    if (!count) {
        return sizeof(TUnboxedValuePod) + sizeof(TDirectArrayHolderInplace);
    }

    static_assert(std::is_final_v<TDirectArrayHolderInplace>, "Memory estimation requires an exact holder type");
    const auto* holder = dynamic_cast<const TDirectArrayHolderInplace*>(value.AsRawBoxed());
    if (!holder) {
        return {};
    }
    MKQL_ENSURE(holder->GetSize() >= count, "Composite holder has fewer elements than its type");

    const auto* elements = holder->GetPtr();
    return EstimateCompositeSize(type, [&](ui32 index, TType* itemType) {
        return EstimateValueMemorySize(elements[index], itemType);
    });
}

TDqHashCombineTupleLayout::TDqHashCombineTupleLayout(TArrayRef<TType* const> types)
    : Types(types.begin(), types.end())
{
    Items.reserve(types.size());
    ui32 optionalNativeCount = 0;

    for (ui32 i = 0; i < types.size(); ++i) {
        TType* type = types[i];
        bool optional = false;
        bool nestedOptional = false;
        if (type->IsOptional()) {
            optional = true;
            type = AS_TYPE(TOptionalType, type)->GetItemType();
            nestedOptional = type->IsOptional();
        }

        TType* unpackedType = type;
        while (unpackedType->IsOptional()) {
            unpackedType = AS_TYPE(TOptionalType, unpackedType)->GetItemType();
        }
        std::optional<EDataSlot> slot;
        if (unpackedType->IsData()) {
            const auto dataSlot = AS_TYPE(TDataType, unpackedType)->GetDataSlot();
            if (dataSlot) {
                slot = dataSlot.GetRef();
            }
        }

        EStorage storage = !nestedOptional && type->IsData() && slot ? GetStorage(*slot) : EStorage::Unboxed;
        TItem item{
            .LogicalIndex = i,
            .Offset = 0,
            .DataSlot = slot,
            .Storage = storage,
            .Optional = optional,
        };
        switch (storage) {
            case EStorage::Unboxed: ++UnboxedCount; break;
            case EStorage::Native64: ++Native64Count; break;
            case EStorage::Native32: ++Native32Count; break;
            case EStorage::Native16: ++Native16Count; break;
        }
        if (optional && storage != EStorage::Unboxed) {
            item.ValidityOffset = sizeof(ui32) * (optionalNativeCount / 32);
            item.ValidityMask = ui32{1} << (optionalNativeCount % 32);
            ++optionalNativeCount;
        }
        Items.push_back(item);
    }

    std::stable_sort(Items.begin(), Items.end(), [](const TItem& left, const TItem& right) {
        return left.Storage < right.Storage;
    });

    ValidityWordCount = (optionalNativeCount + 31) / 32;
    const size_t native64Offset = UnboxedCount * sizeof(TUnboxedValuePod);
    ValidityOffset = native64Offset + Native64Count * sizeof(ui64);
    const size_t native32Offset = ValidityOffset + ValidityWordCount * sizeof(ui32);
    const size_t native16Offset = native32Offset + Native32Count * sizeof(ui32);
    Size = native16Offset + Native16Count * sizeof(ui16);

    size_t unboxed = 0;
    size_t native64 = 0;
    size_t native32 = 0;
    size_t native16 = 0;
    for (auto& item : Items) {
        if (item.ValidityMask) {
            item.ValidityOffset += ValidityOffset;
        }
        switch (item.Storage) {
            case EStorage::Unboxed:
                item.Offset = unboxed++ * sizeof(TUnboxedValuePod);
                break;
            case EStorage::Native64:
                item.Offset = native64Offset + native64++ * sizeof(ui64);
                break;
            case EStorage::Native32:
                item.Offset = native32Offset + native32++ * sizeof(ui32);
                break;
            case EStorage::Native16:
                item.Offset = native16Offset + native16++ * sizeof(ui16);
                break;
        }
    }
    LogicalItems = Items;
    std::sort(LogicalItems.begin(), LogicalItems.end(), [](const TItem& left, const TItem& right) {
        return left.LogicalIndex < right.LogicalIndex;
    });
}

void TDqHashCombineTupleLayout::CopyWithRefs(const void* source, void* destination) const {
    std::memcpy(destination, source, Size);
    auto* values = static_cast<TUnboxedValuePod*>(destination);
    for (size_t i = 0; i < UnboxedCount; ++i) {
        values[i].Ref();
    }
}

void TDqHashCombineTupleLayout::UnpackCopy(const void* storage, TArrayRef<TUnboxedValue> values) const {
    Y_ABORT_UNLESS(values.size() == Items.size());
    for (const auto& item : Items) {
        if (item.Storage == EStorage::Unboxed) {
            values[item.LogicalIndex] = *reinterpret_cast<const TUnboxedValuePod*>(
                static_cast<const char*>(storage) + item.Offset);
        } else if (item.Storage == EStorage::Native64) {
            values[item.LogicalIndex] = UnpackNativeItem<ui64>(item, storage);
        } else if (item.Storage == EStorage::Native32) {
            values[item.LogicalIndex] = UnpackNativeItem<ui32>(item, storage);
        } else {
            values[item.LogicalIndex] = UnpackNativeItem<ui16>(item, storage);
        }
    }
}

void TDqHashCombineTupleLayout::Clear(void* storage) const {
    if (UnboxedCount) {
        std::memset(storage, 0, UnboxedCount * sizeof(TUnboxedValuePod));
    }
    if (ValidityWordCount) {
        std::memset(static_cast<char*>(storage) + ValidityOffset, 0, ValidityWordCount * sizeof(ui32));
    }
}

void TDqHashCombineTupleLayout::Destroy(void* storage) const {
    auto* values = static_cast<TUnboxedValuePod*>(storage);
    for (size_t i = 0; i < UnboxedCount; ++i) {
        values[i].UnRef();
        values[i] = TUnboxedValuePod{};
    }
}

std::optional<size_t> TDqHashCombineTupleLayout::GetStaticExternalMemorySize() const {
    size_t result = 0;
    for (const auto& item : Items) {
        if (item.Storage != EStorage::Unboxed) {
            continue;
        }
        const auto size = GetStaticUvSizeBound(Types[item.LogicalIndex]);
        if (!size) {
            return {};
        }
        result += *size - sizeof(TUnboxedValuePod);
    }
    return result;
}

std::optional<size_t> TDqHashCombineTupleLayout::EstimateExternalMemorySize(const void* storage) const {
    size_t result = 0;
    for (const auto& item : Items) {
        if (item.Storage != EStorage::Unboxed) {
            continue;
        }
        const auto& value = *reinterpret_cast<const TUnboxedValuePod*>(static_cast<const char*>(storage) + item.Offset);
        const auto size = EstimateValueMemorySize(value, Types[item.LogicalIndex]);
        if (!size) {
            return {};
        }
        result += *size - sizeof(TUnboxedValuePod);
    }
    return result;
}

} // namespace NKikimr::NMiniKQL
