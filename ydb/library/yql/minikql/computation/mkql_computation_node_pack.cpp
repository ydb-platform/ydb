#include "mkql_computation_node_pack.h"
#include "mkql_computation_node_pack_impl.h"
#include "mkql_computation_node_holders.h"
#include "presort.h"
#include <ydb/library/yql/parser/pg_wrapper/interface/pack.h>
#include <ydb/library/yql/public/decimal/yql_decimal.h>
#include <ydb/library/yql/public/decimal/yql_decimal_serialize.h>
#include <ydb/library/yql/minikql/defs.h>
#include <ydb/library/yql/minikql/pack_num.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <library/cpp/resource/resource.h>
#include <ydb/library/yql/utils/fp_bits.h>

#include <util/system/yassert.h>
#include <util/system/sanitizers.h>

namespace NKikimr {
namespace NMiniKQL {

template<bool Fast>
TValuePackerImpl<Fast>::TValuePackerImpl(bool stable, const TType* type)
    : Stable_(stable)
    , Type_(type)
    , Properties_(ScanTypeProperties(Type_))
    , OptionalMaskReserve_(Properties_.Test(EProps::UseOptionalMask) ? 1 : 0)
{
    MKQL_ENSURE(!Fast || !Stable_, "Stable mode is not supported");
}

template<bool Fast>
TValuePackerImpl<Fast>::TValuePackerImpl(const TValuePackerImpl<Fast>& other)
    : Stable_(other.Stable_)
    , Type_(other.Type_)
    , Properties_(other.Properties_)
    , OptionalMaskReserve_(other.OptionalMaskReserve_)
{}

template <bool Fast>
std::pair<ui32, bool> TValuePackerImpl<Fast>::SkipEmbeddedLength(TStringBuf& buf) {
    if constexpr (Fast) {
        Y_FAIL("Should not be called");
    }
    ui32 length = 0;
    bool emptySingleOptional = false;
    if (buf.size() > 8) {
        length = ReadUnaligned<ui32>(buf.data());
        MKQL_ENSURE(length + 4 == buf.size(), "Bad packed data. Invalid embedded size");
        buf.Skip(4);
    } else {
        length = *buf.data();
        MKQL_ENSURE(length & 1, "Bad packed data. Invalid embedded size");
        emptySingleOptional = 0 != (length & 0x10);
        length = (length & 0x0f) >> 1;
        MKQL_ENSURE(length + 1 == buf.size(), "Bad packed data. Invalid embedded size");
        buf.Skip(1);
    }
    return {length, emptySingleOptional};
}

template<bool Fast>
NUdf::TUnboxedValue TValuePackerImpl<Fast>::Unpack(TStringBuf buf, const THolderFactory& holderFactory) const {
    if constexpr (Fast) {
        NUdf::TUnboxedValue res;
        res = UnpackImpl(Type_, buf, 0, holderFactory);
        MKQL_ENSURE(buf.empty(), "Bad packed data. Not fully data read");
        return res;
    }

    auto pair = SkipEmbeddedLength(buf);
    ui32 length = pair.first;
    bool emptySingleOptional = pair.second;

    if (Properties_.Test(EProps::UseOptionalMask)) {
        OptionalUsageMask_.Reset(buf);
    }
    NUdf::TUnboxedValue res;
    if (Properties_.Test(EProps::SingleOptional) && emptySingleOptional) {
        res = NUdf::TUnboxedValuePod();
    } else if (Type_->IsStruct()) {
        auto structType = static_cast<const TStructType*>(Type_);
        NUdf::TUnboxedValue * items = nullptr;
        res = TopStruct_.NewArray(holderFactory, structType->GetMembersCount(), items);
        for (ui32 index = 0; index < structType->GetMembersCount(); ++index) {
            auto memberType = structType->GetMemberType(index);
            *items++ = UnpackImpl(memberType, buf, length, holderFactory);
        }
    } else {
        res = UnpackImpl(Type_, buf, length, holderFactory);
    }

    MKQL_ENSURE(buf.empty(), "Bad packed data. Not fully data read");
    return res;
}

template<bool Fast>
NUdf::TUnboxedValue TValuePackerImpl<Fast>::UnpackImpl(const TType* type, TStringBuf& buf, ui32 topLength,
    const THolderFactory& holderFactory) const
{
    switch (type->GetKind()) {
    case TType::EKind::Void:
        return NUdf::TUnboxedValuePod::Void();
    case TType::EKind::Null:
        return NUdf::TUnboxedValuePod();
    case TType::EKind::EmptyList:
        return holderFactory.GetEmptyContainer();
    case TType::EKind::EmptyDict:
        return holderFactory.GetEmptyContainer();

    case TType::EKind::Data: {
        auto dataType = static_cast<const TDataType*>(type);
        switch (*dataType->GetDataSlot()) {
        case NUdf::EDataSlot::Bool:
            return NUdf::TUnboxedValuePod(NDetails::GetRawData<bool>(buf));
        case NUdf::EDataSlot::Int8:
            return NUdf::TUnboxedValuePod(NDetails::GetRawData<i8>(buf));
        case NUdf::EDataSlot::Uint8:
            return NUdf::TUnboxedValuePod(NDetails::GetRawData<ui8>(buf));
        case NUdf::EDataSlot::Int16:
            if constexpr (Fast) {
                return NUdf::TUnboxedValuePod(NDetails::GetRawData<i16>(buf));
            } else {
                return NUdf::TUnboxedValuePod(NDetails::UnpackInt16(buf));
            }
        case NUdf::EDataSlot::Uint16:
            if constexpr (Fast) {
                return NUdf::TUnboxedValuePod(NDetails::GetRawData<ui16>(buf));
            } else {
                return NUdf::TUnboxedValuePod(NDetails::UnpackUInt16(buf));
            }
        case NUdf::EDataSlot::Int32:
            if constexpr (Fast) {
                return NUdf::TUnboxedValuePod(NDetails::GetRawData<i32>(buf));
            } else {
                return NUdf::TUnboxedValuePod(NDetails::UnpackInt32(buf));
            }
        case NUdf::EDataSlot::Uint32:
            if constexpr (Fast) {
                return NUdf::TUnboxedValuePod(NDetails::GetRawData<ui32>(buf));
            } else {
                return NUdf::TUnboxedValuePod(NDetails::UnpackUInt32(buf));
            }
        case NUdf::EDataSlot::Int64:
            if constexpr (Fast) {
                return NUdf::TUnboxedValuePod(NDetails::GetRawData<i64>(buf));
            } else {
                return NUdf::TUnboxedValuePod(NDetails::UnpackInt64(buf));
            }
        case NUdf::EDataSlot::Uint64:
            if constexpr (Fast) {
                return NUdf::TUnboxedValuePod(NDetails::GetRawData<ui64>(buf));
            } else {
                return NUdf::TUnboxedValuePod(NDetails::UnpackUInt64(buf));
            }
        case NUdf::EDataSlot::Float:
            return NUdf::TUnboxedValuePod(NDetails::GetRawData<float>(buf));
        case NUdf::EDataSlot::Double:
            return NUdf::TUnboxedValuePod(NDetails::GetRawData<double>(buf));
        case NUdf::EDataSlot::Date:
            if constexpr (Fast) {
                return NUdf::TUnboxedValuePod(NDetails::GetRawData<ui16>(buf));
            } else {
                return NUdf::TUnboxedValuePod(NDetails::UnpackUInt16(buf));
            }
        case NUdf::EDataSlot::Datetime:
            if constexpr (Fast) {
                return NUdf::TUnboxedValuePod(NDetails::GetRawData<ui32>(buf));
            } else {
                return NUdf::TUnboxedValuePod(NDetails::UnpackUInt32(buf));
            }
        case NUdf::EDataSlot::Timestamp:
            if constexpr (Fast) {
                return NUdf::TUnboxedValuePod(NDetails::GetRawData<ui64>(buf));
            } else {
                return NUdf::TUnboxedValuePod(NDetails::UnpackUInt64(buf));
            }
        case NUdf::EDataSlot::Interval:
            if constexpr (Fast) {
                return NUdf::TUnboxedValuePod(NDetails::GetRawData<i64>(buf));
            } else {
                return NUdf::TUnboxedValuePod(NDetails::UnpackInt64(buf));
            }
        case NUdf::EDataSlot::TzDate: {
            ui16 value;
            if constexpr (Fast) {
                value = NDetails::GetRawData<ui16>(buf);
            } else {
                value = NDetails::UnpackUInt16(buf);
            }

            ui16 tzId;
            if constexpr (Fast) {
                tzId = NDetails::GetRawData<ui16>(buf);
            } else {
                tzId = NDetails::UnpackUInt16(buf);
            }

            auto ret = NUdf::TUnboxedValuePod(value);
            ret.SetTimezoneId(tzId);
            return ret;
        }
        case NUdf::EDataSlot::TzDatetime: {
            ui32 value;
            if constexpr (Fast) {
                value = NDetails::GetRawData<ui32>(buf);
            } else {
                value = NDetails::UnpackUInt32(buf);
            }

            ui16 tzId;
            if constexpr (Fast) {
                tzId = NDetails::GetRawData<ui16>(buf);
            } else {
                tzId = NDetails::UnpackUInt16(buf);
            }

            auto ret = NUdf::TUnboxedValuePod(value);
            ret.SetTimezoneId(tzId);
            return ret;
        }
        case NUdf::EDataSlot::TzTimestamp: {
            ui64 value;
            if constexpr (Fast) {
                value = NDetails::GetRawData<ui64>(buf);
            } else {
                value = NDetails::UnpackUInt64(buf);
            }

            ui16 tzId;
            if constexpr (Fast) {
                tzId = NDetails::GetRawData<ui16>(buf);
            } else {
                tzId = NDetails::UnpackUInt16(buf);
            }

            auto ret = NUdf::TUnboxedValuePod(value);
            ret.SetTimezoneId(tzId);
            return ret;
        }
        case NUdf::EDataSlot::Uuid: {
            MKQL_ENSURE(16 <= buf.size(), "Bad packed data. Buffer too small");
            const char* ptr = buf.data();
            buf.Skip(16);
            return MakeString(NUdf::TStringRef(ptr, 16));
        }
        case NUdf::EDataSlot::Decimal: {
            const auto des = NYql::NDecimal::Deserialize(buf.data(), buf.size());
            MKQL_ENSURE(!NYql::NDecimal::IsError(des.first), "Bad packed data: invalid decimal.");
            buf.Skip(des.second);
            return NUdf::TUnboxedValuePod(des.first);
        }
        default:
            ui32 size = 0;
            if constexpr (Fast) {
                size = NDetails::GetRawData<ui32>(buf);
            } else {
                if (Properties_.Test(EProps::UseTopLength)) {
                    size = topLength;
                } else {
                    size = NDetails::UnpackUInt32(buf);
                }
            }
            MKQL_ENSURE(size <= buf.size(), "Bad packed data. Buffer too small");
            const char* ptr = buf.data();
            buf.Skip(size);
            return MakeString(NUdf::TStringRef(ptr, size));
        }
        break;
    }

    case TType::EKind::Optional: {
        auto optionalType = static_cast<const TOptionalType*>(type);
        bool present;
        if constexpr (Fast) {
            present = NDetails::GetRawData<ui8>(buf);
        } else {
            present = !OptionalUsageMask_.IsNextEmptyOptional();
        }

        if (present) {
            return UnpackImpl(optionalType->GetItemType(), buf, topLength, holderFactory).Release().MakeOptional();
        } else {
            return NUdf::TUnboxedValuePod();
        }
    }

    case TType::EKind::Pg: {
        auto pgType = static_cast<const TPgType*>(type);
        bool present;
        if constexpr (Fast) {
            present = NDetails::GetRawData<ui8>(buf);
        } else {
            present = !OptionalUsageMask_.IsNextEmptyOptional();
        }
        if (present) {
            return PGUnpackImpl(pgType, buf);
        } else {
            return NUdf::TUnboxedValuePod();
        }
    }

    case TType::EKind::List: {
        auto listType = static_cast<const TListType*>(type);
        auto itemType = listType->GetItemType();

        ui64 len;
        if constexpr (Fast) {
            len = NDetails::GetRawData<ui64>(buf);
        } else {
            len = NDetails::UnpackUInt64(buf);
        }

        if (!len) {
            return holderFactory.GetEmptyContainer();
        }

        TTemporaryUnboxedValueVector tmp;
        for (ui64 i = 0; i < len; ++i) {
            tmp.emplace_back(UnpackImpl(itemType, buf, topLength, holderFactory));
        }

        NUdf::TUnboxedValue *items = nullptr;
        auto list = holderFactory.CreateDirectArrayHolder(len, items);
        for (ui64 i = 0; i < len; ++i) {
            items[i] = std::move(tmp[i]);
        }

        return std::move(list);
    }

    case TType::EKind::Struct: {
        auto structType = static_cast<const TStructType*>(type);
        NUdf::TUnboxedValue* itemsPtr = nullptr;
        auto res = holderFactory.CreateDirectArrayHolder(structType->GetMembersCount(), itemsPtr);
        for (ui32 index = 0; index < structType->GetMembersCount(); ++index) {
            auto memberType = structType->GetMemberType(index);
            itemsPtr[index] = UnpackImpl(memberType, buf, topLength, holderFactory);
        }
        return std::move(res);
    }

    case TType::EKind::Tuple: {
        auto tupleType = static_cast<const TTupleType*>(type);
        NUdf::TUnboxedValue* itemsPtr = nullptr;
        auto res = holderFactory.CreateDirectArrayHolder(tupleType->GetElementsCount(), itemsPtr);
        for (ui32 index = 0; index < tupleType->GetElementsCount(); ++index) {
            auto elementType = tupleType->GetElementType(index);
            itemsPtr[index] = UnpackImpl(elementType, buf, topLength, holderFactory);
        }
        return std::move(res);
    }

    case TType::EKind::Dict: {
        auto dictType = static_cast<const TDictType*>(type);
        auto keyType = dictType->GetKeyType();
        auto payloadType = dictType->GetPayloadType();
        auto dictBuilder = holderFactory.NewDict(dictType, NUdf::TDictFlags::EDictKind::Hashed);

        ui64 len;
        if constexpr (Fast) {
            len = NDetails::GetRawData<ui64>(buf);
        } else {
            len = NDetails::UnpackUInt64(buf);
        }

        for (ui64 i = 0; i < len; ++i) {
            auto key = UnpackImpl(keyType, buf, topLength, holderFactory);
            auto payload = UnpackImpl(payloadType, buf, topLength, holderFactory);
            dictBuilder->Add(std::move(key), std::move(payload));
        }
        return dictBuilder->Build();
    }

    case TType::EKind::Variant: {
        auto variantType = static_cast<const TVariantType*>(type);
        ui32 variantIndex;
        if constexpr (Fast) {
            variantIndex = NDetails::GetRawData<ui32>(buf);
        } else {
            variantIndex = NDetails::UnpackUInt32(buf);
        }

        TType* innerType = variantType->GetUnderlyingType();
        if (innerType->IsStruct()) {
            MKQL_ENSURE(variantIndex < static_cast<TStructType*>(innerType)->GetMembersCount(), "Bad variant index: " << variantIndex);
            innerType = static_cast<TStructType*>(innerType)->GetMemberType(variantIndex);
        } else {
            MKQL_ENSURE(innerType->IsTuple(), "Unexpected underlying variant type: " << innerType->GetKindAsStr());
            MKQL_ENSURE(variantIndex < static_cast<TTupleType*>(innerType)->GetElementsCount(), "Bad variant index: " << variantIndex);
            innerType = static_cast<TTupleType*>(innerType)->GetElementType(variantIndex);
        }
        return holderFactory.CreateVariantHolder(UnpackImpl(innerType, buf, topLength, holderFactory).Release(), variantIndex);
    }

    case TType::EKind::Tagged: {
        auto taggedType = static_cast<const TTaggedType*>(type);
        return UnpackImpl(taggedType->GetBaseType(), buf, topLength, holderFactory);
    }

    default:
        THROW yexception() << "Unsupported type: " << type->GetKindAsStr();
    }
}

template<bool Fast>
TStringBuf TValuePackerImpl<Fast>::Pack(const NUdf::TUnboxedValuePod& value) const {
    if constexpr (Fast) {
        Buffer_.Proceed(0);
        PackImpl(Type_, value);
        const char* data = Buffer_.Data();
        size_t len = Buffer_.Size();
        NSan::Unpoison(data, len);
        return TStringBuf(data, len);
    }

    OptionalUsageMask_.Reset();
    const size_t lengthReserve = sizeof(ui32);
    Buffer_.Proceed(lengthReserve + OptionalMaskReserve_);

    PackImpl(Type_, value);

    size_t delta = 0;
    size_t len = Buffer_.Size();

    if (Properties_.Test(EProps::UseOptionalMask)) {
        // Prepend optional mask
        const size_t actualOptionalMaskSize = OptionalUsageMask_.CalcSerializedSize();

        if (actualOptionalMaskSize > OptionalMaskReserve_) {
            TBuffer buf(Buffer_.Size() + actualOptionalMaskSize - OptionalMaskReserve_);
            buf.Proceed(actualOptionalMaskSize - OptionalMaskReserve_);
            buf.Append(Buffer_.Data(), Buffer_.Size());
            Buffer_.Swap(buf);
            OptionalMaskReserve_ = actualOptionalMaskSize;
            len = Buffer_.Size();
        }

        delta = OptionalMaskReserve_ - actualOptionalMaskSize;
        Buffer_.Proceed(lengthReserve + delta);
        OptionalUsageMask_.Serialize(Buffer_);
    }

    // Prepend length
    if (len - delta - lengthReserve > 7) {
        const ui32 length = len - delta - lengthReserve;
        Buffer_.Proceed(delta);
        Buffer_.Append((const char*)&length, sizeof(length));
        // Long length always singnals non-empty optional. So, don't check EProps::SingleOptional here
    } else {
        ui8 length = 1 | ((len - delta - lengthReserve) << 1);
        // Empty root optional always has short length. Embed empty flag into the length
        if (Properties_.Test(EProps::SingleOptional) && !OptionalUsageMask_.IsEmptyMask()) {
            length |= 0x10;
        }
        delta += 3;
        Buffer_.Proceed(delta);
        Buffer_.Append((const char*)&length, sizeof(length));
    }
    NSan::Unpoison(Buffer_.Data() + delta, len - delta);
    return TStringBuf(Buffer_.Data() + delta, len - delta);
}

template<bool Fast>
void TValuePackerImpl<Fast>::PackImpl(const TType* type, const NUdf::TUnboxedValuePod& value) const {
    switch (type->GetKind()) {
    case TType::EKind::Void:
        break;
    case TType::EKind::Null:
        break;
    case TType::EKind::EmptyList:
        break;
    case TType::EKind::EmptyDict:
        break;

    case TType::EKind::Data: {
        auto dataType = static_cast<const TDataType*>(type);
        switch (*dataType->GetDataSlot()) {
        case NUdf::EDataSlot::Bool:
            NDetails::PutRawData(value.Get<bool>(), Buffer_);
            break;
        case NUdf::EDataSlot::Int8:
            NDetails::PutRawData(value.Get<i8>(), Buffer_);
            break;
        case NUdf::EDataSlot::Uint8:
            NDetails::PutRawData(value.Get<ui8>(), Buffer_);
            break;
        case NUdf::EDataSlot::Int16:
            if constexpr (Fast) {
                NDetails::PutRawData(value.Get<i16>(), Buffer_);
            } else {
                NDetails::PackInt16(value.Get<i16>(), Buffer_);
            }
            break;
        case NUdf::EDataSlot::Uint16:
            if constexpr (Fast) {
                NDetails::PutRawData(value.Get<ui16>(), Buffer_);
            } else {
                NDetails::PackUInt16(value.Get<ui16>(), Buffer_);
            }
            break;
        case NUdf::EDataSlot::Int32:
            if constexpr (Fast) {
                NDetails::PutRawData(value.Get<i32>(), Buffer_);
            } else {
                NDetails::PackInt32(value.Get<i32>(), Buffer_);
            }
            break;
        case NUdf::EDataSlot::Uint32:
            if constexpr (Fast) {
                NDetails::PutRawData(value.Get<ui32>(), Buffer_);
            } else {
                NDetails::PackUInt32(value.Get<ui32>(), Buffer_);
            }
            break;
        case NUdf::EDataSlot::Int64:
            if constexpr (Fast) {
                NDetails::PutRawData(value.Get<i64>(), Buffer_);
            } else {
                NDetails::PackInt64(value.Get<i64>(), Buffer_);
            }
            break;
        case NUdf::EDataSlot::Uint64:
            if constexpr (Fast) {
                NDetails::PutRawData(value.Get<ui64>(), Buffer_);
            } else {
                NDetails::PackUInt64(value.Get<ui64>(), Buffer_);
            }
            break;
        case NUdf::EDataSlot::Float: {
            float x = value.Get<float>();
            if constexpr (!Fast) {
                if (Stable_) {
                    NYql::CanonizeFpBits<float>(&x);
                }
            }

            NDetails::PutRawData(x, Buffer_);
            break;
        }
        case NUdf::EDataSlot::Double: {
            double x = value.Get<double>();
            if constexpr (!Fast) {
                if (Stable_) {
                    NYql::CanonizeFpBits<double>(&x);
                }
            }

            NDetails::PutRawData(x, Buffer_);
            break;
        }
        case NUdf::EDataSlot::Date:
            if constexpr (Fast) {
                NDetails::PutRawData(value.Get<ui16>(), Buffer_);
            } else {
                NDetails::PackUInt16(value.Get<ui16>(), Buffer_);
            }
            break;
        case NUdf::EDataSlot::Datetime:
            if constexpr (Fast) {
                NDetails::PutRawData(value.Get<ui32>(), Buffer_);
            } else {
                NDetails::PackUInt32(value.Get<ui32>(), Buffer_);
            }
            break;
        case NUdf::EDataSlot::Timestamp:
            if constexpr (Fast) {
                NDetails::PutRawData(value.Get<ui64>(), Buffer_);
            } else {
                NDetails::PackUInt64(value.Get<ui64>(), Buffer_);
            }
            break;
        case NUdf::EDataSlot::Interval:
            if constexpr (Fast) {
                NDetails::PutRawData(value.Get<i64>(), Buffer_);
            } else {
                NDetails::PackInt64(value.Get<i64>(), Buffer_);
            }
            break;
        case NUdf::EDataSlot::Uuid: {
            auto ref = value.AsStringRef();
            Buffer_.Append(ref.Data(), ref.Size());
            break;
        }
        case NUdf::EDataSlot::TzDate: {
            if constexpr (Fast) {
                NDetails::PutRawData(value.Get<ui16>(), Buffer_);
                NDetails::PutRawData(value.GetTimezoneId(), Buffer_);
            } else {
                NDetails::PackUInt16(value.Get<ui16>(), Buffer_);
                NDetails::PackUInt16(value.GetTimezoneId(), Buffer_);
            }
            break;
        }
        case NUdf::EDataSlot::TzDatetime: {
            if constexpr (Fast) {
                NDetails::PutRawData(value.Get<ui32>(), Buffer_);
                NDetails::PutRawData(value.GetTimezoneId(), Buffer_);
            } else {
                NDetails::PackUInt32(value.Get<ui32>(), Buffer_);
                NDetails::PackUInt16(value.GetTimezoneId(), Buffer_);
            }
            break;
        }
        case NUdf::EDataSlot::TzTimestamp: {
            if constexpr (Fast) {
                NDetails::PutRawData(value.Get<ui64>(), Buffer_);
                NDetails::PutRawData(value.GetTimezoneId(), Buffer_);
            } else {
                NDetails::PackUInt64(value.Get<ui64>(), Buffer_);
                NDetails::PackUInt16(value.GetTimezoneId(), Buffer_);
            }
            break;
        }
        case NUdf::EDataSlot::Decimal: {
            char buff[0x10U];
            Buffer_.Append(buff, NYql::NDecimal::Serialize(value.GetInt128(), buff));
            break;
        }
        default: {
            auto stringRef = value.AsStringRef();
            if constexpr (Fast) {
                static_assert(std::is_same_v<decltype(stringRef.Size()), ui32>);
                NDetails::PutRawData(stringRef.Size(), Buffer_);
            } else {
                if (!Properties_.Test(EProps::UseTopLength)) {
                    NDetails::PackUInt32(stringRef.Size(), Buffer_);
                }
            }
            Buffer_.Append(stringRef.Data(), stringRef.Size());
        }
        }
        break;
    }

    case TType::EKind::Optional: {
        auto optionalType = static_cast<const TOptionalType*>(type);
        if constexpr (Fast) {
            NDetails::PutRawData(ui8(bool(value)), Buffer_);
        } else {
            OptionalUsageMask_.SetNextEmptyOptional(!value);
        }
        if (value) {
            PackImpl(optionalType->GetItemType(), value.GetOptionalValue());
        }
        break;
    }

    case TType::EKind::Pg: {
        auto pgType = static_cast<const TPgType*>(type);
        if constexpr (Fast) {
            NDetails::PutRawData(ui8(bool(value)), Buffer_);
        } else {
            OptionalUsageMask_.SetNextEmptyOptional(!value);
        }
        if (value) {
            PGPackImpl(Stable_, pgType, value, Buffer_);
        }
        break;
    }

    case TType::EKind::List: {
        auto listType = static_cast<const TListType*>(type);
        auto itemType = listType->GetItemType();
        if (value.HasFastListLength()) {
            ui64 len = value.GetListLength();
            if constexpr (Fast) {
                NDetails::PutRawData(len, Buffer_);
            } else {
                NDetails::PackUInt64(len, Buffer_);
            }
            TThresher<false>::DoForEachItem(value,
                [this, itemType](const NYql::NUdf::TUnboxedValuePod& item) { PackImpl(itemType, item); });
        } else {
            const auto iter = value.GetListIterator();
            if constexpr (Fast) {
                size_t pos = Buffer_.Size();
                ui64 count = 0;
                Buffer_.Advance(sizeof(count));
                for (NUdf::TUnboxedValue item; iter.Next(item);) {
                    PackImpl(itemType, item);
                    ++count;
                }
                std::memcpy(Buffer_.Data() + pos, &count, sizeof(count));
            } else {
                TUnboxedValueVector items;
                for (NUdf::TUnboxedValue item; iter.Next(item);) {
                    items.emplace_back(std::move(item));
                }
                NDetails::PackUInt64(items.size(), Buffer_);
                for (const auto& item : items) {
                    PackImpl(itemType, item);
                }
            }
        }
        break;
    }

    case TType::EKind::Struct: {
        auto structType = static_cast<const TStructType*>(type);
        for (ui32 index = 0; index < structType->GetMembersCount(); ++index) {
            auto memberType = structType->GetMemberType(index);
            PackImpl(memberType, value.GetElement(index));
        }
        break;
    }

    case TType::EKind::Tuple: {
        auto tupleType = static_cast<const TTupleType*>(type);
        for (ui32 index = 0; index < tupleType->GetElementsCount(); ++index) {
            auto elementType = tupleType->GetElementType(index);
            PackImpl(elementType, value.GetElement(index));
        }
        break;
    }

    case TType::EKind::Dict:  {
        auto dictType = static_cast<const TDictType*>(type);
        auto keyType = dictType->GetKeyType();
        auto payloadType = dictType->GetPayloadType();

        ui64 length = value.GetDictLength();
        if constexpr (Fast) {
            NDetails::PutRawData(length, Buffer_);
        } else {
            NDetails::PackUInt64(length, Buffer_);
        }
        const auto iter = value.GetDictIterator();
        if constexpr (Fast) {
            for (NUdf::TUnboxedValue key, payload; iter.NextPair(key, payload);) {
                PackImpl(keyType, key);
                PackImpl(payloadType, payload);
            }
        } else {
            if (Stable_ && !value.IsSortedDict()) {
                // no key duplicates here
                TKeyTypes types;
                bool isTuple;
                bool encoded;
                bool useIHash;
                GetDictionaryKeyTypes(keyType, types, isTuple, encoded, useIHash);
                if (encoded) {
                    TGenericPresortEncoder packer(keyType);
                    typename decltype(EncodedDictBuffers_)::value_type dictBuffer;
                    if (!EncodedDictBuffers_.empty()) {
                        dictBuffer = std::move(EncodedDictBuffers_.back());
                        EncodedDictBuffers_.pop_back();
                        dictBuffer.clear();
                    }
                    dictBuffer.reserve(length);
                    for (NUdf::TUnboxedValue key, payload; iter.NextPair(key, payload);) {
                        NUdf::TUnboxedValue encodedKey = MakeString(packer.Encode(key, false));
                        dictBuffer.emplace_back(std::move(encodedKey), std::move(key), std::move(payload));
                    }

                    Sort(dictBuffer.begin(), dictBuffer.end(),
                        [&](const auto &left, const auto &right) {
                            return CompareKeys(std::get<0>(left), std::get<0>(right), types, isTuple) < 0;
                        });

                    for (const auto& x : dictBuffer) {
                        PackImpl(keyType, std::get<1>(x));
                        PackImpl(payloadType, std::get<2>(x));
                    }
                    dictBuffer.clear();
                    EncodedDictBuffers_.push_back(std::move(dictBuffer));
                } else {
                    typename decltype(DictBuffers_)::value_type dictBuffer;
                    if (!DictBuffers_.empty()) {
                        dictBuffer = std::move(DictBuffers_.back());
                        DictBuffers_.pop_back();
                        dictBuffer.clear();
                    }
                    dictBuffer.reserve(length);
                    for (NUdf::TUnboxedValue key, payload; iter.NextPair(key, payload);) {
                        dictBuffer.emplace_back(std::move(key), std::move(payload));
                    }

                    Sort(dictBuffer.begin(), dictBuffer.end(), TKeyPayloadPairLess(types, isTuple, useIHash ? MakeCompareImpl(keyType) : nullptr));
                    for (const auto& p: dictBuffer) {
                        PackImpl(keyType, p.first);
                        PackImpl(payloadType, p.second);
                    }
                    dictBuffer.clear();
                    DictBuffers_.push_back(std::move(dictBuffer));
                }    
            } else {
                for (NUdf::TUnboxedValue key, payload; iter.NextPair(key, payload);) {
                    PackImpl(keyType, key);
                    PackImpl(payloadType, payload);
                }
            }
        }
        break;
    }

    case TType::EKind::Variant: {
        auto variantType = static_cast<const TVariantType*>(type);
        ui32 variantIndex = value.GetVariantIndex();
        TType* innerType = variantType->GetUnderlyingType();
        if (innerType->IsStruct()) {
            innerType = static_cast<TStructType*>(innerType)->GetMemberType(variantIndex);
        } else {
            MKQL_ENSURE(innerType->IsTuple(), "Unexpected underlying variant type: " << innerType->GetKindAsStr());
            innerType = static_cast<TTupleType*>(innerType)->GetElementType(variantIndex);
        }
        if constexpr (Fast) {
            NDetails::PutRawData(variantIndex, Buffer_);
        } else {
            NDetails::PackUInt32(variantIndex, Buffer_);
        }
        PackImpl(innerType, value.GetVariantItem());
        break;
    }

    case TType::EKind::Tagged: {
        auto taggedType = static_cast<const TTaggedType*>(type);
        PackImpl(taggedType->GetBaseType(), value);
        break;
    }

    default:
        THROW yexception() << "Unsupported type: " << type->GetKindAsStr();
    }
}

template<bool Fast>
typename TValuePackerImpl<Fast>::TProperties TValuePackerImpl<Fast>::ScanTypeProperties(const TType* type) {
    if constexpr (Fast) {
        return {};
    }
    TProperties props;
    if (HasOptionalFields(type)) {
        props.Set(EProps::UseOptionalMask);
    }
    if (type->GetKind() == TType::EKind::Optional) {
        type = static_cast<const TOptionalType*>(type)->GetItemType();
        if (!HasOptionalFields(type)) {
            props.Set(EProps::SingleOptional);
            props.Reset(EProps::UseOptionalMask);
        }
    }
    // Here and after the type is unwrapped!!

    if (type->GetKind() == TType::EKind::Data) {
        auto dataType = static_cast<const TDataType*>(type);
        switch (*dataType->GetDataSlot()) {
        case NUdf::EDataSlot::String:
        case NUdf::EDataSlot::Json:
        case NUdf::EDataSlot::Yson:
        case NUdf::EDataSlot::Utf8:
        case NUdf::EDataSlot::JsonDocument:
            // Reuse entire packed value length for strings
            props.Set(EProps::UseTopLength);
            break;
        default:
            break;
        }
    }
    return props;
}

template<bool Fast>
bool TValuePackerImpl<Fast>::HasOptionalFields(const TType* type) {
    switch (type->GetKind()) {
    case TType::EKind::Void:
    case TType::EKind::Null:
    case TType::EKind::EmptyList:
    case TType::EKind::EmptyDict:
    case TType::EKind::Data:
        return false;

    case TType::EKind::Optional:
        return true;

    case TType::EKind::Pg:
        return true;

    case TType::EKind::List:
        return HasOptionalFields(static_cast<const TListType*>(type)->GetItemType());

    case TType::EKind::Struct: {
        auto structType = static_cast<const TStructType*>(type);
        for (ui32 index = 0; index < structType->GetMembersCount(); ++index) {
            if (HasOptionalFields(structType->GetMemberType(index))) {
                return true;
            }
        }
        return false;
    }

    case TType::EKind::Tuple: {
        auto tupleType = static_cast<const TTupleType*>(type);
        for (ui32 index = 0; index < tupleType->GetElementsCount(); ++index) {
            if (HasOptionalFields(tupleType->GetElementType(index))) {
                return true;
            }
        }
        return false;
    }

    case TType::EKind::Dict:  {
        auto dictType = static_cast<const TDictType*>(type);
        return HasOptionalFields(dictType->GetKeyType()) || HasOptionalFields(dictType->GetPayloadType());
    }

    case TType::EKind::Variant:  {
        auto variantType = static_cast<const TVariantType*>(type);
        return HasOptionalFields(variantType->GetUnderlyingType());
    }

    case TType::EKind::Tagged:  {
        auto taggedType = static_cast<const TTaggedType*>(type);
        return HasOptionalFields(taggedType->GetBaseType());
    }

    default:
        THROW yexception() << "Unsupported type: " << type->GetKindAsStr();
    }
}

template class TValuePackerImpl<true>;
template class TValuePackerImpl<false>;

TValuePackerBoxed::TValuePackerBoxed(TMemoryUsageInfo* memInfo, bool stable, const TType* type)
    : TBase(memInfo)
    , TValuePacker(stable, type)
{}

TValuePackerBoxed::TValuePackerBoxed(TMemoryUsageInfo* memInfo, const TValuePacker& other)
    : TBase(memInfo)
    , TValuePacker(other)
{}

} // NMiniKQL
} // NKikimr
