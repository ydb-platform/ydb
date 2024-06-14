#include "mkql_proto.h"

#include <ydb/library/yql/minikql/defs.h>
#include <ydb/library/yql/minikql/mkql_node_visitor.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/public/decimal/yql_decimal.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/type_desc.h>

#include <ydb/core/scheme_types/scheme_types_defs.h>

namespace NKikimr::NMiniKQL {

// NOTE: TCell's can reference memomry from tupleValue
// TODO: Place notNull flag in to the NScheme::TTypeInfo?
bool CellsFromTuple(const NKikimrMiniKQL::TType* tupleType,
                    const NKikimrMiniKQL::TValue& tupleValue,
                    const TConstArrayRef<NScheme::TTypeInfo>& types,
                    TVector<bool> notNullTypes,
                    bool allowCastFromString,
                    TVector<TCell>& key,
                    TString& errStr,
                    TVector<TString>& memoryOwner)
{

#define CHECK_OR_RETURN_ERROR(cond, descr) \
    if (!(cond)) { \
        errStr = descr; \
        return false; \
    }

    // Please note we modify notNullTypes during tuplyType verification to allow cast nullable to non nullable value 
    if (notNullTypes) {
        CHECK_OR_RETURN_ERROR(notNullTypes.size() == types.size(),
            "The size of type array and given not null markers must be equial");
        if (tupleType) {
            CHECK_OR_RETURN_ERROR(notNullTypes.size() >= tupleType->GetTuple().ElementSize(),
                "The size of tuple type and given not null markers must be equal");
        }
    } else {
        notNullTypes.resize(types.size());
    }

    if (tupleType) {
        CHECK_OR_RETURN_ERROR(tupleType->GetKind() == NKikimrMiniKQL::Tuple ||
                              (tupleType->GetKind() == NKikimrMiniKQL::Unknown && tupleType->GetTuple().ElementSize() == 0), "Must be a tuple");
        CHECK_OR_RETURN_ERROR(tupleType->GetTuple().ElementSize() <= types.size(),
            "Tuple size " + ToString(tupleType->GetTuple().ElementSize()) + " is greater that expected size " + ToString(types.size()));

        for (size_t i = 0; i < tupleType->GetTuple().ElementSize(); ++i) {
            const auto& ti = tupleType->GetTuple().GetElement(i);
            if (notNullTypes[i]) {
                // For not null column type we allow to build cell from nullable mkql type for compatibility reason. 
                notNullTypes[i] = ti.GetKind() != NKikimrMiniKQL::Optional;
            } else {
                // But we do not allow to build cell for nullable column from not nullable type
                CHECK_OR_RETURN_ERROR(ti.GetKind() == NKikimrMiniKQL::Optional, "Element at index " + ToString(i) + " in not an Optional");
            }
            const auto& item = notNullTypes[i] ? ti : ti.GetOptional().GetItem();
            CHECK_OR_RETURN_ERROR(item.GetKind() == NKikimrMiniKQL::Data, "Element at index " + ToString(i) + " Item kind is not Data");
            const auto& typeId = item.GetData().GetScheme();
            CHECK_OR_RETURN_ERROR(typeId == types[i].GetTypeId() ||
                allowCastFromString && (typeId == NScheme::NTypeIds::Utf8),
                "Element at index " + ToString(i) + " has type " + ToString(typeId) + " but expected type is " + ToString(types[i].GetTypeId()));
        }

        CHECK_OR_RETURN_ERROR(tupleType->GetTuple().ElementSize() == tupleValue.TupleSize(),
            Sprintf("Tuple value length %" PRISZT " doesn't match the length in type %" PRISZT, tupleValue.TupleSize(), tupleType->GetTuple().ElementSize()));
    } else {
        CHECK_OR_RETURN_ERROR(types.size() >= tupleValue.TupleSize(),
            Sprintf("Tuple length %" PRISZT " is greater than key column count %" PRISZT, tupleValue.TupleSize(), types.size()));
    }

    for (ui32 i = 0; i < tupleValue.TupleSize(); ++i) {

        auto& o = tupleValue.GetTuple(i);

        if (notNullTypes[i]) {
            CHECK_OR_RETURN_ERROR(o.ListSize() == 0 &&
                                  o.StructSize() == 0 &&
                                  o.TupleSize() == 0 &&
                                  o.DictSize() == 0 &&
                                  !o.HasOptional(),
                                  Sprintf("Primitive type is expected in tuple at position %" PRIu32, i));
        } else {
            auto element_case = o.value_value_case();

            CHECK_OR_RETURN_ERROR(element_case == NKikimrMiniKQL::TValue::kOptional ||
                                  element_case == NKikimrMiniKQL::TValue::VALUE_VALUE_NOT_SET,
                                  Sprintf("Optional type is expected in tuple at position %" PRIu32, i));

            CHECK_OR_RETURN_ERROR(o.ListSize() == 0 &&
                                  o.StructSize() == 0 &&
                                  o.TupleSize() == 0 &&
                                  o.DictSize() == 0,
                                  Sprintf("Optional type is expected in tuple at position %" PRIu32, i));

            if (!o.HasOptional()) {
                key.push_back(TCell());
                continue;
            }
        }

        auto& v = notNullTypes[i] ? o : o.GetOptional();

        auto value_case = v.value_value_case();

        CHECK_OR_RETURN_ERROR(value_case != NKikimrMiniKQL::TValue::kOptional &&
                              value_case != NKikimrMiniKQL::TValue::VALUE_VALUE_NOT_SET,
                              Sprintf("Data must be present at position %" PRIu32, i));

        CHECK_OR_RETURN_ERROR(v.ListSize() == 0 &&
                              v.StructSize() == 0 &&
                              v.TupleSize() == 0 &&
                              v.DictSize() == 0,
                              Sprintf("Simple type is expected in tuple at position %" PRIu32, i));

        TCell c;
        auto typeId = types[i].GetTypeId();
        switch (typeId) {

#define CASE_SIMPLE_TYPE(name, type, protoField) \
        case NScheme::NTypeIds::name: \
        { \
            bool valuePresent = v.Has##protoField(); \
            if (valuePresent) { \
                type val = v.Get##protoField(); \
                c = TCell((const char*)&val, sizeof(val)); \
            } else if (allowCastFromString && v.HasText()) { \
                const auto slot = NUdf::GetDataSlot(typeId); \
                const auto out = NMiniKQL::ValueFromString(slot, v.GetText()); \
                CHECK_OR_RETURN_ERROR(out, Sprintf("Cannot parse value of type " #name " from text '%s' in tuple at position %" PRIu32, v.GetText().data(), i)); \
                const auto val = out.Get<type>(); \
                c = TCell((const char*)&val, sizeof(val)); \
            } else { \
                CHECK_OR_RETURN_ERROR(false, Sprintf("Value of type " #name " expected in tuple at position %" PRIu32, i)); \
            } \
            Y_ABORT_UNLESS(c.IsInline()); \
            break; \
        }

        CASE_SIMPLE_TYPE(Bool,   bool,  Bool);
        CASE_SIMPLE_TYPE(Int8,   i8,    Int32);
        CASE_SIMPLE_TYPE(Uint8,  ui8,   Uint32);
        CASE_SIMPLE_TYPE(Int16,  i16,   Int32);
        CASE_SIMPLE_TYPE(Uint16, ui16,  Uint32);
        CASE_SIMPLE_TYPE(Int32,  i32,   Int32);
        CASE_SIMPLE_TYPE(Uint32, ui32,  Uint32);
        CASE_SIMPLE_TYPE(Int64,  i64,   Int64);
        CASE_SIMPLE_TYPE(Uint64, ui64,  Uint64);
        CASE_SIMPLE_TYPE(Float,  float, Float);
        CASE_SIMPLE_TYPE(Double, double,Double);
        CASE_SIMPLE_TYPE(Date,   ui16,  Uint32);
        CASE_SIMPLE_TYPE(Datetime, ui32, Uint32);
        CASE_SIMPLE_TYPE(Timestamp, ui64, Uint64);
        CASE_SIMPLE_TYPE(Interval, i64, Int64);
        CASE_SIMPLE_TYPE(Date32,   i32,  Int32);
        CASE_SIMPLE_TYPE(Datetime64, i64, Int64);
        CASE_SIMPLE_TYPE(Timestamp64, i64, Int64);
        CASE_SIMPLE_TYPE(Interval64, i64, Int64);


#undef CASE_SIMPLE_TYPE

        case NScheme::NTypeIds::Yson:
        case NScheme::NTypeIds::Json:
        case NScheme::NTypeIds::Utf8:
        {
            c = TCell(v.GetText().data(), v.GetText().size());
            break;
        }
        case NScheme::NTypeIds::JsonDocument:
        case NScheme::NTypeIds::DyNumber:
        {
            c = TCell(v.GetBytes().data(), v.GetBytes().size());
            break;
        }
        case NScheme::NTypeIds::String:
        {
            if (v.HasBytes()) {
                c = TCell(v.GetBytes().data(), v.GetBytes().size());
            } else if (allowCastFromString && v.HasText()) {
                c = TCell(v.GetText().data(), v.GetText().size());
            } else {
                CHECK_OR_RETURN_ERROR(false, Sprintf("Cannot parse value of type String in tuple at position %" PRIu32, i));
            }
            break;
        }
        case NScheme::NTypeIds::Pg:
        {
            if (v.HasBytes()) {
                c = TCell(v.GetBytes().data(), v.GetBytes().size());
            } else if (v.HasText()) {
                auto typeDesc = types[i].GetTypeDesc();
                auto convert = NPg::PgNativeBinaryFromNativeText(v.GetText(), NPg::PgTypeIdFromTypeDesc(typeDesc));
                if (convert.Error) {
                    CHECK_OR_RETURN_ERROR(false, Sprintf("Cannot parse value of type Pg: %s in tuple at position %" PRIu32, convert.Error->data(), i));
                } else {
                    auto &data = memoryOwner.emplace_back(convert.Str);
                    c = TCell(data);
                }
            } else {
                CHECK_OR_RETURN_ERROR(false, Sprintf("Cannot parse value of type Pg in tuple at position %" PRIu32, i));
            }
            break;
        }
        case NScheme::NTypeIds::Uuid:
        {
            if (v.HasLow128()) {
                if (!v.HasHi128()) {
                    CHECK_OR_RETURN_ERROR(false, Sprintf("UUID has Low128 but not Hi128 at position: %" PRIu32, i));
                }
                auto &data = memoryOwner.emplace_back();
                data.resize(NUuid::UUID_LEN);
                NUuid::UuidHalfsToBytes(data.Detach(), data.size(), v.GetHi128(), v.GetLow128());
                c = TCell(data);
            } else if (v.HasBytes()) {
                Y_ABORT_UNLESS(v.GetBytes().size() == NUuid::UUID_LEN);
                c = TCell(v.GetBytes().data(), v.GetBytes().size());
            } else {
                CHECK_OR_RETURN_ERROR(false, Sprintf("Cannot parse value of type Uuid in tuple at position %" PRIu32, i));
            }
            break;
        }
        default:
            CHECK_OR_RETURN_ERROR(false, Sprintf("Unsupported typeId %" PRIu16 " at index %" PRIu32, typeId, i));
            break;
        }

        CHECK_OR_RETURN_ERROR(!c.IsNull(), Sprintf("Invalid non-NULL value at index %" PRIu32, i));
        key.push_back(c);
    }

#undef CHECK_OR_RETURN_ERROR

    return true;
}

bool CellToValue(NScheme::TTypeInfo type, const TCell& c, NKikimrMiniKQL::TValue& val, TString& errStr) {
    if (c.IsNull()) {
        return true;
    }

    auto typeId = type.GetTypeId();
    switch (typeId) {
    case NScheme::NTypeIds::Int8:
        Y_ABORT_UNLESS(c.Size() == sizeof(i8));
        val.MutableOptional()->SetInt32(*(i8*)c.Data());
        break;
    case NScheme::NTypeIds::Uint8:
        Y_ABORT_UNLESS(c.Size() == sizeof(ui8));
        val.MutableOptional()->SetUint32(*(ui8*)c.Data());
        break;

    case NScheme::NTypeIds::Int16:
        Y_ABORT_UNLESS(c.Size() == sizeof(i16));
        val.MutableOptional()->SetInt32(ReadUnaligned<i16>(c.Data()));
        break;
    case NScheme::NTypeIds::Uint16:
        Y_ABORT_UNLESS(c.Size() == sizeof(ui16));
        val.MutableOptional()->SetUint32(ReadUnaligned<ui16>(c.Data()));
        break;

    case NScheme::NTypeIds::Date32:
    case NScheme::NTypeIds::Int32:
        Y_ABORT_UNLESS(c.Size() == sizeof(i32));
        val.MutableOptional()->SetInt32(ReadUnaligned<i32>(c.Data()));
        break;
    case NScheme::NTypeIds::Uint32:
        Y_ABORT_UNLESS(c.Size() == sizeof(ui32));
        val.MutableOptional()->SetUint32(ReadUnaligned<ui32>(c.Data()));
        break;

    case NScheme::NTypeIds::Int64:
        Y_ABORT_UNLESS(c.Size() == sizeof(i64));
        val.MutableOptional()->SetInt64(ReadUnaligned<i64>(c.Data()));
        break;
    case NScheme::NTypeIds::Uint64:
        Y_ABORT_UNLESS(c.Size() == sizeof(ui64));
        val.MutableOptional()->SetUint64(ReadUnaligned<ui64>(c.Data()));
        break;

    case NScheme::NTypeIds::Bool:
        Y_ABORT_UNLESS(c.Size() == sizeof(bool));
        val.MutableOptional()->SetBool(*(bool*)c.Data());
        break;

    case NScheme::NTypeIds::Float:
        Y_ABORT_UNLESS(c.Size() == sizeof(float));
        val.MutableOptional()->SetFloat(ReadUnaligned<float>(c.Data()));
        break;

    case NScheme::NTypeIds::Double:
        Y_ABORT_UNLESS(c.Size() == sizeof(double));
        val.MutableOptional()->SetDouble(ReadUnaligned<double>(c.Data()));
        break;

    case NScheme::NTypeIds::Date:
        Y_ABORT_UNLESS(c.Size() == sizeof(ui16));
        val.MutableOptional()->SetUint32(ReadUnaligned<i16>(c.Data()));
        break;
    case NScheme::NTypeIds::Datetime:
        Y_ABORT_UNLESS(c.Size() == sizeof(ui32));
        val.MutableOptional()->SetUint32(ReadUnaligned<ui32>(c.Data()));
        break;
    case NScheme::NTypeIds::Timestamp:
        Y_ABORT_UNLESS(c.Size() == sizeof(ui64));
        val.MutableOptional()->SetUint64(ReadUnaligned<ui64>(c.Data()));
        break;
    case NScheme::NTypeIds::Interval:
    case NScheme::NTypeIds::Interval64:
    case NScheme::NTypeIds::Timestamp64:
    case NScheme::NTypeIds::Datetime64:
        Y_ABORT_UNLESS(c.Size() == sizeof(i64));
        val.MutableOptional()->SetInt64(ReadUnaligned<i64>(c.Data()));
        break;

    case NScheme::NTypeIds::JsonDocument:
    case NScheme::NTypeIds::String:
    case NScheme::NTypeIds::DyNumber:
        val.MutableOptional()->SetBytes(c.Data(), c.Size());
        break;

    case NScheme::NTypeIds::Json:
    case NScheme::NTypeIds::Yson:
    case NScheme::NTypeIds::Utf8:
        val.MutableOptional()->SetText(c.Data(), c.Size());
        break;

    case NScheme::NTypeIds::Pg: {
        auto convert = NPg::PgNativeTextFromNativeBinary(c.AsBuf(), type.GetTypeDesc());
        if (convert.Error) {
            errStr = *convert.Error;
            return false;
        }
        val.MutableOptional()->SetText(convert.Str);
        break;
    }

    case NScheme::NTypeIds::Uuid: {
        ui64 high = 0, low = 0;
        NUuid::UuidBytesToHalfs(c.Data(), c.Size(), high, low);
        val.MutableOptional()->SetHi128(high);
        val.MutableOptional()->SetLow128(low);
        break;
    }

    default:
        errStr = "Unknown type: " + ToString(typeId);
        return false;
    }
    return true;
}


} // namspace NKikimr::NMiniKQL
