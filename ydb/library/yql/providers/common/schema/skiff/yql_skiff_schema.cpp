#include "yql_skiff_schema.h"

#include <ydb/library/yql/providers/common/schema/parser/yql_type_parser.h>
#include <ydb/library/yql/providers/common/codec/yql_codec_type_flags.h>
#include <ydb/library/yql/public/udf/udf_data_type.h>

#include <library/cpp/yson/node/node_io.h>

#include <util/generic/yexception.h>

namespace NYql {
namespace NCommon {

struct TSkiffTypeLoader {
    typedef NYT::TNode TType;

    TSkiffTypeLoader(ui64 nativeYTTypesFlags)
        : NativeYTTypesFlags(nativeYTTypesFlags)
    {
    }

    TMaybe<TType> LoadVoidType(ui32 /*level*/) {
        return NYT::TNode()("wire_type", (NativeYTTypesFlags & NTCF_VOID) ? "nothing" : "yson32");
    }
    TMaybe<TType> LoadNullType(ui32 /*level*/) {
        return NYT::TNode()("wire_type", (NativeYTTypesFlags & NTCF_NULL) ? "nothing" : "yson32");
    }
    TMaybe<TType> LoadUnitType(ui32 /*level*/) {
        ythrow yexception() << "Unsupported type: Unit";
    }
    TMaybe<TType> LoadGenericType(ui32 /*level*/) {
        ythrow yexception() << "Unsupported type: Generic";
    }
    TMaybe<TType> LoadEmptyListType(ui32 /*level*/) {
        return NYT::TNode()("wire_type", (NativeYTTypesFlags & NTCF_COMPLEX) ? "nothing" : "yson32");
    }
    TMaybe<TType> LoadEmptyDictType(ui32 /*level*/) {
        return NYT::TNode()("wire_type", (NativeYTTypesFlags & NTCF_COMPLEX) ? "nothing" : "yson32");
    }
    TMaybe<TType> LoadDataType(const TString& dataType, ui32 /*level*/) {
        const auto slot = NUdf::FindDataSlot(dataType);
        if (!slot) {
            ythrow yexception() << "Unsupported data type: " << dataType;
        }

        switch (*slot) {
        case NUdf::EDataSlot::Bool:
            return NYT::TNode()("wire_type", "boolean");
        case NUdf::EDataSlot::Int8:
        case NUdf::EDataSlot::Int16:
        case NUdf::EDataSlot::Int32:
        case NUdf::EDataSlot::Int64:
        case NUdf::EDataSlot::Interval:
        case NUdf::EDataSlot::Date32:
        case NUdf::EDataSlot::Datetime64:
        case NUdf::EDataSlot::Timestamp64:
        case NUdf::EDataSlot::Interval64:
            return NYT::TNode()("wire_type", "int64");
        case NUdf::EDataSlot::Uint8:
        case NUdf::EDataSlot::Uint16:
        case NUdf::EDataSlot::Uint32:
        case NUdf::EDataSlot::Uint64:
        case NUdf::EDataSlot::Date:
        case NUdf::EDataSlot::Datetime:
        case NUdf::EDataSlot::Timestamp:
            return NYT::TNode()("wire_type", "uint64");
        case NUdf::EDataSlot::String:
        case NUdf::EDataSlot::Utf8:
        case NUdf::EDataSlot::Json:
        case NUdf::EDataSlot::Uuid:
        case NUdf::EDataSlot::DyNumber:
        case NUdf::EDataSlot::JsonDocument:
            return NYT::TNode()("wire_type", "string32");
        case NUdf::EDataSlot::Yson:
            return NYT::TNode()("wire_type", "yson32");
        case NUdf::EDataSlot::Float:
        case NUdf::EDataSlot::Double:
            return NYT::TNode()("wire_type", "double");
        case NUdf::EDataSlot::TzDate:
        case NUdf::EDataSlot::TzDatetime:
        case NUdf::EDataSlot::TzTimestamp:
        case NUdf::EDataSlot::TzDate32:
        case NUdf::EDataSlot::TzDatetime64:
        case NUdf::EDataSlot::TzTimestamp64:
            return NYT::TNode()("wire_type", "string32");
        case NUdf::EDataSlot::Decimal:
            ythrow yexception() << "Decimal type without parameters.";
            break;
        }

        ythrow yexception() << "Unsupported data type" << NUdf::GetDataTypeInfo(*slot).Name;
    }

    TMaybe<TType> LoadPgType(const TString& pgType, ui32 level) {
        if (!(NativeYTTypesFlags & NTCF_COMPLEX) && level > 1) {
            return NYT::TNode()("wire_type", "yson32");
        }

        TType itemType;
        if (pgType == "bool") {
            itemType = NYT::TNode()("wire_type", "boolean");
        } else if (pgType == "int2" || pgType == "int4" || pgType == "int8") {
            itemType = NYT::TNode()("wire_type", "int64");
        } else  if (pgType == "float4" || pgType == "float8") {
            itemType = NYT::TNode()("wire_type", "double");
        } else {
            itemType = NYT::TNode()("wire_type", "string32");
        }

        return NYT::TNode()
            ("wire_type", "variant8")
            ("children", NYT::TNode()
                .Add(NYT::TNode()("wire_type", "nothing"))
                .Add(std::move(itemType))
                );
    }

    TMaybe<TType> LoadDataTypeParams(const TString& dataType, const TString& paramOne, const TString& /*paramTwo*/, ui32 /*level*/) {
        const auto slot = NUdf::FindDataSlot(dataType);
        if (!slot) {
            ythrow yexception() << "Unsupported data type: " << dataType;
        }

        if (NUdf::EDataSlot::Decimal != slot) {
            ythrow yexception() << "Unexpected parameters for type: " << dataType;
        }

        ui8 precision = 0;
        if (!TryFromString(paramOne, precision) || !precision || precision > 35) {
            ythrow yexception() << "Invalid decimal precision: " << paramOne;
        }

        if (NativeYTTypesFlags & NTCF_DECIMAL) {
            if (precision < 10) {
                return NYT::TNode()("wire_type", "int32");
            } else if (precision < 19) {
                return NYT::TNode()("wire_type", "int64");
            } else {
                return NYT::TNode()("wire_type", "int128");
            }
        }

        return NYT::TNode()("wire_type", "string32");
    }

    TMaybe<TType> LoadResourceType(const TString& /*tag*/, ui32 /*level*/) {
        ythrow yexception() << "Unsupported type: Resource";
    }
    TMaybe<TType> LoadTaggedType(TType baseType, const TString& /*tag*/, ui32 /*level*/) {
        return baseType;
    }
    TMaybe<TType> LoadErrorType(ui32 /*row*/, ui32 /*column*/, const TString& /*file*/, const TString& msg, ui32 /*level*/) {
        ythrow yexception() << msg;
    }
    TMaybe<TType> LoadStructType(const TVector<std::pair<TString, TType>>& members, ui32 level) {
        if (!(NativeYTTypesFlags & NTCF_COMPLEX) && level > 0) {
            return NYT::TNode()("wire_type", "yson32");
        }
        auto children = NYT::TNode::CreateList();

        for (auto& item: members) {
            NYT::TNode innerNode = item.second;
            innerNode["name"] = item.first;
            children.Add(std::move(innerNode));
        }

        return NYT::TNode()
            ("wire_type", "tuple")
            ("children", std::move(children));
    }
    TMaybe<TType> LoadListType(TType itemType, ui32 /*level*/) {
        if (!(NativeYTTypesFlags & NTCF_COMPLEX)) {
            return NYT::TNode()("wire_type", "yson32");
        }
        return NYT::TNode()
            ("wire_type", "repeated_variant8")
            ("children", NYT::TNode()
                .Add(std::move(itemType))
            );
    }
    TMaybe<TType> LoadStreamType(TType /*itemType*/, ui32 /*level*/) {
        ythrow yexception() << "Unsupported type: Stream";
    }
    TMaybe<TType> LoadOptionalType(TType itemType, ui32 level) {
        if (!(NativeYTTypesFlags & NTCF_COMPLEX) && level > 1) {
            return NYT::TNode()("wire_type", "yson32");
        }
        return NYT::TNode()
            ("wire_type", "variant8")
            ("children", NYT::TNode()
                .Add(NYT::TNode()("wire_type", "nothing"))
                .Add(std::move(itemType))
            );
    }
    TMaybe<TType> LoadTupleType(const TVector<TType>& elements, ui32 /*level*/) {
        if (!(NativeYTTypesFlags & NTCF_COMPLEX)) {
            return NYT::TNode()("wire_type", "yson32");
        }

        auto children = NYT::TNode::CreateList();
        for (auto& inner: elements) {
            children.Add(inner);
        }

        return NYT::TNode()
            ("wire_type", "tuple")
            ("children", std::move(children));
    }
    TMaybe<TType> LoadDictType(TType keyType, TType valType, ui32 /*level*/) {
        if (!(NativeYTTypesFlags & NTCF_COMPLEX)) {
            return NYT::TNode()("wire_type", "yson32");
        }
        return NYT::TNode()
            ("wire_type", "repeated_variant8")
            ("children", NYT::TNode()
                .Add(NYT::TNode()
                    ("wire_type", "tuple")
                    ("children", NYT::TNode()
                        .Add(std::move(keyType))
                        .Add(std::move(valType))
                    )
                )
            );
    }
    TMaybe<TType> LoadCallableType(TType /*returnType*/, const TVector<TType>& /*argTypes*/, const TVector<TString>& /*argNames*/,
        const TVector<ui64>& /*argFlags*/, size_t /*optionalCount*/, const TString& /*payload*/, ui32 /*level*/) {

        ythrow yexception() << "Unsupported type: Callable";
    }
    TMaybe<TType> LoadVariantType(TType underlyingType, ui32 /*level*/) {
        if (!(NativeYTTypesFlags & NTCF_COMPLEX)) {
            return NYT::TNode()("wire_type", "yson32");
        }
        if (!underlyingType.IsMap() || !underlyingType.HasKey("children") || !underlyingType["children"].IsList()) {
            ythrow yexception() << "Bad variant underlying type: " << NYT::NodeToYsonString(underlyingType);
        }
        auto altCount = underlyingType["children"].AsList().size();
        underlyingType["wire_type"] = altCount < 256 ? "variant8" : "variant16";
        return underlyingType;
    }
    void Error(const TString& info) {
        ythrow yexception() << info;
    }

    ui64 NativeYTTypesFlags;
};

NYT::TNode ParseSkiffTypeFromYson(const NYT::TNode& node, ui64 nativeYTTypesFlags) {
    TSkiffTypeLoader loader(nativeYTTypesFlags);
    return DoLoadTypeFromYson(loader, node, 0).GetOrElse(NYT::TNode());
}

} // namespace NCommon
} // namespace NYql
