#pragma once

#include <ydb/library/yql/public/udf/udf_type_builder.h>
#include <library/cpp/protobuf/yql/descriptor.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/generic/ptr.h>
#include <util/generic/flags.h>

#include <google/protobuf/message.h>

namespace NYql {
namespace NUdf {

enum class EFieldFlag: ui16 {
    Void                = 1 << 0,
    Binary              = 1 << 1,
    OptionalContainer   = 1 << 2,
    Variant             = 1 << 3,
    Dict                = 1 << 4,
    EnumInt             = 1 << 5,
    EnumString          = 1 << 6,
};

struct TMessageInfo {
    struct TFieldInfo {
        ui32 Pos;
        TFlags<EFieldFlag> Flags;
    };
    TType* StructType = nullptr;
    ui32 FieldsCount = 0;
    THashMap<ui64, TFieldInfo> Fields;
    THashMap<ui64, ui32> VariantIndicies;
    THashMap<ui64, TType*> DictTypes;
};

enum class EProtoStringYqlType {
    Bytes,
    Utf8
};

// Don't reuse this structure between UDF calls. It caches TType*, which are valid only in specific scope.
struct TProtoInfo {
    using TMessageMap = THashMap<TString, std::shared_ptr<TMessageInfo>>;

    TType* StructType = nullptr;
    TMessageMap Messages;
    EEnumFormat EnumFormat = EEnumFormat::Number;
    ERecursionTraits Recursion = ERecursionTraits::Fail;
    bool YtMode = false;
    bool OptionalLists = false;
    bool SyntaxAware = false;
    EProtoStringYqlType StringType = EProtoStringYqlType::Bytes;
    bool UseJsonName = false;

    #define SET_VALUE(type, name) \
        TProtoInfo& With##name(const type value) { \
            name = value; \
            return static_cast<TProtoInfo&>(*this); \
        }
    SET_VALUE(EEnumFormat, EnumFormat);
    SET_VALUE(ERecursionTraits, Recursion);
    SET_VALUE(bool, YtMode);
    SET_VALUE(bool, OptionalLists);
    SET_VALUE(bool, SyntaxAware);
    SET_VALUE(EProtoStringYqlType, StringType);
    SET_VALUE(bool, UseJsonName);
};

void ProtoTypeBuild(const NProtoBuf::Descriptor* descriptor,
                    const EEnumFormat enumFormat,
                    const ERecursionTraits recursion,
                    const bool optionalLists,
                    IFunctionTypeInfoBuilder& builder,
                    TProtoInfo* info,
                    EProtoStringYqlType stringType = EProtoStringYqlType::Bytes,
                    const bool syntaxAware = false,
                    const bool useJsonName = false,
                    const bool ytMode = false);

template<class T>
void ProtoTypeBuild(IFunctionTypeInfoBuilder& builder, TProtoInfo* info) {
    ProtoTypeBuild(T::GetDescriptor(), info->EnumFormat, info->Recursion,
                   info->OptionalLists, builder, info, info->StringType, info->SyntaxAware,
                   info->UseJsonName, info->YtMode);
}

bool AvoidOptionalScalars(bool syntaxAware, const NProtoBuf::FieldDescriptor* fd);

} // namespace NUdf
} // namespace NYql
