#include "type_builder.h"
#include "value_builder.h"

#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/udf/udf_value_builder.h>
#include <ydb/library/yql/public/udf/udf_terminator.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node.h>

namespace NYql {
namespace NUdf {

using namespace NProtoBuf;

TProtobufValue::TProtobufValue(const TProtoInfo& info)
    : Info_(info)
{
}

TProtobufValue::~TProtobufValue()
{ }

TUnboxedValue TProtobufValue::Run(
        const IValueBuilder* valueBuilder,
        const TUnboxedValuePod* args) const
{
    auto blob = args[0].AsStringRef();

    try {
        auto result = this->Parse(TStringBuf(blob.Data(), blob.Size()));
        if (result == nullptr) {
            return TUnboxedValue();
        }
        auto proto(result);
        return FillValueFromProto(*proto.Get(), valueBuilder, Info_);
    } catch (const std::exception& e) {
        UdfTerminate(e.what());
    }
}

TProtobufSerialize::TProtobufSerialize(const TProtoInfo& info)
    : Info_(info)
{
}

TProtobufSerialize::~TProtobufSerialize()
{ }

TUnboxedValue TProtobufSerialize::Run(
        const IValueBuilder* valueBuilder,
        const TUnboxedValuePod* args) const
{
    try {
        TAutoPtr<Message> proto = MakeProto();
        FillProtoFromValue(args[0], *proto, Info_);
        TMaybe<TString> result = this->Serialize(*proto);
        if (!result) {
            return TUnboxedValue();
        }
        return valueBuilder->NewString(*result);
    } catch (const std::exception& e) {
        UdfTerminate(e.what());
    }
}

namespace {

static TUnboxedValuePod CreateEnumValue(
        const IValueBuilder* valueBuilder,
        const NProtoBuf::EnumValueDescriptor* desc,
        const EEnumFormat format,
        TFlags<EFieldFlag> fieldFlags)
{
    if (fieldFlags.HasFlags(EFieldFlag::EnumInt)) {
        return TUnboxedValuePod((i64)desc->number());
    } else if (fieldFlags.HasFlags(EFieldFlag::EnumString)) {
        return valueBuilder->NewString(desc->name()).Release();
    }
    switch (format) {
    case EEnumFormat::Number:
        return TUnboxedValuePod((i32)desc->number());
    case EEnumFormat::Name:
        return valueBuilder->NewString(desc->name()).Release();
    case EEnumFormat::FullName:
        return valueBuilder->NewString(desc->full_name()).Release();
    }

    Y_UNREACHABLE();
}

static TUnboxedValuePod CreateSingleField(
    const IValueBuilder* valueBuilder,
    const Message& proto,
    const FieldDescriptor* fd,
    const TProtoInfo& info,
    TFlags<EFieldFlag> fieldFlags)
{
    auto r = proto.GetReflection();

#define FIELD_TO_VALUE(EProtoCppType, ProtoGet) \
case FieldDescriptor::EProtoCppType: { \
    return TUnboxedValuePod(r->ProtoGet(proto, fd)); \
}

    switch (fd->cpp_type()) {
        FIELD_TO_VALUE(CPPTYPE_INT32,  GetInt32);
        FIELD_TO_VALUE(CPPTYPE_INT64,  GetInt64);
        FIELD_TO_VALUE(CPPTYPE_UINT32, GetUInt32);
        FIELD_TO_VALUE(CPPTYPE_UINT64, GetUInt64);
        FIELD_TO_VALUE(CPPTYPE_DOUBLE, GetDouble);
        FIELD_TO_VALUE(CPPTYPE_BOOL,   GetBool);

        case FieldDescriptor::CPPTYPE_FLOAT: {
            const auto f = r->GetFloat(proto, fd);
            return info.YtMode ? TUnboxedValuePod(double(f)) : TUnboxedValuePod(f);
        }
        case FieldDescriptor::CPPTYPE_ENUM: {
            return CreateEnumValue(valueBuilder, r->GetEnum(proto, fd), info.EnumFormat, fieldFlags);
        }
        case FieldDescriptor::CPPTYPE_STRING: {
            return valueBuilder->NewString(r->GetString(proto, fd)).Release();
        }
        case FieldDescriptor::CPPTYPE_MESSAGE: {
            const auto& protoField = r->GetMessage(proto, fd);
            if (fieldFlags.HasFlags(EFieldFlag::Binary)) {
                return valueBuilder->NewString(protoField.SerializeAsString()).Release();
            } else {
                auto msg = FillValueFromProto(protoField, valueBuilder, info);
                return fd->is_optional() ? msg.Release().MakeOptional() : msg.Release();
            }
        }
    }
#undef FIELD_TO_VALUE

    return TUnboxedValuePod();
}

static TUnboxedValuePod CreateDefaultValue(
        const IValueBuilder* valueBuilder,
        const FieldDescriptor* fd,
        const TProtoInfo& info,
        TFlags<EFieldFlag> fieldFlags)
{
#define DEFAULT_TO_VALUE(EProtoCppType, ValueGet) \
case FieldDescriptor::EProtoCppType: { \
    return TUnboxedValuePod(fd->ValueGet()); \
    break; \
}

    switch (fd->cpp_type()) {
        DEFAULT_TO_VALUE(CPPTYPE_INT32,  default_value_int32);
        DEFAULT_TO_VALUE(CPPTYPE_INT64,  default_value_int64);
        DEFAULT_TO_VALUE(CPPTYPE_UINT32, default_value_uint32);
        DEFAULT_TO_VALUE(CPPTYPE_UINT64, default_value_uint64);
        DEFAULT_TO_VALUE(CPPTYPE_DOUBLE, default_value_double);
        DEFAULT_TO_VALUE(CPPTYPE_BOOL,   default_value_bool);

        case FieldDescriptor::CPPTYPE_FLOAT: {
            const auto f = fd->default_value_float();
            return info.YtMode ? TUnboxedValuePod(double(f)) : TUnboxedValuePod(f);
        }
        case FieldDescriptor::CPPTYPE_ENUM:
            return CreateEnumValue(valueBuilder, fd->default_value_enum(), info.EnumFormat, fieldFlags);

        case FieldDescriptor::CPPTYPE_STRING:
            return valueBuilder->NewString(fd->default_value_string()).Release();
        default:
            return TUnboxedValuePod();
}
#undef DEFAULT_TO_VALUE
}

static TUnboxedValuePod CreateRepeatedField(
        const IValueBuilder* valueBuilder,
        const Message& proto,
        const FieldDescriptor* fd,
        const TProtoInfo& info,
        TFlags<EFieldFlag> fieldFlags)
{
    auto r = proto.GetReflection();

#define REPEATED_FIELD_TO_VALUE(EProtoCppType, ProtoGet) \
case FieldDescriptor::EProtoCppType: { \
    for (int i = 0; i < endI; ++i) { \
        *items++ = TUnboxedValuePod(r->ProtoGet(proto, fd, i)); \
    } \
    break; \
}

    const auto endI = r->FieldSize(proto, fd);
    NUdf::TUnboxedValue *items = nullptr;
    auto list = valueBuilder->NewArray(endI, items);
    switch (fd->cpp_type()) {
        REPEATED_FIELD_TO_VALUE(CPPTYPE_INT32,  GetRepeatedInt32);
        REPEATED_FIELD_TO_VALUE(CPPTYPE_INT64,  GetRepeatedInt64);
        REPEATED_FIELD_TO_VALUE(CPPTYPE_UINT32, GetRepeatedUInt32);
        REPEATED_FIELD_TO_VALUE(CPPTYPE_UINT64, GetRepeatedUInt64);
        REPEATED_FIELD_TO_VALUE(CPPTYPE_DOUBLE, GetRepeatedDouble);
        REPEATED_FIELD_TO_VALUE(CPPTYPE_BOOL,   GetRepeatedBool);

        case FieldDescriptor::CPPTYPE_FLOAT:
            for (int i = 0; i < endI; ++i) {
                const auto f = r->GetRepeatedFloat(proto, fd, i);
                *items++ = info.YtMode ? TUnboxedValuePod(double(f)) : TUnboxedValuePod(f);
            }
            break;
        case FieldDescriptor::CPPTYPE_ENUM:
            for (int i = 0; i < endI; ++i) {
                *items++ = CreateEnumValue(valueBuilder, r->GetRepeatedEnum(proto, fd, i), info.EnumFormat, fieldFlags);
            }
            break;
        case FieldDescriptor::CPPTYPE_STRING:
            for (int i = 0; i < endI; ++i) {
                *items++ = valueBuilder->NewString(r->GetRepeatedString(proto, fd, i));
            }
            break;
        case FieldDescriptor::CPPTYPE_MESSAGE:
            for (int i = 0; i < endI; ++i) {
                const auto& protoFieldElement = r->GetRepeatedMessage(proto, fd, i);
                if (fieldFlags.HasFlags(EFieldFlag::Binary)) {
                    *items++ = valueBuilder->NewString(protoFieldElement.SerializeAsString());
                } else {
                    *items++ = FillValueFromProto(protoFieldElement, valueBuilder, info);
                }
            }
            break;
    }
#undef REPEATED_FIELD_TO_VALUE

    return list.Release();
}

static TUnboxedValuePod CreateMapField(
    const IValueBuilder* valueBuilder,
    const Message& proto,
    const FieldDescriptor* fd,
    const TProtoInfo& info,
    const TMessageInfo& msgInfo,
    TFlags<EFieldFlag> fieldFlags)
{
    auto r = proto.GetReflection();

    auto dictType = msgInfo.DictTypes.Value(fd->number(), nullptr);
    Y_ENSURE(dictType);
    auto dictBuilder = valueBuilder->NewDict(dictType, TDictFlags::Hashed);

    const auto noBinaryFlags = TFlags<EFieldFlag>(fieldFlags).RemoveFlags(EFieldFlag::Binary);
    for (int i = 0, end = r->FieldSize(proto, fd); i < end; ++i) {
        const auto& protoDictElement = r->GetRepeatedMessage(proto, fd, i);
        dictBuilder->Add(
            TUnboxedValue(CreateSingleField(valueBuilder, protoDictElement, fd->message_type()->map_key(), info, noBinaryFlags)),
            TUnboxedValue(CreateSingleField(valueBuilder, protoDictElement, fd->message_type()->map_value(), info, fieldFlags))
        );
    }

    return dictBuilder->Build().Release();
}

}

TUnboxedValue FillValueFromProto(
        const Message& proto,
        const IValueBuilder* valueBuilder,
        const TProtoInfo& info)
{
    const auto d  = proto.GetDescriptor();
    const auto r  = proto.GetReflection();
    const auto mi = info.Messages.find(d->full_name());

    if (mi == info.Messages.end()) {
        ythrow yexception() << "unknown message " << d->full_name();
    }

    const auto msgInfo = mi->second;
    TUnboxedValue* items = nullptr;
    auto value = valueBuilder->NewArray(msgInfo->FieldsCount, items);

    auto makeValue = [&](const FieldDescriptor* fd, const TMessageInfo::TFieldInfo& fInfo) -> TUnboxedValuePod {
        if (fInfo.Flags.HasFlags(EFieldFlag::Void)) {
            return TUnboxedValuePod::Void();
        }

        if (fd->is_map() && fInfo.Flags.HasFlags(EFieldFlag::Dict)) {
            if (r->FieldSize(proto, fd) == 0 && fInfo.Flags.HasFlags(EFieldFlag::OptionalContainer)) {
                return TUnboxedValuePod();
            } else {
                return CreateMapField(valueBuilder, proto, fd, info, *msgInfo, fInfo.Flags);
            }
        } else if (fd->is_optional()) {
            if (r->HasField(proto, fd)) {
                return CreateSingleField(valueBuilder, proto, fd, info, fInfo.Flags);
            } else if (fd->has_default_value() || AvoidOptionalScalars(info.SyntaxAware, fd)) {
                return CreateDefaultValue(valueBuilder, fd, info, fInfo.Flags);
            } else {
                return TUnboxedValuePod();
            }
        } else if (fd->is_repeated()) {
            if (r->FieldSize(proto, fd) > 0) {
                return CreateRepeatedField(valueBuilder, proto, fd, info, fInfo.Flags);
            } else {
                if (info.OptionalLists || fInfo.Flags.HasFlags(EFieldFlag::OptionalContainer)) {
                    return TUnboxedValuePod();
                } else {
                    return valueBuilder->NewEmptyList().Release();
                }
            }
        } else if (fd->is_required()) {
            if (r->HasField(proto, fd)) {
                return CreateSingleField(valueBuilder, proto, fd, info, fInfo.Flags);
            } else {
                ythrow yexception() << "required field " << fd->name() << " has no value";
            }
        }
        return TUnboxedValuePod();
    };

    THashSet<const OneofDescriptor*> visitedOneofs;
    for (int i = 0, end = d->field_count(); i < end; ++i) {
        const FieldDescriptor* fd = d->field(i);
        const auto& fInfo = msgInfo->Fields[fd->number()];

        if (auto oneofDescriptor = fd->containing_oneof(); info.YtMode && oneofDescriptor && fInfo.Flags.HasFlags(EFieldFlag::Variant)) {
            if (visitedOneofs.insert(oneofDescriptor).second) {
                items[fInfo.Pos] = TUnboxedValuePod();
                if (auto ofd = r->GetOneofFieldDescriptor(proto, oneofDescriptor)) {
                    const auto& ofInfo = msgInfo->Fields[ofd->number()];
                    if (fInfo.Pos != ofInfo.Pos) {
                        ythrow yexception() << "mismatch of oneof field " << ofd->name() << " position";
                    }
                    const ui32* varIndex = msgInfo->VariantIndicies.FindPtr(ofd->number());
                    if (!varIndex) {
                        ythrow yexception() << "missing oneof field " << ofd->name() << " index";
                    }
                    items[ofInfo.Pos] = valueBuilder->NewVariant(*varIndex, TUnboxedValue(makeValue(ofd, ofInfo))).Release().MakeOptional();
                }
            }
        } else {
            items[fInfo.Pos] = makeValue(fd, fInfo);
        }
    }

    return value;
}

} // namespace NUdf
} // namespace NYql
