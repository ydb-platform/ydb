#include "wb_merge.h"

namespace NKikimr::NViewer {

using namespace NNodeWhiteboard;
using namespace ::google::protobuf;

std::unordered_map<TWhiteboardMergerBase::TMergerKey, TWhiteboardMergerBase::TMergerValue> TWhiteboardMergerBase::FieldMerger;

void TWhiteboardMergerBase::ProtoMaximizeEnumField(
        const ::google::protobuf::Reflection& reflectionTo,
        const ::google::protobuf::Reflection& reflectionFrom,
        ::google::protobuf::Message& protoTo,
        const ::google::protobuf::Message& protoFrom,
        const ::google::protobuf::FieldDescriptor* field) {
    bool has = reflectionTo.HasField(protoTo, field);
    auto val = reflectionFrom.GetEnum(protoFrom, field);
    if (!has || reflectionTo.GetEnum(protoTo, field)->number() < val->number()) {
        reflectionTo.SetEnum(&protoTo, field, val);
    }
}

void TWhiteboardMergerBase::ProtoMaximizeBoolField(
        const ::google::protobuf::Reflection& reflectionTo,
        const ::google::protobuf::Reflection& reflectionFrom,
        ::google::protobuf::Message& protoTo,
        const ::google::protobuf::Message& protoFrom,
        const ::google::protobuf::FieldDescriptor* field) {
    bool has = reflectionTo.HasField(protoTo, field);
    auto val = reflectionFrom.GetBool(protoFrom, field);
    if (!has || reflectionTo.GetBool(protoTo, field) < val) {
        reflectionTo.SetBool(&protoTo, field, val);
    }
}

void TWhiteboardMergerBase::ProtoMerge(google::protobuf::Message& protoTo, const google::protobuf::Message& protoFrom) {
    using namespace ::google::protobuf;
    const Descriptor& descriptor = *protoTo.GetDescriptor();
    const Reflection& reflectionTo = *protoTo.GetReflection();
    const Reflection& reflectionFrom = *protoFrom.GetReflection();
    int fieldCount = descriptor.field_count();
    for (int index = 0; index < fieldCount; ++index) {
        const FieldDescriptor* field = descriptor.field(index);
        auto it = FieldMerger.find(field);
        if (it != FieldMerger.end()) {
            it->second(reflectionTo, reflectionFrom, protoTo, protoFrom, field);
            continue;
        }
        if (field->is_repeated()) {
            FieldDescriptor::CppType type = field->cpp_type();
            int size = reflectionFrom.FieldSize(protoFrom, field);
            if (size != 0 && reflectionTo.FieldSize(protoTo, field) != size) {
                reflectionTo.ClearField(&protoTo, field);
                for (int i = 0; i < size; ++i) {
                    switch (type) {
                    case FieldDescriptor::CPPTYPE_INT32:
                        reflectionTo.AddInt32(&protoTo, field, reflectionFrom.GetRepeatedInt32(protoFrom, field, i));
                        break;
                    case FieldDescriptor::CPPTYPE_INT64:
                        reflectionTo.AddInt64(&protoTo, field, reflectionFrom.GetRepeatedInt64(protoFrom, field, i));
                        break;
                    case FieldDescriptor::CPPTYPE_UINT32:
                        reflectionTo.AddUInt32(&protoTo, field, reflectionFrom.GetRepeatedUInt32(protoFrom, field, i));
                        break;
                    case FieldDescriptor::CPPTYPE_UINT64:
                        reflectionTo.AddUInt64(&protoTo, field, reflectionFrom.GetRepeatedUInt64(protoFrom, field, i));
                        break;
                    case FieldDescriptor::CPPTYPE_DOUBLE:
                        reflectionTo.AddDouble(&protoTo, field, reflectionFrom.GetRepeatedDouble(protoFrom, field, i));
                        break;
                    case FieldDescriptor::CPPTYPE_FLOAT:
                        reflectionTo.AddFloat(&protoTo, field, reflectionFrom.GetRepeatedFloat(protoFrom, field, i));
                        break;
                    case FieldDescriptor::CPPTYPE_BOOL:
                        reflectionTo.AddBool(&protoTo, field, reflectionFrom.GetRepeatedBool(protoFrom, field, i));
                        break;
                    case FieldDescriptor::CPPTYPE_ENUM:
                        reflectionTo.AddEnum(&protoTo, field, reflectionFrom.GetRepeatedEnum(protoFrom, field, i));
                        break;
                    case FieldDescriptor::CPPTYPE_STRING:
                        reflectionTo.AddString(&protoTo, field, reflectionFrom.GetRepeatedString(protoFrom, field, i));
                        break;
                    case FieldDescriptor::CPPTYPE_MESSAGE:
                        reflectionTo.AddMessage(&protoTo, field)->CopyFrom(reflectionFrom.GetRepeatedMessage(protoFrom, field, i));
                        break;
                    }
                }
            } else {
                for (int i = 0; i < size; ++i) {
                    switch (type) {
                    case FieldDescriptor::CPPTYPE_INT32: {
                        auto val = reflectionFrom.GetRepeatedInt32(protoFrom, field, i);
                        if (val != reflectionTo.GetRepeatedInt32(protoTo, field, i)) {
                            reflectionTo.SetRepeatedInt32(&protoTo, field, i, val);
                        }
                        break;
                    }
                    case FieldDescriptor::CPPTYPE_INT64: {
                        auto val = reflectionFrom.GetRepeatedInt64(protoFrom, field, i);
                        if (val != reflectionTo.GetRepeatedInt64(protoTo, field, i)) {
                            reflectionTo.SetRepeatedInt64(&protoTo, field, i, val);
                        }
                        break;
                    }
                    case FieldDescriptor::CPPTYPE_UINT32: {
                        auto val = reflectionFrom.GetRepeatedUInt32(protoFrom, field, i);
                        if (val != reflectionTo.GetRepeatedUInt32(protoTo, field, i)) {
                            reflectionTo.SetRepeatedUInt32(&protoTo, field, i, val);
                        }
                        break;
                    }
                    case FieldDescriptor::CPPTYPE_UINT64: {
                        auto val = reflectionFrom.GetRepeatedUInt64(protoFrom, field, i);
                        if (val != reflectionTo.GetRepeatedUInt64(protoTo, field, i)) {
                            reflectionTo.SetRepeatedUInt64(&protoTo, field, i, val);
                        }
                        break;
                    }
                    case FieldDescriptor::CPPTYPE_DOUBLE: {
                        auto val = reflectionFrom.GetRepeatedDouble(protoFrom, field, i);
                        if (val != reflectionTo.GetRepeatedDouble(protoTo, field, i)) {
                            reflectionTo.SetRepeatedDouble(&protoTo, field, i, val);
                        }
                        break;
                    }
                    case FieldDescriptor::CPPTYPE_FLOAT: {
                        auto val = reflectionFrom.GetRepeatedFloat(protoFrom, field, i);
                        if (val != reflectionTo.GetRepeatedFloat(protoTo, field, i)) {
                            reflectionTo.SetRepeatedFloat(&protoTo, field, i, val);
                        }
                        break;
                    }
                    case FieldDescriptor::CPPTYPE_BOOL: {
                        auto val = reflectionFrom.GetRepeatedBool(protoFrom, field, i);
                        if (val != reflectionTo.GetRepeatedBool(protoTo, field, i)) {
                            reflectionTo.SetRepeatedBool(&protoTo, field, i, val);
                        }
                        break;
                    }
                    case FieldDescriptor::CPPTYPE_ENUM: {
                        auto val = reflectionFrom.GetRepeatedEnum(protoFrom, field, i);
                        if (val->number() != reflectionTo.GetRepeatedEnum(protoTo, field, i)->number()) {
                            reflectionTo.SetRepeatedEnum(&protoTo, field, i, val);
                        }
                        break;
                    }
                    case FieldDescriptor::CPPTYPE_STRING: {
                        auto val = reflectionFrom.GetRepeatedString(protoFrom, field, i);
                        if (val != reflectionTo.GetRepeatedString(protoTo, field, i)) {
                            reflectionTo.SetRepeatedString(&protoTo, field, i, val);
                        }
                        break;
                    }
                    case FieldDescriptor::CPPTYPE_MESSAGE:
                        ProtoMerge(*reflectionTo.MutableRepeatedMessage(&protoTo, field, i), reflectionFrom.GetRepeatedMessage(protoFrom, field, i));
                        break;
                    }
                }
            }
        } else {
            if (reflectionFrom.HasField(protoFrom, field)) {
                FieldDescriptor::CppType type = field->cpp_type();
                switch (type) {
                case FieldDescriptor::CPPTYPE_INT32: {
                    ProtoMergeField(reflectionTo, reflectionFrom, protoTo, protoFrom, field, &Reflection::GetInt32, &Reflection::SetInt32);
                    break;
                }
                case FieldDescriptor::CPPTYPE_INT64: {
                    ProtoMergeField(reflectionTo, reflectionFrom, protoTo, protoFrom, field, &Reflection::GetInt64, &Reflection::SetInt64);
                    break;
                }
                case FieldDescriptor::CPPTYPE_UINT32: {
                    ProtoMergeField(reflectionTo, reflectionFrom, protoTo, protoFrom, field, &Reflection::GetUInt32, &Reflection::SetUInt32);
                    break;
                }
                case FieldDescriptor::CPPTYPE_UINT64: {
                    ProtoMergeField(reflectionTo, reflectionFrom, protoTo, protoFrom, field, &Reflection::GetUInt64, &Reflection::SetUInt64);
                    break;
                }
                case FieldDescriptor::CPPTYPE_DOUBLE: {
                    ProtoMergeField(reflectionTo, reflectionFrom, protoTo, protoFrom, field, &Reflection::GetDouble, &Reflection::SetDouble);
                    break;
                }
                case FieldDescriptor::CPPTYPE_FLOAT: {
                    ProtoMergeField(reflectionTo, reflectionFrom, protoTo, protoFrom, field, &Reflection::GetFloat, &Reflection::SetFloat);
                    break;
                }
                case FieldDescriptor::CPPTYPE_BOOL: {
                    ProtoMergeField(reflectionTo, reflectionFrom, protoTo, protoFrom, field, &Reflection::GetBool, &Reflection::SetBool);
                    break;
                }
                case FieldDescriptor::CPPTYPE_ENUM: {
                    bool has = reflectionTo.HasField(protoTo, field);
                    auto val = reflectionFrom.GetEnum(protoFrom, field);
                    if (!has || reflectionTo.GetEnum(protoTo, field)->number() != val->number()) {
                        reflectionTo.SetEnum(&protoTo, field, val);
                    }
                    break;
                }
                case FieldDescriptor::CPPTYPE_STRING: {
                    bool has = reflectionTo.HasField(protoTo, field);
                    auto val = reflectionFrom.GetString(protoFrom, field);
                    if (!has || reflectionTo.GetString(protoTo, field) != val) {
                        reflectionTo.SetString(&protoTo, field, val);
                    }
                    break;
                }
                case FieldDescriptor::CPPTYPE_MESSAGE:
                    ProtoMerge(*reflectionTo.MutableMessage(&protoTo, field), reflectionFrom.GetMessage(protoFrom, field));
                    break;
                }
            }
        }
    }
}

}
