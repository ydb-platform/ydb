#include "wb_aggregate.h"

namespace NKikimr::NViewer {

void AggregateMessage(::google::protobuf::Message& protoTo, const ::google::protobuf::Message& protoFrom) {
    const Reflection& reflectionFrom = *protoFrom.GetReflection();
    const Reflection& reflectionTo = *protoTo.GetReflection();
    const Descriptor& descriptorTo = *protoTo.GetDescriptor();
    std::vector<const FieldDescriptor*> fields;
    reflectionFrom.ListFields(protoFrom, &fields);
    for (auto it = fields.begin(); it != fields.end(); ++it) {
        const FieldDescriptor* fieldFrom = *it;
        const FieldDescriptor* fieldTo = descriptorTo.FindFieldByNumber(fieldFrom->number());
        FieldDescriptor::CppType type = fieldFrom->cpp_type();
        if (fieldFrom->is_repeated()) {
            int fromSize = reflectionFrom.FieldSize(protoFrom, fieldFrom);
            int toSize = std::min(reflectionTo.FieldSize(protoTo, fieldTo), fromSize);

            for (int i = 0; i < toSize; ++i) {
                switch (type) {
                case FieldDescriptor::CPPTYPE_INT32:
                    reflectionTo.SetRepeatedInt32(&protoTo, fieldTo, i,
                                                  reflectionTo.GetRepeatedInt32(protoTo, fieldTo, i) +
                                                  reflectionFrom.GetRepeatedInt32(protoFrom, fieldFrom, i));
                    break;
                case FieldDescriptor::CPPTYPE_INT64:
                    reflectionTo.SetRepeatedInt64(&protoTo, fieldTo, i,
                                                  reflectionTo.GetRepeatedInt64(protoTo, fieldTo, i) +
                                                  reflectionFrom.GetRepeatedInt64(protoFrom, fieldFrom, i));
                    break;
                case FieldDescriptor::CPPTYPE_UINT32:
                    reflectionTo.SetRepeatedUInt32(&protoTo, fieldTo, i,
                                                   reflectionTo.GetRepeatedUInt32(protoTo, fieldTo, i) +
                                                   reflectionFrom.GetRepeatedUInt32(protoFrom, fieldFrom, i));
                    break;
                case FieldDescriptor::CPPTYPE_UINT64:
                    reflectionTo.SetRepeatedUInt64(&protoTo, fieldTo, i,
                                                   reflectionTo.GetRepeatedUInt64(protoTo, fieldTo, i) +
                                                   reflectionFrom.GetRepeatedUInt64(protoFrom, fieldFrom, i));
                    break;
                case FieldDescriptor::CPPTYPE_DOUBLE:
                    reflectionTo.SetRepeatedDouble(&protoTo, fieldTo, i,
                                                   reflectionTo.GetRepeatedDouble(protoTo, fieldTo, i) +
                                                   reflectionFrom.GetRepeatedDouble(protoFrom, fieldFrom, i));
                    break;
                case FieldDescriptor::CPPTYPE_FLOAT:
                    reflectionTo.SetRepeatedFloat(&protoTo, fieldTo, i,
                                                   reflectionTo.GetRepeatedFloat(protoTo, fieldTo, i) +
                                                   reflectionFrom.GetRepeatedFloat(protoFrom, fieldFrom, i));
                    break;
                case FieldDescriptor::CPPTYPE_BOOL:
                    reflectionTo.SetRepeatedBool(&protoTo, fieldTo, i, reflectionFrom.GetRepeatedBool(protoFrom, fieldFrom, i));
                    break;
                case FieldDescriptor::CPPTYPE_ENUM:
                    reflectionTo.SetRepeatedEnum(&protoTo, fieldTo, i, reflectionFrom.GetRepeatedEnum(protoFrom, fieldFrom, i));
                    break;
                case FieldDescriptor::CPPTYPE_STRING:
                    reflectionTo.SetRepeatedString(&protoTo, fieldTo, i, reflectionFrom.GetRepeatedString(protoFrom, fieldFrom, i));
                    break;
                case FieldDescriptor::CPPTYPE_MESSAGE:
                    AggregateMessage(*reflectionTo.MutableRepeatedMessage(&protoTo, fieldTo, i), reflectionFrom.GetRepeatedMessage(protoFrom, fieldFrom, i));
                    break;
                }
            }
            for (int i = toSize; i < fromSize; ++i) {
                switch (type) {
                case FieldDescriptor::CPPTYPE_INT32:
                    reflectionTo.AddInt32(&protoTo, fieldTo, reflectionFrom.GetRepeatedInt32(protoFrom, fieldFrom, i));
                    break;
                case FieldDescriptor::CPPTYPE_INT64:
                    reflectionTo.AddInt64(&protoTo, fieldTo, reflectionFrom.GetRepeatedInt64(protoFrom, fieldFrom, i));
                    break;
                case FieldDescriptor::CPPTYPE_UINT32:
                    reflectionTo.AddUInt32(&protoTo, fieldTo, reflectionFrom.GetRepeatedUInt32(protoFrom, fieldFrom, i));
                    break;
                case FieldDescriptor::CPPTYPE_UINT64:
                    reflectionTo.AddUInt64(&protoTo, fieldTo, reflectionFrom.GetRepeatedUInt64(protoFrom, fieldFrom, i));
                    break;
                case FieldDescriptor::CPPTYPE_DOUBLE:
                    reflectionTo.AddDouble(&protoTo, fieldTo, reflectionFrom.GetRepeatedDouble(protoFrom, fieldFrom, i));
                    break;
                case FieldDescriptor::CPPTYPE_FLOAT:
                    reflectionTo.AddFloat(&protoTo, fieldTo, reflectionFrom.GetRepeatedFloat(protoFrom, fieldFrom, i));
                    break;
                case FieldDescriptor::CPPTYPE_BOOL:
                    reflectionTo.AddBool(&protoTo, fieldTo, reflectionFrom.GetRepeatedBool(protoFrom, fieldFrom, i));
                    break;
                case FieldDescriptor::CPPTYPE_ENUM:
                    reflectionTo.AddEnum(&protoTo, fieldTo, reflectionFrom.GetRepeatedEnum(protoFrom, fieldFrom, i));
                    break;
                case FieldDescriptor::CPPTYPE_STRING:
                    reflectionTo.AddString(&protoTo, fieldTo, reflectionFrom.GetRepeatedString(protoFrom, fieldFrom, i));
                    break;
                case FieldDescriptor::CPPTYPE_MESSAGE:
                    reflectionTo.AddMessage(&protoTo, fieldTo)->CopyFrom(reflectionFrom.GetRepeatedMessage(protoFrom, fieldFrom, i));
                    break;
                }
            }
        } else {
            switch (type) {
            case FieldDescriptor::CPPTYPE_INT32:
                if (reflectionTo.HasField(protoTo, fieldTo)) {
                    reflectionTo.SetInt32(&protoTo, fieldTo,
                                          reflectionFrom.GetInt32(protoTo, fieldTo) +
                                          reflectionFrom.GetInt32(protoFrom, fieldFrom));
                } else {
                    reflectionTo.SetInt32(&protoTo, fieldTo,
                                          reflectionFrom.GetInt32(protoFrom, fieldFrom));
                }
                break;
            case FieldDescriptor::CPPTYPE_INT64:
                if (reflectionTo.HasField(protoTo, fieldTo)) {
                    reflectionTo.SetInt64(&protoTo, fieldTo,
                                          reflectionFrom.GetInt64(protoTo, fieldTo) +
                                          reflectionFrom.GetInt64(protoFrom, fieldFrom));
                } else {
                    reflectionTo.SetInt64(&protoTo, fieldTo,
                                          reflectionFrom.GetInt64(protoFrom, fieldFrom));
                }
                break;
            case FieldDescriptor::CPPTYPE_UINT32:
                if (reflectionTo.HasField(protoTo, fieldTo)) {
                    reflectionTo.SetUInt32(&protoTo, fieldTo,
                                           reflectionFrom.GetUInt32(protoTo, fieldTo) +
                                           reflectionFrom.GetUInt32(protoFrom, fieldFrom));
                } else {
                    reflectionTo.SetUInt32(&protoTo, fieldTo,
                                           reflectionFrom.GetUInt32(protoFrom, fieldFrom));
                }
                break;
            case FieldDescriptor::CPPTYPE_UINT64:
                if (reflectionTo.HasField(protoTo, fieldTo)) {
                    reflectionTo.SetUInt64(&protoTo, fieldTo,
                                           reflectionFrom.GetUInt64(protoTo, fieldTo) +
                                           reflectionFrom.GetUInt64(protoFrom, fieldFrom));
                } else {
                    reflectionTo.SetUInt64(&protoTo, fieldTo,
                                           reflectionFrom.GetUInt64(protoFrom, fieldFrom));
                }
                break;
            case FieldDescriptor::CPPTYPE_DOUBLE:
                if (reflectionTo.HasField(protoTo, fieldTo)) {
                    reflectionTo.SetDouble(&protoTo, fieldTo,
                                           reflectionFrom.GetDouble(protoTo, fieldTo) +
                                           reflectionFrom.GetDouble(protoFrom, fieldFrom));
                } else {
                    reflectionTo.SetDouble(&protoTo, fieldTo,
                                           reflectionFrom.GetDouble(protoFrom, fieldFrom));
                }
                break;
            case FieldDescriptor::CPPTYPE_FLOAT:
                if (reflectionTo.HasField(protoTo, fieldTo)) {
                    reflectionTo.SetFloat(&protoTo, fieldTo,
                                          reflectionFrom.GetFloat(protoTo, fieldTo) +
                                          reflectionFrom.GetFloat(protoFrom, fieldFrom));
                } else {
                    reflectionTo.SetFloat(&protoTo, fieldTo,
                                          reflectionFrom.GetFloat(protoFrom, fieldFrom));
                }
                break;
            case FieldDescriptor::CPPTYPE_BOOL:
                reflectionTo.SetBool(&protoTo, fieldTo, reflectionFrom.GetBool(protoFrom, fieldFrom));
                break;
            case FieldDescriptor::CPPTYPE_ENUM:
                reflectionTo.SetEnum(&protoTo, fieldTo, reflectionFrom.GetEnum(protoFrom, fieldFrom));
                break;
            case FieldDescriptor::CPPTYPE_STRING:
                reflectionTo.SetString(&protoTo, fieldTo, reflectionFrom.GetString(protoFrom, fieldFrom));
                break;
            case FieldDescriptor::CPPTYPE_MESSAGE:
                AggregateMessage(*reflectionTo.MutableMessage(&protoTo, fieldTo), reflectionFrom.GetMessage(protoFrom, fieldFrom));
                break;
            }
        }
    }
}

}
