#pragma once
#include <util/string/split.h>
#include <util/string/vector.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>

namespace NKikimr::NViewer {

using namespace NNodeWhiteboard;
using namespace ::google::protobuf;

template<typename ResponseType>
struct TWhiteboardInfo;

template<typename ResponseType>
class TWhiteboardGrouper {
public:
    using TResponseType = typename TWhiteboardInfo<ResponseType>::TResponseType;
    using TElementType = typename TWhiteboardInfo<ResponseType>::TElementType;
    using TElementsFieldType = typename ::google::protobuf::RepeatedPtrField<TElementType>;

    class TFieldProtoValue {
    public:
        TFieldProtoValue(TElementType& element, const FieldDescriptor* field)
            : Element(element)
            , Field(field)
        {}

        bool operator ==(const TFieldProtoValue& value) const {
            Y_ABORT_UNLESS(Field == value.Field);
            const Reflection& reflection = *Element.GetReflection();
            if (!reflection.HasField(Element, Field) || !reflection.HasField(value.Element, value.Field)) {
                return false;
            }
            switch(Field->cpp_type()) {
            case FieldDescriptor::CPPTYPE_INT32:
                return reflection.GetInt32(Element, Field) == reflection.GetInt32(value.Element, value.Field);
            case FieldDescriptor::CPPTYPE_INT64:
                return reflection.GetInt64(Element, Field) == reflection.GetInt64(value.Element, value.Field);
            case FieldDescriptor::CPPTYPE_UINT32:
                return reflection.GetUInt32(Element, Field) == reflection.GetUInt32(value.Element, value.Field);
            case FieldDescriptor::CPPTYPE_UINT64:
                return reflection.GetUInt64(Element, Field) == reflection.GetUInt64(value.Element, value.Field);
            case FieldDescriptor::CPPTYPE_DOUBLE:
                return reflection.GetDouble(Element, Field) == reflection.GetDouble(value.Element, value.Field);
            case FieldDescriptor::CPPTYPE_FLOAT:
                return reflection.GetFloat(Element, Field) == reflection.GetFloat(value.Element, value.Field);
            case FieldDescriptor::CPPTYPE_BOOL:
                return reflection.GetBool(Element, Field) == reflection.GetBool(value.Element, value.Field);
            case FieldDescriptor::CPPTYPE_ENUM:
                return reflection.GetEnum(Element, Field)->number() == reflection.GetEnum(value.Element, value.Field)->number();
            case FieldDescriptor::CPPTYPE_STRING:
                return reflection.GetString(Element, Field) == reflection.GetString(value.Element, value.Field);
            default:
                return false;
            }
        }

        bool operator <(const TFieldProtoValue& value) const {
            Y_ABORT_UNLESS(Field == value.Field);
            const Reflection& reflection = *Element.GetReflection();
            if (!reflection.HasField(Element, Field) || !reflection.HasField(value.Element, value.Field)) {
                return false;
            }
            switch(Field->cpp_type()) {
            case FieldDescriptor::CPPTYPE_INT32:
                return reflection.GetInt32(Element, Field) < reflection.GetInt32(value.Element, value.Field);
            case FieldDescriptor::CPPTYPE_INT64:
                return reflection.GetInt64(Element, Field) < reflection.GetInt64(value.Element, value.Field);
            case FieldDescriptor::CPPTYPE_UINT32:
                return reflection.GetUInt32(Element, Field) < reflection.GetUInt32(value.Element, value.Field);
            case FieldDescriptor::CPPTYPE_UINT64:
                return reflection.GetUInt64(Element, Field) < reflection.GetUInt64(value.Element, value.Field);
            case FieldDescriptor::CPPTYPE_DOUBLE:
                return reflection.GetDouble(Element, Field) < reflection.GetDouble(value.Element, value.Field);
            case FieldDescriptor::CPPTYPE_FLOAT:
                return reflection.GetFloat(Element, Field) < reflection.GetFloat(value.Element, value.Field);
            case FieldDescriptor::CPPTYPE_BOOL:
                return reflection.GetBool(Element, Field) < reflection.GetBool(value.Element, value.Field);
            case FieldDescriptor::CPPTYPE_ENUM:
                return reflection.GetEnum(Element, Field)->number() < reflection.GetEnum(value.Element, value.Field)->number();
            case FieldDescriptor::CPPTYPE_STRING:
                return reflection.GetString(Element, Field) < reflection.GetString(value.Element, value.Field);
            default:
                return false;
            }
        }

        TFieldProtoValue& operator =(const TFieldProtoValue& value) {
            Y_ABORT_UNLESS(Field == value.Field);
            const Reflection& reflection = *Element.GetReflection();
            switch(Field->cpp_type()) {
            case FieldDescriptor::CPPTYPE_INT32:
                reflection.SetInt32(&Element, Field, reflection.GetInt32(value.Element, value.Field));
                break;
            case FieldDescriptor::CPPTYPE_INT64:
                reflection.SetInt64(&Element, Field, reflection.GetInt64(value.Element, value.Field));
                break;
            case FieldDescriptor::CPPTYPE_UINT32:
                reflection.SetUInt32(&Element, Field, reflection.GetUInt32(value.Element, value.Field));
                break;
            case FieldDescriptor::CPPTYPE_UINT64:
                reflection.SetUInt64(&Element, Field, reflection.GetUInt64(value.Element, value.Field));
                break;
            case FieldDescriptor::CPPTYPE_DOUBLE:
                reflection.SetDouble(&Element, Field, reflection.GetDouble(value.Element, value.Field));
                break;
            case FieldDescriptor::CPPTYPE_FLOAT:
                reflection.SetFloat(&Element, Field, reflection.GetFloat(value.Element, value.Field));
                break;
            case FieldDescriptor::CPPTYPE_BOOL:
                reflection.SetBool(&Element, Field, reflection.GetBool(value.Element, value.Field));
                break;
            case FieldDescriptor::CPPTYPE_ENUM:
                reflection.SetEnum(&Element, Field, reflection.GetEnum(value.Element, value.Field));
                break;
            case FieldDescriptor::CPPTYPE_STRING:
                reflection.SetString(&Element, Field, reflection.GetString(value.Element, value.Field));
                break;
            default:
                break;
            }
            return *this;
        }

        TFieldProtoValue& operator =(const EnumValueDescriptor* descriptor) {
            const Reflection& reflection = *Element.GetReflection();
            reflection.SetEnum(&Element, Field, descriptor);
            return *this;
        }

    protected:
        TElementType& Element;
        const FieldDescriptor* Field;
    };

    class TPartProtoKeyEnum {
    public:
        TPartProtoKeyEnum(const TVector<const FieldDescriptor*>& fields)
        {
            for (const FieldDescriptor* field : fields) {
                const EnumDescriptor* enumDescriptor = field->enum_type();
                Fields.push_back(enumDescriptor);
                Values.push_back(enumDescriptor->value(0));
            }
        }

        bool operator ++() {
            for (int p = Fields.size() - 1; p >= 0; --p) {
                int index = Values[p]->index();
                if (++index >= Fields[p]->value_count()) {
                    index = 0;
                }
                Values[p] = Fields[p]->value(index);
                if (index == 0)
                    continue;
                return true;
            }
            return false;
        }

        TVector<const EnumDescriptor*> Fields;
        TVector<const EnumValueDescriptor*> Values;
    };

    class TPartProtoKey {
    public:
        TPartProtoKey(TElementType& element, const TVector<const FieldDescriptor*>& fields)
            : Element(element)
            , Fields(fields)
        {}

        TPartProtoKey(const TPartProtoKey& other)
            : Element(other.Element)
            , Fields(other.Fields)
        {
            *this = other;
        }

        bool operator ==(const TPartProtoKey& other) const {
            Y_ABORT_UNLESS(Fields == other.Fields);
            for (const FieldDescriptor* field : Fields) {
                if (TFieldProtoValue(Element, field) == TFieldProtoValue(other.Element, field)) {
                    continue;
                }
                return false;
            }
            return true;
        }

        bool operator <(const TPartProtoKey& other) const {
            Y_ABORT_UNLESS(Fields == other.Fields);
            for (const FieldDescriptor* field : Fields) {
                if (TFieldProtoValue(Element, field) < TFieldProtoValue(other.Element, field)) {
                    return true;
                }
                if (TFieldProtoValue(Element, field) == TFieldProtoValue(other.Element, field)) {
                    continue;
                }
                return false;
            }
            return false;
        }

        TPartProtoKey& operator =(const TPartProtoKey& other) {
            Y_ABORT_UNLESS(Fields == other.Fields);
            for (const FieldDescriptor* field : Fields) {
                TFieldProtoValue(Element, field) = TFieldProtoValue(other.Element, field);
            }
            return *this;
        }

        TPartProtoKey& operator =(const TPartProtoKeyEnum& other) {
            Y_ABORT_UNLESS(Fields.size() == other.Fields.size());
            for (size_t i = 0; i < Fields.size(); ++i) {
                TFieldProtoValue(Element, Fields[i]) = other.Values[i];
            }
            return *this;
        }

        bool Exists() const {
            const Reflection& reflection = *Element.GetReflection();
            for (const FieldDescriptor* field : Fields) {
                if (!reflection.HasField(Element, field)) {
                    return false;
                }
            }
            return true;
        }

    protected:
        TElementType& Element;
        const TVector<const FieldDescriptor*>& Fields;
    };

    static bool IsEnum(const TVector<const FieldDescriptor*>& fields) {
        for (const FieldDescriptor* field : fields) {
            if (field->cpp_type() != FieldDescriptor::CPPTYPE_ENUM) {
                return false;
            }
        }
        return true;
    }

    static void GroupResponse(TResponseType& source, const TVector<const FieldDescriptor*>& groupFields, bool allEnums = false) {
        ResponseType result;
        TElementsFieldType& field = TWhiteboardInfo<ResponseType>::GetElementsField(result);
        bool allKeys = allEnums && IsEnum(groupFields);
        TMap<TPartProtoKey, ui32> counters;
        TMap<TPartProtoKey, TElementType*> elements;
        if (allKeys) {
            TPartProtoKeyEnum keyEnum(groupFields);
            do {
                auto* element = field.Add();
                TPartProtoKey key(*element, groupFields);
                key = keyEnum;
                element->SetCount(0);
                elements.emplace(key, element);
            } while (++keyEnum);
        }
        auto& sourceField = TWhiteboardInfo<ResponseType>::GetElementsField(source);
        for (TElementType& info : sourceField) {
            TPartProtoKey key(info, groupFields);
            if (key.Exists()) {
                counters[key]++;
            }
        }
        for (const auto& pr : counters) {
            if (pr.second != 0) {
                if (allKeys) {
                    elements[pr.first]->SetCount(pr.second);
                } else {
                    auto* element = field.Add();
                    TPartProtoKey(*element, groupFields) = pr.first;
                    element->SetCount(pr.second);
                }
            }
        }
        result.SetResponseTime(source.GetResponseTime());
        source = std::move(result);
    }

    static TVector<const FieldDescriptor*> GetProtoFields(const TString& fields) {
        const Descriptor& descriptor = *TElementType::descriptor();
        TVector<TString> requestedFields;
        TVector<const FieldDescriptor*> foundFields;
        StringSplitter(fields).Split(',').SkipEmpty().Collect(&requestedFields);
        for (const TString& str : requestedFields) {
            const FieldDescriptor* fieldDescriptor = descriptor.FindFieldByName(str);
            if (fieldDescriptor != nullptr) {
                foundFields.push_back(fieldDescriptor);
            }
        }
        // TODO: replace with error reporting
        //Y_ABORT_UNLESS(requestedFields.size() == foundFields.size());
        return foundFields;
    }
};

template<typename ResponseType>
void GroupWhiteboardResponses(ResponseType& response, const TString& fields, bool allEnums = false) {
    TVector<const FieldDescriptor*> groupFields = TWhiteboardGrouper<ResponseType>::GetProtoFields(fields);
    TWhiteboardGrouper<ResponseType>::GroupResponse(response, groupFields, allEnums);
}

}
