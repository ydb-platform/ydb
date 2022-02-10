#include "scheme.h"

#include <util/generic/vector.h>
#include <util/generic/yexception.h>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <google/protobuf/reflection.h>

using namespace google::protobuf;

namespace NSc {
    TValue TValue::From(const Message& msg, bool mapAsDict) {
        TValue v;
        const Reflection* r = msg.GetReflection();
        TVector<const FieldDescriptor*> fields;
        TVector<const FieldDescriptor*>::iterator it;
        int i1;

        r->ListFields(msg, &fields);
        for (it = fields.begin(), i1 = 0; it != fields.end(); ++it, ++i1) {
            const FieldDescriptor* field = *it;
            try {
                if (field->is_repeated()) {
                    if (field->is_map() && mapAsDict) {
                        auto& elem = v[field->name()];
                        for (int i2 = 0; i2 < r->FieldSize(msg, field); ++i2) {
                            auto val = FromRepeatedField(msg, field, i2);
                            if (val.IsDict()) {
                                elem[TStringBuf(val["key"])] = val["value"];
                            }
                        }
                    } else {
                        for (int i2 = 0; i2 < r->FieldSize(msg, field); ++i2)
                            v[field->name()][i2] = FromRepeatedField(msg, field, i2);
                    }
                } else {
                    v[field->name()] = FromField(msg, field);
                }
            } catch (...) {
                /* conversion failed, skip this field */
            }
        }

        return v;
    }

    TValue TValue::FromField(const Message& msg, const FieldDescriptor* field) {
        TValue v;
        const Reflection* r = msg.GetReflection();

        switch (field->cpp_type()) {
            case FieldDescriptor::CPPTYPE_INT32:
                v = r->GetInt32(msg, field);
                break;
            case FieldDescriptor::CPPTYPE_INT64:
                v = r->GetInt64(msg, field);
                break;
            case FieldDescriptor::CPPTYPE_UINT32:
                v = r->GetUInt32(msg, field);
                break;
            case FieldDescriptor::CPPTYPE_UINT64:
                v = r->GetUInt64(msg, field);
                break;
            case FieldDescriptor::CPPTYPE_DOUBLE:
                v = r->GetDouble(msg, field);
                break;
            case FieldDescriptor::CPPTYPE_FLOAT:
                v = r->GetFloat(msg, field);
                break;
            case FieldDescriptor::CPPTYPE_BOOL:
                v.SetBool(r->GetBool(msg, field));
                break;
            case FieldDescriptor::CPPTYPE_ENUM:
                v = r->GetEnum(msg, field)->name();
                break;
            case FieldDescriptor::CPPTYPE_STRING:
                v = r->GetString(msg, field);
                break;
            case FieldDescriptor::CPPTYPE_MESSAGE:
                v = From(r->GetMessage(msg, field));
                break;
            default:
                ythrow TSchemeException() << "field " << field->full_name() << " unexpected type " << (int)field->cpp_type();
        }

        return v;
    }

    TValue TValue::FromRepeatedField(const Message& msg, const FieldDescriptor* field, int index) {
        TValue v;
        const Reflection* r = msg.GetReflection();

        switch (field->cpp_type()) {
            case FieldDescriptor::CPPTYPE_INT32:
                v = r->GetRepeatedInt32(msg, field, index);
                break;
            case FieldDescriptor::CPPTYPE_INT64:
                v = r->GetRepeatedInt64(msg, field, index);
                break;
            case FieldDescriptor::CPPTYPE_UINT32:
                v = r->GetRepeatedUInt32(msg, field, index);
                break;
            case FieldDescriptor::CPPTYPE_UINT64:
                v = r->GetRepeatedUInt64(msg, field, index);
                break;
            case FieldDescriptor::CPPTYPE_DOUBLE:
                v = r->GetRepeatedDouble(msg, field, index);
                break;
            case FieldDescriptor::CPPTYPE_FLOAT:
                v = r->GetRepeatedFloat(msg, field, index);
                break;
            case FieldDescriptor::CPPTYPE_BOOL:
                v.SetBool(r->GetRepeatedBool(msg, field, index));
                break;
            case FieldDescriptor::CPPTYPE_ENUM:
                v = r->GetRepeatedEnum(msg, field, index)->name();
                break;
            case FieldDescriptor::CPPTYPE_STRING:
                v = r->GetRepeatedString(msg, field, index);
                break;
            case FieldDescriptor::CPPTYPE_MESSAGE:
                v = From(r->GetRepeatedMessage(msg, field, index));
                break;
            default:
                ythrow TSchemeException() << "field " << field->full_name() << " unexpected type " << (int)field->cpp_type();
        }

        return v;
    }

    void TValue::To(Message& msg, const TProtoOpts& opts) const {
        msg.Clear();

        if (IsNull()) {
            return;
        }

        if (!IsDict()) {
            ythrow TSchemeException() << "expected dictionary";
        }

        const Descriptor* descriptor = msg.GetDescriptor();
        for (int i = 0, count = descriptor->field_count(); i < count; ++i) {
            const FieldDescriptor* field = descriptor->field(i);
            if (field->is_map()) {
                ToMapField(msg, field, opts);
            } else if (field->is_repeated()) {
                ToRepeatedField(msg, field, opts);
            } else {
                ToField(msg, field, opts);
            }
        }
    }

    void TValue::ValueToField(const TValue& value, Message& msg, const FieldDescriptor* field, const TProtoOpts& opts) const {
        const TString& name = field->name();
        if (value.IsNull()) {
            if (field->is_required() && !field->has_default_value()) {
                ythrow TSchemeException() << "has no value for required field " << name;
            }
            return;
        }

        const Reflection* reflection = msg.GetReflection();

        switch (field->cpp_type()) {
            case FieldDescriptor::CPPTYPE_INT32:
                reflection->SetInt32(&msg, field, value.ForceIntNumber());
                break;
            case FieldDescriptor::CPPTYPE_INT64:
                reflection->SetInt64(&msg, field, value.ForceIntNumber());
                break;
            case FieldDescriptor::CPPTYPE_UINT32:
                reflection->SetUInt32(&msg, field, value.ForceIntNumber());
                break;
            case FieldDescriptor::CPPTYPE_UINT64:
                reflection->SetUInt64(&msg, field, value.ForceIntNumber());
                break;
            case FieldDescriptor::CPPTYPE_DOUBLE:
                reflection->SetDouble(&msg, field, value.ForceNumber());
                break;
            case FieldDescriptor::CPPTYPE_FLOAT:
                reflection->SetFloat(&msg, field, value.ForceNumber());
                break;
            case FieldDescriptor::CPPTYPE_BOOL:
                reflection->SetBool(&msg, field, value.IsTrue());
                break;
            case FieldDescriptor::CPPTYPE_STRING:
                reflection->SetString(&msg, field, value.ForceString());
                break;
            case FieldDescriptor::CPPTYPE_ENUM:
                value.ToEnumField(msg, field, opts);
                break;
            case FieldDescriptor::CPPTYPE_MESSAGE:
                value.To(*reflection->MutableMessage(&msg, field), opts);
                break;
            default:
                ythrow TSchemeException()
                    << "field " << field->full_name()
                    << " unexpected type " << (int)field->cpp_type();
        }
    }

    void TValue::ToField(Message& msg, const FieldDescriptor* field, const TProtoOpts& opts) const {
        const TString& name = field->name();
        const TValue& value = Get(name);
        ValueToField(value, msg, field, opts);
    }

    void TValue::ToEnumField(Message& msg, const FieldDescriptor* field, const TProtoOpts& opts) const {
        const EnumDescriptor* enumField = field->enum_type();

        const EnumValueDescriptor* enumFieldValue = IsString()
                                                        ? enumField->FindValueByName(ForceString())
                                                        : enumField->FindValueByNumber(ForceIntNumber());

        if (!enumFieldValue) {
            if (opts.UnknownEnumValueIsDefault) {
                enumFieldValue = field->default_value_enum();
            } else {
                ythrow TSchemeException() << "invalid value of enum field " << field->name();
            }
        }

        const Reflection* reflection = msg.GetReflection();

        if (field->is_repeated()) {
            reflection->AddEnum(&msg, field, enumFieldValue);
        } else {
            reflection->SetEnum(&msg, field, enumFieldValue);
        }
    }

    void TValue::ToRepeatedField(Message& msg, const FieldDescriptor* field, const TProtoOpts& opts) const {
        const TString& name = field->name();

        const TValue& fieldValue = Get(name);
        if (fieldValue.IsNull()) {
            return;
        }

        if (!fieldValue.IsArray()) {
            if (opts.SkipTypeMismatch) {
                return; // leave repeated field empty
            } else {
                ythrow TSchemeException() << "invalid type of repeated field " << name << ": not an array";
            }
        }

        const Reflection* reflection = msg.GetReflection();

        for (const TValue& value : fieldValue.GetArray()) {
            switch (field->cpp_type()) {
                case FieldDescriptor::CPPTYPE_INT32:
                    reflection->AddInt32(&msg, field, value.ForceIntNumber());
                    break;
                case FieldDescriptor::CPPTYPE_INT64:
                    reflection->AddInt64(&msg, field, value.ForceIntNumber());
                    break;
                case FieldDescriptor::CPPTYPE_UINT32:
                    reflection->AddUInt32(&msg, field, value.ForceIntNumber());
                    break;
                case FieldDescriptor::CPPTYPE_UINT64:
                    reflection->AddUInt64(&msg, field, value.ForceIntNumber());
                    break;
                case FieldDescriptor::CPPTYPE_DOUBLE:
                    reflection->AddDouble(&msg, field, value.ForceNumber());
                    break;
                case FieldDescriptor::CPPTYPE_FLOAT:
                    reflection->AddFloat(&msg, field, value.ForceNumber());
                    break;
                case FieldDescriptor::CPPTYPE_BOOL:
                    reflection->AddBool(&msg, field, value.IsTrue());
                    break;
                case FieldDescriptor::CPPTYPE_STRING:
                    reflection->AddString(&msg, field, value.ForceString());
                    break;
                case FieldDescriptor::CPPTYPE_ENUM:
                    value.ToEnumField(msg, field, opts);
                    break;
                case FieldDescriptor::CPPTYPE_MESSAGE:
                    value.To(*reflection->AddMessage(&msg, field));
                    break;
                default:
                    ythrow TSchemeException()
                        << "field " << field->full_name()
                        << " unexpected type " << (int)field->cpp_type();
            }
        }
    }

    void TValue::ToMapField(Message& msg, const FieldDescriptor* field, const TProtoOpts& opts) const {
        const TString& name = field->name();

        const TValue& fieldValue = Get(name);
        if (fieldValue.IsNull()) {
            return;
        }

        if (fieldValue.IsArray()) {
            // read dict from key, value array
            ToRepeatedField(msg, field, opts);
            return;
        }

        if (!fieldValue.IsDict()) {
            if (opts.SkipTypeMismatch) {
                return; // leave map field empty
            } else {
                ythrow TSchemeException() << "invalid type of map field " << name << ": not dict or array";
            }
        }

        const Reflection* reflection = msg.GetReflection();

        auto mutableField = reflection->GetMutableRepeatedFieldRef<Message>(&msg, field);
        for (const auto& value : fieldValue.GetDict()) {
            THolder<Message> entry(mutableField.NewMessage());
            auto entryDesc = entry->GetDescriptor();
            auto keyField = entryDesc->FindFieldByNumber(1);
            auto valueField = entryDesc->FindFieldByNumber(2);
            auto entryReflection = entry->GetReflection();
            entryReflection->SetString(entry.Get(), keyField, TString(value.first));
            ValueToField(value.second, *entry, valueField, opts);
            mutableField.Add(*entry);
        }
    }
}
