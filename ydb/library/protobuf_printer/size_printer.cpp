#include "size_printer.h"

#include <contrib/libs/protobuf/src/google/protobuf/descriptor.pb.h>

namespace NKikimr {

namespace {

struct FieldIndexSorter {
    bool operator()(const google::protobuf::FieldDescriptor* left, const google::protobuf::FieldDescriptor* right) const {
        if (left->is_extension() && right->is_extension()) {
            return left->number() < right->number();
        } else if (left->is_extension()) {
            return false;
        } else if (right->is_extension()) {
            return true;
        } else {
            return left->index() < right->index();
        }
    }
};

}

TSizeFormatPrinter::TSizeFormatPrinter(google::protobuf::Message& message)
    : Message(message)
{}

TString TSizeFormatPrinter::ToString() const {
    TStringBuilder builder;
    PrintMessage(Message, builder);
    return builder;
}

void TSizeFormatPrinter::PrintMessage(const google::protobuf::Message& message, TStringBuilder& builder) const {
    const google::protobuf::Reflection* reflection = message.GetReflection();
    if (!reflection) {
        return;
    }
    const google::protobuf::Descriptor* descriptor = message.GetDescriptor();
    std::vector<const google::protobuf::FieldDescriptor*> fields;
    if (!descriptor->options().map_entry()) {
        reflection->ListFields(message, &fields);
    }
    std::sort(fields.begin(), fields.end(), FieldIndexSorter());
    for (const google::protobuf::FieldDescriptor* field : fields) {
        PrintField(message, reflection, field, builder);
    }
}

void TSizeFormatPrinter::PrintFieldName(const google::protobuf::FieldDescriptor* field, TStringBuilder& builder) const {
    if (field->is_extension()) {
        builder << "[" << field->PrintableNameForExtension() << "]";
    } else if (field->type() == google::protobuf::FieldDescriptor::TYPE_GROUP) {
        // Groups must be serialized with their original capitalization.
        builder << field->message_type()->name();
    } else {
        builder << field->name();
    }
}

size_t TSizeFormatPrinter::RepeatedByteSizeLong(const google::protobuf::Message& message, const google::protobuf::Reflection* reflection, const google::protobuf::FieldDescriptor* field) const {
    const int fieldSize = reflection->FieldSize(message, field);
    switch (field->cpp_type()) {
        case google::protobuf::FieldDescriptor::CppType::CPPTYPE_FLOAT:
            return fieldSize * sizeof(float);
        case google::protobuf::FieldDescriptor::CppType::CPPTYPE_UINT32:
            return fieldSize * sizeof(uint32_t);
        case google::protobuf::FieldDescriptor::CppType::CPPTYPE_INT32:
            return fieldSize * sizeof(int32_t);
        case google::protobuf::FieldDescriptor::CppType::CPPTYPE_DOUBLE:
            return fieldSize * sizeof(double);
        case google::protobuf::FieldDescriptor::CppType::CPPTYPE_UINT64:
            return fieldSize * sizeof(uint64_t);
        case google::protobuf::FieldDescriptor::CppType::CPPTYPE_INT64:
            return fieldSize * sizeof(int64_t);
        case google::protobuf::FieldDescriptor::CppType::CPPTYPE_STRING: {
            size_t size = 0;
            for (int i = 0; i < fieldSize; i++) {
                size += reflection->GetRepeatedString(message, field, i).size() + 1 /* null terminated */;
            }
            return size;
        }
        case google::protobuf::FieldDescriptor::CppType::CPPTYPE_BOOL:
            return fieldSize * sizeof(bool);
        case google::protobuf::FieldDescriptor::CppType::CPPTYPE_ENUM:
            return fieldSize * sizeof(int);
        case google::protobuf::FieldDescriptor::CppType::CPPTYPE_MESSAGE: {
            size_t size = 0;
            for (int i = 0; i < fieldSize; i++) {
                size += reflection->GetRepeatedMessage(message, field, i).ByteSizeLong();
            }
            return size;
        }
    }
}

size_t TSizeFormatPrinter::ItemByteSizeLong(const google::protobuf::Message& message, const google::protobuf::Reflection* reflection, const google::protobuf::FieldDescriptor* field) const {
    switch (field->cpp_type()) {
        case google::protobuf::FieldDescriptor::CppType::CPPTYPE_FLOAT:
            return sizeof(float);
        case google::protobuf::FieldDescriptor::CppType::CPPTYPE_UINT32:
            return sizeof(uint32_t);
        case google::protobuf::FieldDescriptor::CppType::CPPTYPE_INT32:
            return sizeof(int32_t);
        case google::protobuf::FieldDescriptor::CppType::CPPTYPE_DOUBLE:
            return sizeof(double);
        case google::protobuf::FieldDescriptor::CppType::CPPTYPE_UINT64:
            return sizeof(uint64_t);
        case google::protobuf::FieldDescriptor::CppType::CPPTYPE_INT64:
            return sizeof(int64_t);
        case google::protobuf::FieldDescriptor::CppType::CPPTYPE_STRING:
            return reflection->GetString(message, field).size() + 1 /* null terminated */;
        case google::protobuf::FieldDescriptor::CppType::CPPTYPE_BOOL:
            return sizeof(bool);
        case google::protobuf::FieldDescriptor::CppType::CPPTYPE_ENUM:
            return sizeof(int);
        case google::protobuf::FieldDescriptor::CppType::CPPTYPE_MESSAGE:
            return message.ByteSizeLong();
    }
}

void TSizeFormatPrinter::PrintField(const google::protobuf::Message& message,
                const google::protobuf::Reflection* reflection,
                const google::protobuf::FieldDescriptor* field,
                TStringBuilder& builder) const {
    PrintFieldName(field, builder);
    if (field->is_map()) {
        builder << ": " << RepeatedByteSizeLong(message, reflection, field) << " bytes ";
    } else if (field->is_repeated()) {
        builder << ": " << RepeatedByteSizeLong(message, reflection, field) << " bytes ";
    } else if (field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE) {
        const google::protobuf::Message& subMessage = reflection->GetMessage(message, field);
        builder << " { ";
        PrintMessage(subMessage, builder);
        builder << "} ";
    } else {
        builder << ": " << ItemByteSizeLong(message, reflection, field) << " bytes ";
    }
}
}
