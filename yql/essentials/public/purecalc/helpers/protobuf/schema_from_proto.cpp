#include "schema_from_proto.h"

#include <yt/yt_proto/yt/formats/extension.pb.h>

#include <util/generic/algorithm.h>
#include <util/generic/string.h>
#include <util/string/printf.h>
#include <util/string/vector.h>

namespace pb = google::protobuf;

namespace NYql {
    namespace NPureCalc {

        TProtoSchemaOptions::TProtoSchemaOptions()
            : EnumPolicy(EEnumPolicy::Int32)
            , ListIsOptional(false)
        {
        }

        TProtoSchemaOptions& TProtoSchemaOptions::SetEnumPolicy(EEnumPolicy policy) {
            EnumPolicy = policy;
            return *this;
        }

        TProtoSchemaOptions& TProtoSchemaOptions::SetListIsOptional(bool value) {
            ListIsOptional = value;
            return *this;
        }

        TProtoSchemaOptions& TProtoSchemaOptions::SetFieldRenames(
            THashMap<TString, TString> fieldRenames
        ) {
            FieldRenames = std::move(fieldRenames);
            return *this;
        }

        namespace {
            EEnumFormatType EnumFormatTypeWithYTFlag(const pb::FieldDescriptor& enumField, EEnumFormatType defaultEnumFormatType) {
                auto flags = enumField.options().GetRepeatedExtension(NYT::flags);
                for (auto flag : flags) {
                    if (flag == NYT::EWrapperFieldFlag::ENUM_INT) {
                        return EEnumFormatType::Int32;
                    } else if (flag == NYT::EWrapperFieldFlag::ENUM_STRING) {
                        return EEnumFormatType::String;
                    }
                }
                return defaultEnumFormatType;
            }
        }

        EEnumFormatType EnumFormatType(const pb::FieldDescriptor& enumField, EEnumPolicy enumPolicy) {
            switch (enumPolicy) {
                case EEnumPolicy::Int32:
                    return EEnumFormatType::Int32;
                case EEnumPolicy::String:
                    return EEnumFormatType::String;
                case EEnumPolicy::YTFlagDefaultInt32:
                    return EnumFormatTypeWithYTFlag(enumField, EEnumFormatType::Int32);
                case EEnumPolicy::YTFlagDefaultString:
                    return EnumFormatTypeWithYTFlag(enumField, EEnumFormatType::String);
            }
        }

        namespace {
            const char* FormatTypeName(const pb::FieldDescriptor* field, EEnumPolicy enumPolicy) {
                switch (field->type()) {
                    case pb::FieldDescriptor::TYPE_DOUBLE:
                        return "Double";
                    case pb::FieldDescriptor::TYPE_FLOAT:
                        return "Float";
                    case pb::FieldDescriptor::TYPE_INT64:
                    case pb::FieldDescriptor::TYPE_SFIXED64:
                    case pb::FieldDescriptor::TYPE_SINT64:
                        return "Int64";
                    case pb::FieldDescriptor::TYPE_UINT64:
                    case pb::FieldDescriptor::TYPE_FIXED64:
                        return "Uint64";
                    case pb::FieldDescriptor::TYPE_INT32:
                    case pb::FieldDescriptor::TYPE_SFIXED32:
                    case pb::FieldDescriptor::TYPE_SINT32:
                        return "Int32";
                    case pb::FieldDescriptor::TYPE_UINT32:
                    case pb::FieldDescriptor::TYPE_FIXED32:
                        return "Uint32";
                    case pb::FieldDescriptor::TYPE_BOOL:
                        return "Bool";
                    case pb::FieldDescriptor::TYPE_STRING:
                        return "Utf8";
                    case pb::FieldDescriptor::TYPE_BYTES:
                        return "String";
                    case pb::FieldDescriptor::TYPE_ENUM:
                        switch (EnumFormatType(*field, enumPolicy)) {
                            case EEnumFormatType::Int32:
                                return "Int32";
                            case EEnumFormatType::String:
                                return "String";
                        }
                    default:
                        ythrow yexception() << "Unsupported protobuf type: " << field->type_name()
                                            << ", field: " << field->name() << ", " << int(field->type());
                }
            }
        }

        NYT::TNode MakeSchemaFromProto(const pb::Descriptor& descriptor, TVector<const pb::Descriptor*>& nested, const TProtoSchemaOptions& options) {
            if (Find(nested, &descriptor) != nested.end()) {
                TVector<TString> nestedNames;
                for (const auto* d : nested) {
                    nestedNames.push_back(d->full_name());
                }
                nestedNames.push_back(descriptor.full_name());
                ythrow yexception() << Sprintf("recursive messages are not supported (%s)",
                        JoinStrings(nestedNames, "->").c_str());
            }
            nested.push_back(&descriptor);

            auto items = NYT::TNode::CreateList();
            for (int fieldNo = 0; fieldNo < descriptor.field_count(); ++fieldNo) {
                const auto& fieldDescriptor = *descriptor.field(fieldNo);

                auto name = fieldDescriptor.name();
                if (
                    auto renamePtr = options.FieldRenames.FindPtr(name);
                    nested.size() == 1 && renamePtr
                ) {
                    name = *renamePtr;
                }

                NYT::TNode itemType;
                if (fieldDescriptor.type() == pb::FieldDescriptor::TYPE_MESSAGE) {
                    itemType = MakeSchemaFromProto(*fieldDescriptor.message_type(), nested, options);
                } else {
                    itemType = NYT::TNode::CreateList();
                    itemType.Add("DataType");
                    itemType.Add(FormatTypeName(&fieldDescriptor, options.EnumPolicy));
                }
                switch (fieldDescriptor.label()) {
                    case pb::FieldDescriptor::LABEL_OPTIONAL:
                        {
                            auto optionalType = NYT::TNode::CreateList();
                            optionalType.Add("OptionalType");
                            optionalType.Add(std::move(itemType));
                            itemType = std::move(optionalType);
                        }
                        break;
                    case pb::FieldDescriptor::LABEL_REQUIRED:
                        break;
                    case pb::FieldDescriptor::LABEL_REPEATED:
                        {
                            auto listType = NYT::TNode::CreateList();
                            listType.Add("ListType");
                            listType.Add(std::move(itemType));
                            itemType = std::move(listType);
                            if (options.ListIsOptional) {
                                itemType = NYT::TNode::CreateList().Add("OptionalType").Add(std::move(itemType));
                            }
                        }
                        break;
                    default:
                        ythrow yexception() << "Unknown protobuf label: " << (ui32)fieldDescriptor.label() << ", field: " << name;
                }

                auto itemNode = NYT::TNode::CreateList();
                itemNode.Add(name);
                itemNode.Add(std::move(itemType));

                items.Add(std::move(itemNode));
            }
            auto root = NYT::TNode::CreateList();
            root.Add("StructType");
            root.Add(std::move(items));

            nested.pop_back();
            return root;
        }

        NYT::TNode MakeSchemaFromProto(const pb::Descriptor& descriptor, const TProtoSchemaOptions& options) {
            TVector<const pb::Descriptor*> nested;
            return MakeSchemaFromProto(descriptor, nested, options);
        }

        NYT::TNode MakeVariantSchemaFromProtos(const TVector<const pb::Descriptor*>& descriptors, const TProtoSchemaOptions& options) {
            Y_ENSURE(options.FieldRenames.empty(), "Renames are not supported in variant mode");

            auto tupleItems = NYT::TNode::CreateList();
            for (auto descriptor : descriptors) {
                tupleItems.Add(MakeSchemaFromProto(*descriptor, options));
            }

            auto tupleType = NYT::TNode::CreateList();
            tupleType.Add("TupleType");
            tupleType.Add(std::move(tupleItems));

            auto variantType = NYT::TNode::CreateList();
            variantType.Add("VariantType");
            variantType.Add(std::move(tupleType));

            return variantType;
        }
    }
}
