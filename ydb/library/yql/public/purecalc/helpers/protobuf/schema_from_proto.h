#pragma once

#include <library/cpp/yson/node/node.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>

#include <google/protobuf/descriptor.h>


namespace NYql {
    namespace NPureCalc {
        enum class EEnumPolicy {
            Int32,
            String,
            YTFlagDefaultInt32,
            YTFlagDefaultString
        };

        enum class EEnumFormatType {
            Int32,
            String
        };

        /**
         * Options that customize building of struct type from protobuf descriptor.
         */
        struct TProtoSchemaOptions {
        public:
            EEnumPolicy EnumPolicy;
            bool ListIsOptional;
            THashMap<TString, TString> FieldRenames;

        public:
            TProtoSchemaOptions();

        public:
            TProtoSchemaOptions& SetEnumPolicy(EEnumPolicy);

            TProtoSchemaOptions& SetListIsOptional(bool);

            TProtoSchemaOptions& SetFieldRenames(
                THashMap<TString, TString> fieldRenames
            );
        };

        EEnumFormatType EnumFormatType(const google::protobuf::FieldDescriptor& enumField, EEnumPolicy enumPolicy);

        /**
         * Build struct type from a protobuf descriptor. The returned yson can be loaded into a struct annotation node
         * using the ParseTypeFromYson function.
         */
        NYT::TNode MakeSchemaFromProto(const google::protobuf::Descriptor&, const TProtoSchemaOptions& = {});

        /**
         * Build variant over tuple type from protobuf descriptors.
         */
        NYT::TNode MakeVariantSchemaFromProtos(const TVector<const google::protobuf::Descriptor*>&, const TProtoSchemaOptions& = {});
    }
}
