#include "types_metadata.h"

#include <ydb/core/protos/scheme_type_metadata.pb.h>
#include <ydb/core/scheme_types/scheme_type_metadata.h>

#include <util/generic/vector.h>

namespace NKikimr {

void SerializeMetadata(const NScheme::TTypeMetadataRegistry& typesRegistry, TString* out)
{
    NKikimrSchemeTypeMetadata::TMetadata metadata;
    for (const auto& type: typesRegistry) {
        auto protoType = metadata.AddType();
        protoType->SetId(type.second->GetTypeId());
        protoType->SetName(type.second->GetName());
    }
    Y_PROTOBUF_SUPPRESS_NODISCARD metadata.SerializeToString(out);
}

void DeserializeMetadata(TStringBuf buffer, NScheme::TTypeMetadataRegistry* registry)
{
    using TTypeMetadata = NScheme::TTypeMetadataRegistry::TTypeMetadata;

    NKikimrSchemeTypeMetadata::TMetadata metadata;
    Y_ABORT_UNLESS(metadata.ParseFromArray(buffer.data(), buffer.size()));

    TVector<TTypeMetadata> deserializedMetadata;
    deserializedMetadata.reserve(metadata.TypeSize());
    for (const auto& protoType : metadata.GetType()) {
        TTypeMetadata typeMetadata(
                    protoType.GetId(), protoType.GetName());
        deserializedMetadata.push_back(typeMetadata);
    }

    registry->Clear();
    for (auto& type : deserializedMetadata) {
        registry->Register(&type);
    }
}

} // namespace NKikimr
