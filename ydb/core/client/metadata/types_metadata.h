#pragma once

#include <util/generic/fwd.h>


namespace NKikimr {
namespace NScheme {
    class TTypeMetadataRegistry;
} // namespace NScheme

void SerializeMetadata(const NScheme::TTypeMetadataRegistry& typesRegistry, TString* out);
void DeserializeMetadata(TStringBuf buffer, NScheme::TTypeMetadataRegistry* registry);

} // namespace NKikimr
