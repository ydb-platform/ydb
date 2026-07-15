#pragma once

#include <util/generic/strbuf.h>
#include <util/system/types.h>

#include <optional>

namespace NKikimr::NYamlConfig {

    enum class EDefaultDiskType {
        Rot,
        Ssd,
        Nvme,
    };

    std::optional<EDefaultDiskType> TryParseDefaultDiskType(TStringBuf value);
    TStringBuf DefaultDiskTypeName(EDefaultDiskType type);
    TStringBuf DefaultStoragePoolKind(EDefaultDiskType type);
    ui64 DefaultPDiskCategory(EDefaultDiskType type);

    enum class EFailDomainType {
        Rack,
        Body,
        Disk,
    };

    struct TFailDomainGeometryRange {
        ui32 RealmLevelBegin;
        ui32 RealmLevelEnd;
        ui32 DomainLevelBegin;
        ui32 DomainLevelEnd;

        bool operator==(const TFailDomainGeometryRange&) const = default;
    };

    std::optional<EFailDomainType> TryParseFailDomainType(TStringBuf value);
    TStringBuf FailDomainTypeName(EFailDomainType type);
    TFailDomainGeometryRange GetFailDomainGeometryRange(EFailDomainType type);
    std::optional<EFailDomainType> TryInferFailDomainType(const TFailDomainGeometryRange& range);

} // namespace NKikimr::NYamlConfig
