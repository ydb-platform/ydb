#include "storage_defaults.h"

#include <util/generic/yexception.h>
#include <util/string/ascii.h>

#include <algorithm>
#include <array>

namespace NKikimr::NYamlConfig {
    namespace {

        struct TDefaultDiskTypeDescriptor {
            EDefaultDiskType Type;
            TStringBuf Name;
            TStringBuf StoragePoolKind;
            ui64 PDiskCategory;
        };

        struct TFailDomainTypeDescriptor {
            EFailDomainType Type;
            TStringBuf Name;
            TFailDomainGeometryRange Range;
        };

        constexpr ui64 SolidStateFlag = 1;
        constexpr ui64 ExtendedTypeShift = 56;

        // PDisk categories use the same stable encoding as TPDiskCategory.
        constexpr std::array<TDefaultDiskTypeDescriptor, 3> DefaultDiskTypes = {{
            {EDefaultDiskType::Rot, "ROT", "rot", 0},
            {EDefaultDiskType::Ssd, "SSD", "ssd", SolidStateFlag},
            {EDefaultDiskType::Nvme, "NVME", "nvme", (ui64{2} << ExtendedTypeShift) | SolidStateFlag},
        }};

        constexpr std::array<TFailDomainTypeDescriptor, 3> FailDomainTypes = {{
            {EFailDomainType::Rack, "rack", {10, 20, 10, 40}},
            {EFailDomainType::Body, "body", {10, 20, 10, 50}},
            {EFailDomainType::Disk, "disk", {10, 20, 10, 256}},
        }};

        template <class TDescriptor, size_t Size, class TType>
        const TDescriptor& FindDescriptor(const std::array<TDescriptor, Size>& descriptors, TType type) {
            auto it = std::find_if(descriptors.begin(), descriptors.end(), [&](const auto& descriptor) {
                return descriptor.Type == type;
            });
            if (it == descriptors.end()) {
                ythrow yexception() << "Unknown storage default type " << static_cast<int>(type);
            }
            return *it;
        }

    } // anonymous namespace

    std::optional<EDefaultDiskType> TryParseDefaultDiskType(TStringBuf value) {
        auto it = std::find_if(DefaultDiskTypes.begin(), DefaultDiskTypes.end(), [&](const auto& type) {
            return AsciiEqualsIgnoreCase(value, type.Name);
        });
        return it == DefaultDiskTypes.end() ? std::nullopt : std::make_optional(it->Type);
    }

    TStringBuf DefaultDiskTypeName(EDefaultDiskType type) {
        return FindDescriptor(DefaultDiskTypes, type).Name;
    }

    TStringBuf DefaultStoragePoolKind(EDefaultDiskType type) {
        return FindDescriptor(DefaultDiskTypes, type).StoragePoolKind;
    }

    ui64 DefaultPDiskCategory(EDefaultDiskType type) {
        return FindDescriptor(DefaultDiskTypes, type).PDiskCategory;
    }

    std::optional<EFailDomainType> TryParseFailDomainType(TStringBuf value) {
        auto it = std::find_if(FailDomainTypes.begin(), FailDomainTypes.end(), [&](const auto& type) {
            return AsciiEqualsIgnoreCase(value, type.Name);
        });
        return it == FailDomainTypes.end() ? std::nullopt : std::make_optional(it->Type);
    }

    TStringBuf FailDomainTypeName(EFailDomainType type) {
        return FindDescriptor(FailDomainTypes, type).Name;
    }

    TFailDomainGeometryRange GetFailDomainGeometryRange(EFailDomainType type) {
        return FindDescriptor(FailDomainTypes, type).Range;
    }

    std::optional<EFailDomainType> TryInferFailDomainType(const TFailDomainGeometryRange& range) {
        auto it = std::find_if(FailDomainTypes.begin(), FailDomainTypes.end(), [&](const auto& type) {
            return type.Range == range;
        });
        return it == FailDomainTypes.end() ? std::nullopt : std::make_optional(it->Type);
    }

} // namespace NKikimr::NYamlConfig
