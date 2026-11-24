#include "partition_key_range.h"

#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/scheme/scheme_borders.h>
#include <ydb/core/scheme/scheme_types_defs.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/services/lib/sharding/sharding.h>

#include <library/cpp/digest/md5/md5.h>

#include <util/generic/algorithm.h>

namespace NKikimr {
namespace NPQ {

TString MiddleOf(const TString& from, const TString& to) {
    AFL_ENSURE(to.empty() || from < to);

    auto GetChar = [](const TString& str, size_t i, unsigned char defaultValue) {
        if (i >= str.size()) {
            return defaultValue;
        }
        return static_cast<unsigned char>(str[i]);
    };

    TStringBuilder result;
    if (from.empty() && to.empty()) {
        result << static_cast<unsigned char>(0x7F);
        return result;
    }

    bool splitted = false;
    bool diffFound = false;

    size_t maxSize = std::max(from.size(), to.size());
    result.reserve(maxSize + 1);

    size_t size = std::max(from.size(), to.size());
    size_t i = 0;
    for (; i < size; ++i) {
        ui16 f = GetChar(from, i, 0);
        ui16 t = GetChar(to, i, 0);

        if (f != t) {
            diffFound = true;
        }

        if (!splitted) {
            if (!diffFound) {
                result << static_cast<unsigned char>(f);
                continue;
            }

            if (f < t) {
                auto m = (f + t) / 2u;
                result << static_cast<unsigned char>(m);
                splitted = m != f;
                continue;
            }
            auto n = (f + t + 0x100u) / 2u;
            if (n < 0x100) {
                result << static_cast<unsigned char>(n);
                splitted = n != f;
                continue;
            }

            for(size_t j = i - 1; j >= 0; --j) {
                result.pop_back();

                ui16 prev = GetChar(from, j, 0);
                if (prev == 0xFFu) {
                    continue;
                }

                result << static_cast<unsigned char>(prev + 1u);
                for (++j; j < i; ++j) {
                    result << static_cast<unsigned char>(0u);
                }

                break;
            }
            result << static_cast<unsigned char>(n - 0x100u);
            splitted = true;
        } else {
            auto n = f < t ? (f + t) / 2u : std::min<ui16>(0xFFu, (f + t + 0x100u) / 2u);
            result << static_cast<unsigned char>(n);
            break;
        }
    }

    if (!splitted) {
        for (; i < from.size() && !splitted; ++i) {
            ui16 f = GetChar(from, i, 0);
            if (f < 0xFF) {
                auto n = (f + 0x100u) / 2u;
                result << static_cast<unsigned char>(n);
                splitted = true;
            } else {
                result << static_cast<unsigned char>(0xFFu);
            }
        }

        for (; i < to.size() && !splitted; ++i) {
            ui16 t = GetChar(to, i, 0);
            auto n = t / 2u;
            result << static_cast<unsigned char>(n);
            splitted = !!n;
        }
    }

    if (result == from) {
        size_t j = maxSize - 1;
        ui16 f = GetChar(from, j, 0);
        ui16 t = GetChar(to, j, to.empty() ? 0xFF : 0x00);

        result << static_cast<unsigned char>((f - t) & 1 ? 0x7Fu : 0xFFu);
    }

    return result;
}

NYql::NDecimal::TUint128 Hash(const TString& sourceId) {
    return NKikimr::NDataStreams::V1::HexBytesToDecimal(MD5::Calc(sourceId));
}

TPartitionKeyRange TPartitionKeyRange::Parse(const NKikimrPQ::TPartitionKeyRange& proto) {
    TPartitionKeyRange result;

    if (proto.HasFromBound()) {
        ParseBound(proto.GetFromBound(), result.FromBound);
    }

    if (proto.HasToBound()) {
        ParseBound(proto.GetToBound(), result.ToBound);
    }

    return result;
}

std::optional<TPartitionKeyRange> TPartitionKeyRange::TryParse(const NKikimrPQ::TPartitionKeyRange& proto) {
    auto tryParseBound = [](const TString& data, TMaybe<TSerializedCellVec>& bound) {
        TSerializedCellVec cells;
        if (!TSerializedCellVec::TryParse(data, cells)) {
            return false;
        }
        bound.ConstructInPlace(std::move(cells));
        return true;
    };

    TPartitionKeyRange result;
    if (proto.HasFromBound() && !tryParseBound(proto.GetFromBound(), result.FromBound)) {
        return std::nullopt;
    }
    if (proto.HasToBound() && !tryParseBound(proto.GetToBound(), result.ToBound)) {
        return std::nullopt;
    }
    return result;
}

void TPartitionKeyRange::Serialize(NKikimrPQ::TPartitionKeyRange& proto) const {
    if (FromBound) {
        proto.SetFromBound(FromBound->GetBuffer());
    }

    if (ToBound) {
        proto.SetToBound(ToBound->GetBuffer());
    }
}

void TPartitionKeyRange::ParseBound(const TString& data, TMaybe<TSerializedCellVec>& bound) {
    TSerializedCellVec cells;
    AFL_ENSURE(TSerializedCellVec::TryParse(data, cells));
    bound.ConstructInPlace(std::move(cells));
}

namespace {

bool IsEntirelyBefore(const TPartitionKeyRange& range, const TPartitionKeyRange& other, TConstArrayRef<NScheme::TTypeInfo> keyTypes) {
    if (!range.ToBound || !other.FromBound) {
        return false;
    }
    return ComparePrefixBorders(keyTypes,
        other.FromBound->GetCells(), PrefixModeLeftBorderInclusive,
        range.ToBound->GetCells(), PrefixModeRightBorderNonInclusive) >= 0;
}

} // anonymous

bool Intersects(const TPartitionKeyRange& lhs, const TPartitionKeyRange& rhs, TConstArrayRef<NScheme::TTypeInfo> keyTypes) {
    return !IsEntirelyBefore(lhs, rhs, keyTypes) && !IsEntirelyBefore(rhs, lhs, keyTypes);
}

TVector<NScheme::TTypeInfo> MakePartitionKeyTypes(const NKikimrPQ::TPQTabletConfig& tabletConfig) {
    TVector<NScheme::TTypeInfo> keyTypes;
    keyTypes.reserve(tabletConfig.PartitionKeySchemaSize());
    for (const auto& component : tabletConfig.GetPartitionKeySchema()) {
        const auto typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(component.GetTypeId(),
            component.HasTypeInfo() ? &component.GetTypeInfo() : nullptr);
        keyTypes.push_back(typeInfoMod.TypeInfo);
    }
    return keyTypes;
}

namespace {

bool IsValidBound(const TMaybe<TSerializedCellVec>& bound, TConstArrayRef<NScheme::TTypeInfo> keyTypes) {
    if (!bound) {
        return true;
    }
    const auto cells = bound->GetCells();
    if (cells.size() > keyTypes.size()) {
        return false;
    }
    for (size_t i = 0; i < cells.size(); ++i) {
        if (NScheme::HasUnexpectedValueSize(cells[i], keyTypes[i])) {
            return false;
        }
    }
    return true;
}

bool IsValidKeyRange(const TPartitionKeyRange& range, TConstArrayRef<NScheme::TTypeInfo> keyTypes) {
    return IsValidBound(range.FromBound, keyTypes) && IsValidBound(range.ToBound, keyTypes);
}

int CompareBounds(const TSerializedCellVec& lhs, const TSerializedCellVec& rhs, TConstArrayRef<NScheme::TTypeInfo> keyTypes) {
    return ComparePrefixBorders(keyTypes,
        lhs.GetCells(), PrefixModeLeftBorderInclusive,
        rhs.GetCells(), PrefixModeLeftBorderInclusive);
}

bool IsFromBoundLess(const TPartitionKeyRange& lhs, const TPartitionKeyRange& rhs, TConstArrayRef<NScheme::TTypeInfo> keyTypes) {
    if (!lhs.FromBound || !rhs.FromBound) {
        return !lhs.FromBound && rhs.FromBound;
    }
    return CompareBounds(*lhs.FromBound, *rhs.FromBound, keyTypes) < 0;
}

bool IsToBoundLess(const TPartitionKeyRange& lhs, const TPartitionKeyRange& rhs, TConstArrayRef<NScheme::TTypeInfo> keyTypes) {
    if (!lhs.ToBound || !rhs.ToBound) {
        return lhs.ToBound && !rhs.ToBound;
    }
    return CompareBounds(*lhs.ToBound, *rhs.ToBound, keyTypes) < 0;
}

bool IsKeyRangeLess(const TPartitionKeyRange& lhs, const TPartitionKeyRange& rhs, TConstArrayRef<NScheme::TTypeInfo> keyTypes) {
    if (IsFromBoundLess(lhs, rhs, keyTypes)) {
        return true;
    }
    if (IsFromBoundLess(rhs, lhs, keyTypes)) {
        return false;
    }
    return IsToBoundLess(lhs, rhs, keyTypes);
}

bool IsStrictlyInside(const TSerializedCellVec& boundary, const TPartitionKeyRange& range, TConstArrayRef<NScheme::TTypeInfo> keyTypes) {
    return (!range.FromBound || CompareBounds(boundary, *range.FromBound, keyTypes) > 0)
        && (!range.ToBound || CompareBounds(boundary, *range.ToBound, keyTypes) < 0);
}

TString MakeFullKey(const TSerializedCellVec& bound, size_t keySize) {
    TVector<TCell> cells(bound.GetCells().begin(), bound.GetCells().end());
    cells.resize(keySize);
    return TSerializedCellVec::Serialize(cells);
}

} // anonymous

std::optional<TString> ChooseSplitBoundary(TConstArrayRef<TWeightedKeyRange> sources, const TPartitionKeyRange& partition,
    TConstArrayRef<NScheme::TTypeInfo> keyTypes)
{
    if (!IsValidKeyRange(partition, keyTypes)) {
        return std::nullopt;
    }

    TVector<const TWeightedKeyRange*> partitionSources;
    ui64 total = 0;
    for (const auto& source : sources) {
        if (source.Weight > 0 && IsValidKeyRange(source.KeyRange, keyTypes) && Intersects(source.KeyRange, partition, keyTypes)) {
            partitionSources.push_back(&source);
            total += source.Weight;
        }
    }

    Sort(partitionSources.begin(), partitionSources.end(), [&](const TWeightedKeyRange* lhs, const TWeightedKeyRange* rhs) {
        return IsKeyRangeLess(lhs->KeyRange, rhs->KeyRange, keyTypes);
    });

    std::optional<TString> boundary;
    ui64 bestImbalance = Max<ui64>();
    ui64 left = 0;
    TMaybe<TSerializedCellVec> leftEnd;
    bool leftUnbounded = false;
    for (size_t i = 0; i < partitionSources.size(); ++i) {
        const auto& range = partitionSources[i]->KeyRange;
        if (i > 0 && !leftUnbounded && range.FromBound
            && CompareBounds(*leftEnd, *range.FromBound, keyTypes) <= 0
            && IsStrictlyInside(*range.FromBound, partition, keyTypes))
        {
            const ui64 right = total - left;
            const ui64 imbalance = left > right ? left - right : right - left;
            if (imbalance < bestImbalance) {
                bestImbalance = imbalance;
                boundary = MakeFullKey(*range.FromBound, keyTypes.size());
            }
        }

        left += partitionSources[i]->Weight;
        if (!range.ToBound) {
            leftUnbounded = true;
        } else if (!leftEnd || CompareBounds(*range.ToBound, *leftEnd, keyTypes) > 0) {
            leftEnd = range.ToBound;
        }
    }

    return boundary;
}

} // namespace NKikimr::NPQ

} // namespace NKikimr
