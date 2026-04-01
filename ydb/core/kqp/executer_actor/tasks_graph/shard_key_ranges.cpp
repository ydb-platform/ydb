#include "shard_key_ranges.h"

#include <ydb/core/tx/datashard/range_ops.h>

namespace NKikimr::NKqp {

void TShardKeyRanges::AddPoint(TSerializedCellVec&& point) {
    if (!IsFullRange()) {
        Ranges.emplace_back(std::move(point));
    }
}

void TShardKeyRanges::AddRange(TSerializedTableRange&& range) {
    Y_DEBUG_ABORT_UNLESS(!range.Point);
    if (!IsFullRange()) {
        Ranges.emplace_back(std::move(range));
    }
}

void TShardKeyRanges::Add(TSerializedPointOrRange&& pointOrRange) {
    if (!IsFullRange()) {
        Ranges.emplace_back(std::move(pointOrRange));
        if (std::holds_alternative<TSerializedTableRange>(Ranges.back())) {
            Y_DEBUG_ABORT_UNLESS(!std::get<TSerializedTableRange>(Ranges.back()).Point);
        }
    }
}

void TShardKeyRanges::CopyFrom(const TVector<TSerializedPointOrRange>& ranges) {
    if (!IsFullRange()) {
        Ranges = ranges;
        for (auto& x : Ranges) {
            if (std::holds_alternative<TSerializedTableRange>(x)) {
                Y_DEBUG_ABORT_UNLESS(!std::get<TSerializedTableRange>(x).Point);
            }
        }
    }
};

void TShardKeyRanges::MakeFullRange(TSerializedTableRange&& range) {
    Ranges.clear();
    FullRange.emplace(std::move(range));
}

void TShardKeyRanges::MakeFullPoint(TSerializedCellVec&& point) {
    Ranges.clear();
    FullRange.emplace(TSerializedTableRange(std::move(point.GetBuffer()), "", true, true));
    FullRange->Point = true;
}

void TShardKeyRanges::MakeFull(TSerializedPointOrRange&& pointOrRange) {
    if (std::holds_alternative<TSerializedTableRange>(pointOrRange)) {
        MakeFullRange(std::move(std::get<TSerializedTableRange>(pointOrRange)));
    } else {
        MakeFullPoint(std::move(std::get<TSerializedCellVec>(pointOrRange)));
    }
}

void TShardKeyRanges::MergeWritePoints(TShardKeyRanges&& other, const TVector<NScheme::TTypeInfo>& keyTypes) {
    if (IsFullRange()) {
        return;
    }

    if (other.IsFullRange()) {
        std::swap(Ranges, other.Ranges);
        FullRange.swap(other.FullRange);
        return;
    }

    TVector<TSerializedPointOrRange> result;
    result.reserve(Ranges.size() + other.Ranges.size());

    ui64 i = 0, j = 0;
    while (true) {
        if (i >= Ranges.size()) {
            while (j < other.Ranges.size()) {
                result.emplace_back(std::move(other.Ranges[j++]));
            }
            break;
        }
        if (j >= other.Ranges.size()) {
            while (i < Ranges.size()) {
                result.emplace_back(std::move(Ranges[i++]));
            }
            break;
        }

        auto& x = Ranges[i];
        auto& y = other.Ranges[j];

        int cmp = 0;

        // ensure `x` and `y` are points
        YQL_ENSURE(std::holds_alternative<TSerializedCellVec>(x));
        YQL_ENSURE(std::holds_alternative<TSerializedCellVec>(y));

        // common case for multi-effects transactions
        cmp = CompareTypedCellVectors(
            std::get<TSerializedCellVec>(x).GetCells().data(),
            std::get<TSerializedCellVec>(y).GetCells().data(),
            keyTypes.data(), keyTypes.size());

        if (cmp < 0) {
            result.emplace_back(std::move(x));
            ++i;
        } else if (cmp > 0) {
            result.emplace_back(std::move(y));
            ++j;
        } else {
            result.emplace_back(std::move(x));
            ++i;
            ++j;
        }
    }

    Ranges = std::move(result);
}

TString TShardKeyRanges::ToString(const TVector<NScheme::TTypeInfo>& keyTypes, const NScheme::TTypeRegistry& typeRegistry) const {
    TStringBuilder sb;
    sb << "TShardKeyRanges{ ";
    if (IsFullRange()) {
        sb << "full " << DebugPrintRange(keyTypes, FullRange->ToTableRange(), typeRegistry);
    } else {
        if (Ranges.empty()) {
            sb << "<empty> ";
        }
        for (auto& range : Ranges) {
            if (std::holds_alternative<TSerializedCellVec>(range)) {
                sb << DebugPrintPoint(keyTypes, std::get<TSerializedCellVec>(range).GetCells(), typeRegistry) << ", ";
            } else {
                sb << DebugPrintRange(keyTypes, std::get<TSerializedTableRange>(range).ToTableRange(), typeRegistry) << ", ";
            }
        }
    }
    sb << "}";
    return sb;
}

bool TShardKeyRanges::HasRanges() const {
    if (IsFullRange()) {
        return true;
    }
    for (const auto& range : Ranges) {
        if (std::holds_alternative<TSerializedTableRange>(range)) {
            return true;
        }
    }
    return false;
}

void TShardKeyRanges::SerializeTo(NKikimrTxDataShard::TKqpTransaction_TDataTaskMeta_TKeyRange* proto) const {
    if (IsFullRange()) {
        auto& protoRange = *proto->MutableFullRange();
        FullRange->Serialize(protoRange);
    } else {
        auto* protoRanges = proto->MutableRanges();
        for (auto& range : Ranges) {
            if (std::holds_alternative<TSerializedCellVec>(range)) {
                const auto& x = std::get<TSerializedCellVec>(range);
                protoRanges->AddKeyPoints(x.GetBuffer());
            } else {
                auto& x = std::get<TSerializedTableRange>(range);
                Y_DEBUG_ABORT_UNLESS(!x.Point);
                auto& keyRange = *protoRanges->AddKeyRanges();
                x.Serialize(keyRange);
            }
        }
    }
}

void TShardKeyRanges::SerializeTo(NKikimrTxDataShard::TKqpTransaction_TScanTaskMeta_TReadOpMeta* proto) const {
    if (IsFullRange()) {
        auto& protoRange = *proto->AddKeyRanges();
        FullRange->Serialize(protoRange);
    } else {
        for (auto& range : Ranges) {
            auto& keyRange = *proto->AddKeyRanges();
            if (std::holds_alternative<TSerializedTableRange>(range)) {
                auto& x = std::get<TSerializedTableRange>(range);
                Y_DEBUG_ABORT_UNLESS(!x.Point);
                x.Serialize(keyRange);
            } else {
                const auto& x = std::get<TSerializedCellVec>(range);
                keyRange.SetFrom(x.GetBuffer());
                keyRange.SetTo(x.GetBuffer());
                keyRange.SetFromInclusive(true);
                keyRange.SetToInclusive(true);
            }
        }
    }
}

void TShardKeyRanges::SerializeTo(NKikimrTxDataShard::TKqpReadRangesSourceSettings* proto, bool allowPoints) const {
    if (IsFullRange()) {
        auto& protoRange = *proto->MutableRanges()->AddKeyRanges();
        FullRange->Serialize(protoRange);
    } else {
        bool usePoints = allowPoints;
        for (auto& range : Ranges) {
            if (std::holds_alternative<TSerializedTableRange>(range)) {
                usePoints = false;
            }
        }
        auto* protoRanges = proto->MutableRanges();
        for (auto& range : Ranges) {
            if (std::holds_alternative<TSerializedCellVec>(range)) {
                if (usePoints) {
                    const auto& x = std::get<TSerializedCellVec>(range);
                    protoRanges->AddKeyPoints(x.GetBuffer());
                } else {
                    const auto& x = std::get<TSerializedCellVec>(range);
                    auto& keyRange = *protoRanges->AddKeyRanges();
                    keyRange.SetFrom(x.GetBuffer());
                    keyRange.SetTo(x.GetBuffer());
                    keyRange.SetFromInclusive(true);
                    keyRange.SetToInclusive(true);
                }
            } else {
                auto& x = std::get<TSerializedTableRange>(range);
                Y_DEBUG_ABORT_UNLESS(!x.Point);
                auto& keyRange = *protoRanges->AddKeyRanges();
                x.Serialize(keyRange);
            }
        }
    }
}

void TShardKeyRanges::ParseFrom(const NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta::TReadOpMeta& proto) {
    Ranges.reserve(proto.KeyRangesSize());
    for (const auto& range : proto.GetKeyRanges()) {
        Ranges.emplace_back(TSerializedTableRange(range));
    }
}

std::pair<const TSerializedCellVec*, bool> TShardKeyRanges::GetRightBorder() const {
    if (FullRange) {
        return !FullRange->Point ? std::make_pair(&FullRange->To, true) : std::make_pair(&FullRange->From, true);
    }

    YQL_ENSURE(!Ranges.empty());
    const auto& last = Ranges.back();
    if (std::holds_alternative<TSerializedCellVec>(last)) {
        return std::make_pair(&std::get<TSerializedCellVec>(last), true);
    }

    const auto& lastRange = std::get<TSerializedTableRange>(last);
    return !lastRange.Point ? std::make_pair(&lastRange.To, lastRange.ToInclusive) : std::make_pair(&lastRange.From, true);
}

} // namespace NKikimr::NKqp
