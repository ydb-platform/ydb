#include "partition_key_range.h"

#include <ydb/core/protos/pqconfig.pb.h>

namespace NKikimr {
namespace NPQ {

TString MiddleOf(const TString& from, const TString& to) {
    if (from > to && to.size() != 0) {
        return "";
    }

    auto GetChar = [](const TString& str, size_t i, unsigned char defaultValue) {
        if (i >= str.size()) {
            return defaultValue;
        }
        return static_cast<unsigned char>(str[i]);
    };

    auto GetMiddle = [](ui16 a, ui16 b) {
        if (a == 0xFF) {
            return std::pair<ui16, bool>{0xFF, false};
        }
        if (a + 1 < b) {
            return std::pair<ui16, bool>{(a + b) / 2, true};
        }
        if (b < a) {
            ui16 n = (a + b + 0x100) / 2;
            return std::pair<ui16, bool>{(n < 0x100) ? n : 0xFF, true};
        }

        return std::pair<ui16, bool>{a, false};
    };


    TStringBuilder result;
    if (from.empty() && to.empty()) {
        result << static_cast<unsigned char>(0x7F);
        return result;
    }

    bool splitted = false;

    size_t maxSize = std::max(from.size(), to.size());
    for (size_t i = 0; i < maxSize; ++i) {
        ui16 f = GetChar(from, i, 0);
        ui16 t = GetChar(to, i, 0xFF);

        if (!splitted) {
            auto [n, s] = GetMiddle(f, t);
            result << static_cast<unsigned char>(n);
            splitted = s;
        } else {
            auto n = (f + t) / 2u;
            result << static_cast<unsigned char>(n);
            break;
        }
    }

    if (result == from) {
        result << static_cast<unsigned char>(0xFFu);
    }

    return result;
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
    Y_ABORT_UNLESS(TSerializedCellVec::TryParse(data, cells));
    bound.ConstructInPlace(std::move(cells));
}

} // NPQ
} // NKikimr
