#include "partition_key_range.h"

#include <ydb/core/protos/pqconfig.pb.h>

namespace NKikimr {
namespace NPQ {

TString MiddleOf(const TString& from, const TString& to) {
    Y_ABORT_UNLESS(to.empty() || from < to);

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
