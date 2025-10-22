#include "partition_key_range_sequence.h"

#include <util/generic/maybe.h>
#include <util/generic/vector.h>
#include <util/stream/format.h>
#include <util/string/builder.h>
#include <algorithm>
#include <tuple>

namespace NKikimr::NPQ {
    namespace {
        struct TPoint {
            TMaybe<TStringBuf> Bound;
            bool StartOfInterval;
            ui32 Partition;

            friend std::strong_ordering operator<=>(const TPoint& a, const TPoint& b) {
                return std::forward_as_tuple(a.SignedBound(), a.StartOfInterval) <=> std::forward_as_tuple(b.SignedBound(), b.StartOfInterval);
            }

            std::string BoundToString() const {
                if (!Bound) {
                    return StartOfInterval ? "\"-inf\"" : "\"+inf\"";
                }
                return TStringBuilder() << '"' << HexText(TStringBuf(*Bound)) << '"';
            }

            constexpr std::tuple<int, TStringBuf> SignedBound() const {
                if (Bound) {
                    return {0, *Bound};
                }
                if (StartOfInterval) {
                    return {0, {}};
                } else {
                    return {1, {}};
                }
            }
        };

        TMaybe<TString> ValidateBoundsChainImpl(const std::span<const TPartitionKeyRangeView> bounds) {
            if (bounds.empty()) {
                return "Empty partitions list";
            }

            TVector<TPoint> points(Reserve(bounds.size() * 2));
            for (const auto& set : bounds) {
                TPoint start{
                    .Bound = set.FromBound,
                    .StartOfInterval = true,
                    .Partition = set.PartitionId,
                };
                TPoint end{
                    .Bound = set.ToBound,
                    .StartOfInterval = false,
                    .Partition = set.PartitionId,
                };
                if (auto c = start <=> end; !(c < 0)) {
                    return std::format("Partition {} has invalid bounds range: {}-{}", start.Partition, start.BoundToString(), end.BoundToString());
                }
                points.push_back(std::move(start));
                points.push_back(std::move(end));
            }
            std::sort(points.begin(), points.end());
            if (const auto& first = points.front(); !first.StartOfInterval || first.Bound.GetOrElse({}) != ""sv) {
                return std::format("First patrition {} doesn't have the lowest bound {}", first.Partition, first.BoundToString());
            }
            if (const auto& last = points.back(); last.StartOfInterval || last.Bound.Defined()) {
                return std::format("Last patrition {} doesn't have the highest bound {}", last.Partition, last.BoundToString());
            }
            for (size_t i = 0; i + 1 < points.size(); ++i) {
                const TPoint& prev = points[i + 0];
                const TPoint& next = points[i + 1];
                if (prev.SignedBound() == next.SignedBound()) {
                    if (!prev.StartOfInterval && next.StartOfInterval && prev.Partition != next.Partition) {
                        // ok, adjacent intervals
                    } else {
                        return std::format("Partitions {} and {} have overlapped bounds at point {}", prev.Partition, next.Partition, prev.BoundToString());
                    }
                } else {
                    if (prev.StartOfInterval && !next.StartOfInterval && prev.Partition == next.Partition) {
                        // ok, same partition
                    } else {
                        if (!prev.StartOfInterval && next.StartOfInterval) {
                            return std::format("Partitions {} and {} have a bounds gap {}-{} between them", prev.Partition, next.Partition, prev.BoundToString(), next.BoundToString());
                        } else {
                            return std::format("Partitions {} and {} bounds overlap at the point {}", prev.Partition, next.Partition, next.BoundToString());
                        }
                    }
                }
            }
            return Nothing();
        }
    } // namespace

    std::expected<void, std::string> ValidateKeyRangeSequence(const std::span<const TPartitionKeyRangeView> bounds) {
        auto error = ValidateBoundsChainImpl(bounds);
        if (error.Empty()) {
            return {};
        }
        return std::unexpected(std::move(*error));
    }

} // namespace NKikimr::NPQ
