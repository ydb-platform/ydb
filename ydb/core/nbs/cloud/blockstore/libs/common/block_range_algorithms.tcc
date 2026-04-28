namespace NYdb::NBS::NBlockStore {

namespace NBlockRangeAlgorithmsInternal {

struct TBoundary
{
    ui64 Offset{};
    ui64 Key{};
    bool Open{};

    bool operator<(const TBoundary& rhs) const;
};

void SplitExecOnNonOverlappingRanges(TVector<TBoundary> boundaries, Cb cb);

}   // namespace NBlockRangeAlgorithmsInternal

// The main goal is recieve template container, create boundaries-edges of
// each overlapping range and call the main algorithm
template<typename TContainerRanges>
void SplitExecOnNonOverlappingRanges(ui64 start, ui64 end, const TContainerRanges& overlappigRanges, Cb cb)
{
    if (overlappigRanges.empty()) {
        return;
    }

    TVector<NBlockRangeAlgorithmsInternal::TBoundary> boundaries;
    boundaries.push_back({start, 0, true});
    boundaries.push_back({end + 1, 0, false});

    for (const auto& item: overlappigRanges) {
        const ui64 clippedStart = Max(item.Range.Start, start);
        const ui64 clippedEnd = Min(item.Range.End, end) + 1;

        boundaries.push_back({clippedStart, item.Key, true});
        boundaries.push_back({clippedEnd, item.Key, false});
    }

    NBlockRangeAlgorithmsInternal::SplitExecOnNonOverlappingRanges(boundaries, std::move(cb));
}


}   // namespace NYdb::NBS::NBlockStore
