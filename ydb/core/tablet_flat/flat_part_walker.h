#pragma once

#include "flat_part_iface.h"
#include "flat_page_btree_index.h"

namespace NKikimr {
namespace NTable {

// B-Tree Index V2 specific
class TBTreePartWalker {
public:
    TBTreePartWalker() = default;

    /// Initialise with a V2 B-tree meta (must have HasV2Root()).
    void Start(const NPage::TBtreeIndexMeta& meta)
    {
        Y_ENSURE(meta.HasRootV2(), "TBTreePartWalker requires V2 root");

        LevelCount_ = meta.LevelCount();
        Levels.clear();
        Levels.resize(LevelCount_ + 1);
        Levels[0].push_back(meta.RootV2);
        CurrentLevel_ = 0;
    }

    /// One resumable descent step.
    bool Step(const TPart* part, IPages* pages, NPage::TGroupId dataGroupId,
              bool skipDataPages = false)
    {
        for (ui32 level = CurrentLevel_; level < Levels.size(); level++) {
            auto& nodes = Levels[level];
            const bool isDataLevel = (LevelCount_ == 0) || (level >= LevelCount_);

            if (isDataLevel && skipDataPages) {
                Levels[level] = { };
                return true; // data level always last, walker is done
            }

            /// Index pages are always fetched from the main group (room 0).
            const auto pageGroupId = isDataLevel ? dataGroupId : NPage::TGroupId{};

            bool anyMissed = false;
            size_t keep = 0;
            for (size_t pos = 0; pos < nodes.size(); ++pos) {
                const auto loc = nodes[pos];
                const TSharedData* data = pages->TryGetPage(part, loc, pageGroupId);
                if (!data) {
                    anyMissed = true;
                    nodes[keep++] = loc; // the page is not here yet, keep it for the next round
                    continue;
                }

                if (!isDataLevel) {
                    auto node = NPage::TBtreeIndexNode(*data, /*v2Format=*/true);
                    const bool childrenAreData = (LevelCount_ > 0 && level + 1 >= LevelCount_);
                    if (!(skipDataPages && childrenAreData)) {
                        for (NPage::TRecIdx childPos : xrange(node.GetChildrenCount())) {
                            Levels[level + 1].push_back(std::get<NPage::TPageLocation>(node.GetChild(childPos, childrenAreData)));
                        }
                    }
                }
            }
            nodes.resize(keep); // resolved levels are dropped, only the misses stay

            if (anyMissed) {
                return false;
            }

            // The whole level is resolved — advance past it, its locations are not needed any more.
            Levels[level] = { };
            CurrentLevel_ = level + 1;
        }

        // The entire B-tree is resident.
        return true;
    }

private:
    TVector<TVector<NPage::TPageLocation>> Levels;
    ui32 LevelCount_ = 0;
    ui32 CurrentLevel_ = 0;
};

} // namespace NTable
} // namespace NKikimr
