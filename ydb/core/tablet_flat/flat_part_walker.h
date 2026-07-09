#pragma once

#include "flat_part_iface.h"
#include "flat_page_btree_index.h"

namespace NKikimr {
namespace NTable {

class TBTreePartWalker {
public:
    TBTreePartWalker() = default;

    /// Initialise with a V2 B-tree meta (must have HasV2Root()).
    void Start(const NPage::TBtreeIndexMeta& meta)
    {
        Y_ENSURE(meta.HasV2Root(), "TBTreePartWalker requires V2 root");

        LevelCount_ = meta.LevelCount;
        Levels.clear();
        Levels.resize(LevelCount_ + 1);
        Levels[0].push_back(meta.V2Root);
        CurrentLevel_ = 0;
        CurrentPos_ = 0;
    }

    /// One resumable descent step.
    bool Step(const TPart* part, IPages* pages, NPage::TGroupId dataGroupId,
              bool skipDataPages = false)
    {
        for (ui32 level = CurrentLevel_; level < Levels.size(); level++) {
            auto& nodes = Levels[level];
            bool isDataLevel = (LevelCount_ == 0) || (level >= LevelCount_);

            if (isDataLevel && skipDataPages) {
                continue;
            }

            bool anyMissed = false;
            ui32 resumePos = 0;
            for (; CurrentPos_ < nodes.size(); CurrentPos_++) {
                auto& loc = nodes[CurrentPos_];
                if (!loc) continue;

                /// Index pages are always fetched from the main group (room 0).
                auto pageGroupId = isDataLevel ? dataGroupId : NPage::TGroupId{};
                const TSharedData* data = pages->TryGetPage(part, loc, pageGroupId);
                if (data) {
                    if (!isDataLevel) {
                        auto node = NPage::TBtreeIndexNode(*data);
                        bool childrenAreData = (LevelCount_ > 0 && level + 1 >= LevelCount_);
                        if (!(skipDataPages && childrenAreData)) {
                            auto childType = childrenAreData ? NPage::EPage::DataPage : NPage::EPage::BTreeIndexV2;
                            for (NPage::TRecIdx pos : xrange(node.GetChildrenCount())) {
                                Levels[level + 1].push_back(node.GetChildLocationV2(pos, childType));
                            }
                        }
                    }
                    loc = NPage::TPageLocation::Max();
                } else if (!anyMissed) {
                    anyMissed = true;
                    resumePos = CurrentPos_;
                }
            }

            if (anyMissed) {
                CurrentPos_ = resumePos;
                return false;
            }

            // All nodes in this level are resolved — advance past it.
            CurrentLevel_ = level + 1;
            CurrentPos_ = 0;
        }

        // The entire B-tree is resident.
        return true;
    }

private:
    TVector<TVector<NPage::TPageLocation>> Levels;
    ui32 LevelCount_ = 0;
    ui32 CurrentLevel_ = 0;
    ui32 CurrentPos_ = 0;
};

} // namespace NTable
} // namespace NKikimr
