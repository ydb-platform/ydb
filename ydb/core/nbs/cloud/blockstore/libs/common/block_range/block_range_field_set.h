#pragma once

#include "block_range.h"
#include "block_range_field_impl.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/memory/arena_allocator_index_pool.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/disable_copy.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

#include <cstddef>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// Memory-optimized interval set for block ranges.
//
// Designed for scenarios with millions of instances:
// - Uses a custom BST backed by a TArenaAllocatorIndexPool that hands out
//   8-byte chunks (a node).
// - Nodes are linked by 16-bit indices instead of pointers.
// - Freed nodes are reused via a free list embedded into the nodes.
//
// Invariant: all ranges stored in the tree are pairwise non-overlapping
// and non-adjacent (they are merged on insertion), so ordering by Start
// is a total order and Start is a unique BST key.
class TBlockRangeFieldSet
    : public TNodeBasedBlockRangeField
    , public TDisableCopyMove
{
public:
    struct TNode;
    using TNodePtr = TNode*;
    enum class EWalk
    {
        ContinueChanged,
        ContinueUnchanged,
        StopChanged,
        StopUnchanged,
        StopOutOfMemory,
    };

    TBlockRangeFieldSet(IArenaAllocatorPtr allocator, size_t maxSizeBytes);

    [[nodiscard]] EBackend GetBackend() const override;

    bool TryAdd(TRange range, bool* changed) override;
    bool TryRemove(TRange range, bool* changed) override;

    void Clear() override;

    [[nodiscard]] bool Overlaps(TRange other) const override;

    void Enumerate(TEnumerateFunc func) const override;

    [[nodiscard]] bool Empty() const override;
    [[nodiscard]] size_t GetBlockCount() const override;
    [[nodiscard]] size_t GetSegmentCount() const override;
    [[nodiscard]] TArenaPoolStats GetMemoryStats() const override;

private:
    ui16 Root;
    ui32 BlockCount = 0;
    TArenaAllocatorIndexPool Pool;

    ui16 AllocNode();
    void FreeNode(ui16 idx);
    [[nodiscard]] TNode* GetNode(ui16 idx);
    [[nodiscard]] const TNode* GetNode(ui16 idx) const;

    EWalk WalkAdd(ui16& parentId, TRange& newRange, TNodePtr& acceptor);
    EWalk WalkRemove(ui16& parentId, TRange range);

    void RemoveNode(ui16& nodeId);
    // Returns true when node has inserted. False when out of memory.
    bool TryInsertNode(TRange range);
    // Detaches the minimum node from the subtree and returns its index.
    // The subtree root reference is updated to keep the tree connected.
    [[nodiscard]] ui16 DetachMin(ui16& nodeId);
    [[nodiscard]] EWalk SplitNode(ui16 parentId, TRange range);

    void InsertNode(ui16 idx);
    void RemoveNodeByKey(ui16 key);
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
