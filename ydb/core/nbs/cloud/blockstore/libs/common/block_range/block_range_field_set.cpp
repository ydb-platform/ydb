#include "block_range_field_set.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/memory/arena_allocator.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

#include <new>

namespace NYdb::NBS::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui16 NodeNullIndex = 0xffff;
constexpr size_t NodeSize = 8;
constexpr size_t SlotSize = 512;

////////////////////////////////////////////////////////////////////////////////

bool HasChanged(TBlockRangeFieldSet::EWalk walk)
{
    switch (walk) {
        case TBlockRangeFieldSet::EWalk::ContinueChanged:
        case TBlockRangeFieldSet::EWalk::StopChanged:
            return true;
        default:
            return false;
    }
}

bool HasOutOfMemory(TBlockRangeFieldSet::EWalk walk)
{
    switch (walk) {
        case TBlockRangeFieldSet::EWalk::StopOutOfMemory:
            return true;
        default:
            return false;
    }
}

bool HasStop(TBlockRangeFieldSet::EWalk walk)
{
    switch (walk) {
        case TBlockRangeFieldSet::EWalk::StopChanged:
        case TBlockRangeFieldSet::EWalk::StopUnchanged:
        case TBlockRangeFieldSet::EWalk::StopOutOfMemory:
            return true;
        default:
            return false;
    }
}

TBlockRangeFieldSet::EWalk MixChanged(
    TBlockRangeFieldSet::EWalk walk,
    bool changed)
{
    if (!changed) {
        return walk;
    }
    switch (walk) {
        case TBlockRangeFieldSet::EWalk::ContinueChanged:
        case TBlockRangeFieldSet::EWalk::StopChanged:
        case TBlockRangeFieldSet::EWalk::StopOutOfMemory:
            return walk;
        case TBlockRangeFieldSet::EWalk::StopUnchanged:
            return TBlockRangeFieldSet::EWalk::StopChanged;
        case TBlockRangeFieldSet::EWalk::ContinueUnchanged:
            return TBlockRangeFieldSet::EWalk::ContinueChanged;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////
struct TBlockRangeFieldSet::TNode
{
    TBlockRange<ui16> Range;
    ui16 Left = NodeNullIndex;
    ui16 Right = NodeNullIndex;

    void Init()
    {
        Range.Start = 0;
        Range.End = 0;
        Left = NodeNullIndex;
        Right = NodeNullIndex;
    }
};

static_assert(NodeSize == sizeof(TBlockRangeFieldSet::TNode));

////////////////////////////////////////////////////////////////////////////////

TBlockRangeFieldSet::TBlockRangeFieldSet(
    IArenaAllocatorPtr allocator,
    size_t maxSizeBytes)
    : Root(NodeNullIndex)
    , Pool(allocator, SlotSize, maxSizeBytes, NodeSize)
{}

IBlockRangeFieldImpl::EBackend TBlockRangeFieldSet::GetBackend() const
{
    return EBackend::Set;
}

bool TBlockRangeFieldSet::TryAdd(TRange range, bool* changed)
{
    *changed = false;
    if (Root == NodeNullIndex) {
        // Empty tree: plain insertion.
        const ui16 idx = AllocNode();
        if (idx == NodeNullIndex) {
            return false;
        }
        GetNode(idx)->Range = range;
        Root = idx;
        BlockCount += range.Size();
        *changed = true;
        return true;
    }

    TNode* acceptor = nullptr;
    auto walk = WalkAdd(Root, range, acceptor);
    *changed = HasChanged(walk);
    if (walk == EWalk::StopUnchanged) {
        return true;
    }
    if (acceptor) {
        const size_t oldSize = acceptor->Range.Size();
        acceptor->Range = range;
        BlockCount += range.Size() - oldSize;
        return true;
    }
    return TryInsertNode(range);
}

bool TBlockRangeFieldSet::TryRemove(TRange range, bool* changed)
{
    *changed = false;
    if (Root == NodeNullIndex) {
        return true;
    }

    // Remove algorithm:
    // 1. Find the first node overlapping range.
    // 2. Walk the continuous run of overlapping nodes left to right:
    //    - range fully inside node → trim left (End = range.Start - 1),
    //      insert a new node on the right (range.End+1 .. node.End);
    //    - left overlap (node.Start < range.Start <= node.End)
    //      → trim right (node.End = range.Start - 1), continue;
    //    - right overlap (node.End > range.End >= node.Start)
    //      → trim left (node.Start = range.End + 1), exit;
    //    - node fully covered → remove by Start key, continue.

    auto walk = WalkRemove(Root, range);
    *changed = HasChanged(walk);
    return !HasOutOfMemory(walk);
}

void TBlockRangeFieldSet::Clear()
{
    Root = NodeNullIndex;
    BlockCount = 0;
    Pool.DeallocateAll();
}

////////////////////////////////////////////////////////////////////////////////

bool TBlockRangeFieldSet::Overlaps(TRange other) const
{
    if (Root == NodeNullIndex) {
        return false;
    }

    ui16 current = Root;
    while (current != NodeNullIndex) {
        const TNode* node = GetNode(current);
        if (node->Range.End < other.Start) {
            current = node->Right;
        } else if (node->Range.Start > other.End) {
            current = node->Left;
        } else {
            return true;
        }
    }

    return false;
}

void TBlockRangeFieldSet::Enumerate(TEnumerateFunc func) const
{
    if (Root == NodeNullIndex) {
        return;
    }

    ui16 current = Root;
    TVector<ui16> stack;

    while (current != NodeNullIndex || !stack.empty()) {
        while (current != NodeNullIndex) {
            stack.push_back(current);
            current = GetNode(current)->Left;
        }
        current = stack.back();
        stack.pop_back();

        const TNode* node = GetNode(current);
        if (func(node->Range) == EEnumerateContinuation::Stop) {
            return;
        }
        current = node->Right;
    }
}

////////////////////////////////////////////////////////////////////////////////

bool TBlockRangeFieldSet::Empty() const
{
    return Root == NodeNullIndex;
}

size_t TBlockRangeFieldSet::GetBlockCount() const
{
    return BlockCount;
}

size_t TBlockRangeFieldSet::GetSegmentCount() const
{
    return Pool.GetAllocatedCount();
}

TArenaPoolStats TBlockRangeFieldSet::GetMemoryStats() const
{
    return Pool.GetMemoryStats();
}

ui16 TBlockRangeFieldSet::AllocNode()
{
    const ui64 result = Pool.Allocate();
    if (result == TArenaAllocatorIndexPool::InvalidIndex) {
        return NodeNullIndex;
    }
    auto* node = GetNode(result);
    node->Left = NodeNullIndex;
    node->Right = NodeNullIndex;
    return result;
}

void TBlockRangeFieldSet::FreeNode(ui16 idx)
{
    Pool.Deallocate(idx);
}

TBlockRangeFieldSet::TNode* TBlockRangeFieldSet::GetNode(ui16 idx)
{
    if (idx == NodeNullIndex) {
        return nullptr;
    }
    return Pool.GetAddress<TNode>(idx);
}

const TBlockRangeFieldSet::TNode* TBlockRangeFieldSet::GetNode(ui16 idx) const
{
    return Pool.GetAddress<const TNode>(idx);
}

TBlockRangeFieldSet::EWalk TBlockRangeFieldSet::WalkAdd(
    ui16& parentId,
    TRange& newRange,
    TNodePtr& acceptor)
{
    TNode* parent = GetNode(parentId);
    if (!parent) {
        return EWalk::ContinueUnchanged;
    }

    const auto& p = parent->Range;

    if (p.Contains(newRange)) {
        return EWalk::StopUnchanged;
    }

    bool needLeftWalk = newRange.Start < p.Start;
    bool needRightWalk = newRange.End > p.End;
    if (p.Overlaps(newRange) ||
        static_cast<ui32>(p.End) + 1 == newRange.Start ||
        static_cast<ui32>(newRange.End) + 1 == p.Start)
    {
        if (newRange.Start > p.Start) {
            newRange.Start = p.Start;
            needLeftWalk = false;
        }
        if (newRange.End < p.End) {
            newRange.End = p.End;
            needRightWalk = false;
        }

        if (!acceptor) {
            acceptor = parent;
        } else {
            BlockCount -= parent->Range.Size();
            RemoveNode(parentId);
            return WalkAdd(parentId, newRange, acceptor);
        }
    }

    if (needLeftWalk && parent->Left != NodeNullIndex) {
        auto walk = WalkAdd(parent->Left, newRange, acceptor);
        if (HasStop(walk)) {
            return walk;
        }
    }
    if (needRightWalk && parent->Right != NodeNullIndex) {
        auto walk = WalkAdd(parent->Right, newRange, acceptor);
        if (HasStop(walk)) {
            return walk;
        }
    }
    return EWalk::ContinueChanged;
}

TBlockRangeFieldSet::EWalk TBlockRangeFieldSet::WalkRemove(
    ui16& parentId,
    TRange range)
{
    TNode* parent = GetNode(parentId);
    if (!parent) {
        return EWalk::ContinueUnchanged;
    }

    auto& p = parent->Range;

    if (range.Contains(p)) {
        BlockCount -= p.Size();
        RemoveNode(parentId);
        return MixChanged(WalkRemove(parentId, range), true);
    }

    bool needLeftWalk = range.Start < p.Start;
    bool needRightWalk = range.End > p.End;
    bool changed = false;
    if (range.Overlaps(p)) {
        changed = true;
        if (p.Start < range.Start && p.End > range.End) {
            // Cut inside — split the node
            return SplitNode(parentId, range);
        }
        const size_t oldSize = p.Size();
        if (p.Start < range.Start) {
            // Trim right
            p.End = range.Start - 1;
            needLeftWalk = false;
        } else if (p.End > range.End) {
            // Trim left
            p.Start = range.End + 1;
            needRightWalk = false;
        }
        BlockCount -= oldSize - p.Size();
    }
    if (needLeftWalk && parent->Left != NodeNullIndex) {
        auto walk = WalkRemove(parent->Left, range);
        changed = changed || HasChanged(walk);
        if (HasStop(walk)) {
            return MixChanged(walk, changed);
        }
    }
    if (needRightWalk && parent->Right != NodeNullIndex) {
        auto walk = WalkRemove(parent->Right, range);
        changed = changed || HasChanged(walk);
        if (HasStop(walk)) {
            return MixChanged(walk, changed);
        }
    }
    return changed ? EWalk::ContinueChanged : EWalk::ContinueUnchanged;
}

void TBlockRangeFieldSet::RemoveNode(ui16& nodeId)
{
    TNode* node = GetNode(nodeId);
    if (node->Left == NodeNullIndex && node->Right == NodeNullIndex) {
        FreeNode(nodeId);
        nodeId = NodeNullIndex;
        return;
    }

    if (node->Left == NodeNullIndex) {
        ui16 r = node->Right;
        FreeNode(nodeId);
        nodeId = r;
        return;
    }

    if (node->Right == NodeNullIndex) {
        ui16 l = node->Left;
        FreeNode(nodeId);
        nodeId = l;
        return;
    }

    // Two children: detach the successor (minimum of the right subtree),
    // copy its data into this node and free the successor.
    const ui16 successor = DetachMin(node->Right);
    node->Range = GetNode(successor)->Range;
    FreeNode(successor);
}

ui16 TBlockRangeFieldSet::DetachMin(ui16& nodeId)
{
    TNode* node = GetNode(nodeId);
    if (node->Left != NodeNullIndex) {
        return DetachMin(node->Left);
    }
    const ui16 min = nodeId;
    nodeId = node->Right;
    return min;
}

// It is guaranteed that the new range does not overlap or touch
// any range in the tree before this call.
// Returns true if the node was inserted; false on out-of-memory.
bool TBlockRangeFieldSet::TryInsertNode(TRange range)
{
    const ui16 idx = AllocNode();
    if (idx == NodeNullIndex) {
        return false;
    }
    GetNode(idx)->Range = range;
    InsertNode(idx);
    return true;
}

TBlockRangeFieldSet::EWalk TBlockRangeFieldSet::SplitNode(
    ui16 parentId,
    TRange range)
{
    ui16 newNodeId = AllocNode();
    if (newNodeId == NodeNullIndex) {
        return EWalk::StopOutOfMemory;
    }
    TNode* parentNode = GetNode(parentId);
    TNode* newNode = GetNode(newNodeId);
    newNode->Range = parentNode->Range;
    parentNode->Range.End = range.Start - 1;
    newNode->Range.Start = range.End + 1;
    BlockCount -= range.Size();
    newNode->Right = parentNode->Right;
    parentNode->Right = newNodeId;
    return EWalk::StopChanged;
}

void TBlockRangeFieldSet::InsertNode(ui16 idx)
{
    const size_t blockCount = GetNode(idx)->Range.Size();
    BlockCount += blockCount;
    if (Root == NodeNullIndex) {
        Root = idx;
    } else {
        const ui16 key = GetNode(idx)->Range.Start;
        ui16 current = Root;
        while (true) {
            TNode* curNode = GetNode(current);
            if (key < curNode->Range.Start) {
                if (curNode->Left == NodeNullIndex) {
                    curNode->Left = idx;
                    return;
                }
                current = curNode->Left;
            } else {
                if (curNode->Right == NodeNullIndex) {
                    curNode->Right = idx;
                    return;
                }
                current = curNode->Right;
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
