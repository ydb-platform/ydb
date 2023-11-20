#include "balance_coverage_builder.h"

namespace NKikimr {

    TBalanceCoverageBuilder::TBalanceCoverageBuilder()
        : IsComplete0(false)
    {
    }

    bool TBalanceCoverageBuilder::IsComplete() const {
        return IsComplete0;
    }

    bool TBalanceCoverageBuilder::AddResult(const NKikimrTx::TBalanceTrackList& tracks) {
        if (IsComplete())
            return false;

        if (tracks.TrackSize() == 0) {
            IsComplete0 = true;
            return true;
        }

        bool hasAdded = false;
        for (const auto& track : tracks.GetTrack()) {
            Y_DEBUG_ABORT_UNLESS(track.HopSize() > 0);
            bool result = AddToTree(track);
            hasAdded = hasAdded || result;
        }

        IsComplete0 = CheckTree();
        return hasAdded;
    }

    bool TBalanceCoverageBuilder::AddToTree(const NKikimrTx::TBalanceTrack& track) {
        if (!Root)
            Root.Reset(new TNode(track.GetHop(0).GetShard()));

        return AddNode(Root, track, 0);
    }

    bool TBalanceCoverageBuilder::AddNode(TAutoPtr<TNode>& node, const NKikimrTx::TBalanceTrack& track, ui32 index) {
        Y_ABORT_UNLESS(node->Tablet == track.GetHop(index).GetShard());
        ++index;
        if (index == track.HopSize()) {
            bool oldResult = node->HasResult;
            node->HasResult = true;
            return !oldResult;
        }

        for (ui64 sibling : track.GetHop(index).GetSibling()) {
            bool isSiblingFound = false;
            for (auto& child : node->Children) {
                if (child->Tablet == sibling) {
                    isSiblingFound = true;
                    break;
                }
            }

            if (!isSiblingFound) {
                node->Children.push_back(new TNode(sibling));
            }
        }

        ui32 nextChild = track.GetHop(index).GetShard();
        for (auto& child : node->Children) {
            if (child->Tablet == nextChild) {
                return AddNode(child, track, index);
            }
        }

        node->Children.push_back(new TNode(nextChild));
        return AddNode(node->Children.back(), track, index);
    }

    bool TBalanceCoverageBuilder::CheckTree() const {
        return CheckNode(*Root);
    }

    bool TBalanceCoverageBuilder::CheckNode(const TNode& node) const {
        if (node.Children.empty())
            return node.HasResult;

        for (const auto& child : node.Children) {
            if (!CheckNode(*child))
                return false;
        }

        return true;
    }

}
