#pragma once
#include "defs.h"
#include <ydb/core/protos/tx.pb.h>

namespace NKikimr {

    class TBalanceCoverageBuilder : TNonCopyable {
    public:
        TBalanceCoverageBuilder();
        bool IsComplete() const;
        /// returns false if result was already added
        bool AddResult(const NKikimrTx::TBalanceTrackList& tracks);

    private:
        struct TNode {
            const ui64 Tablet;
            bool HasResult;
            TVector<TAutoPtr<TNode>> Children;

            TNode(ui64 tablet)
                : Tablet(tablet)
                , HasResult(false)
            {
            }
        };

        bool AddToTree(const NKikimrTx::TBalanceTrack& track);
        bool AddNode(TAutoPtr<TNode>& node, const NKikimrTx::TBalanceTrack& track, ui32 index);
        bool CheckTree() const;
        bool CheckNode(const TNode& node) const;

    private:
        bool IsComplete0;
        TAutoPtr<TNode> Root;
    };

}
