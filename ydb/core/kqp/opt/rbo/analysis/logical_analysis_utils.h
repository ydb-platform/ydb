#pragma once

#include <ydb/core/kqp/opt/rbo/kqp_operator.h>

namespace NKikimr {
namespace NKqp {

class TPlanAnalysisTraversal {
public:
    explicit TPlanAnalysisTraversal(TOpRoot& root) {
        for (const auto& iter : root) {
            PostOrder_.push_back(iter.Current);
        }

        PreOrder_.reserve(PostOrder_.size());
        for (auto it = PostOrder_.rbegin(); it != PostOrder_.rend(); ++it) {
            PreOrder_.push_back(*it);
        }
    }

    const TVector<TIntrusivePtr<IOperator>>& PostOrder() const {
        return PostOrder_;
    }

    const TVector<TIntrusivePtr<IOperator>>& PreOrder() const {
        return PreOrder_;
    }

private:
    TVector<TIntrusivePtr<IOperator>> PostOrder_;
    TVector<TIntrusivePtr<IOperator>> PreOrder_;
};

} // namespace NKqp
} // namespace NKikimr
