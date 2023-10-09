#include "yql_gc_transformer.h"
#include <ydb/library/yql/utils/yql_panic.h>

namespace NYql {

namespace {

class TGcNodeTransformer : public TSyncTransformerBase {
public:
    TGcNodeTransformer()
    {}

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override {
        output = input;

        if (!CurrentThreshold)
            CurrentThreshold = ctx.GcConfig.Settings.NodeCountThreshold;

        if (ctx.NodeAllocationCounter < LastGcCount + CurrentThreshold) {
            return TStatus::Ok;
        }

        const auto oldSize = ctx.ExprNodes.size();
        const auto zombies = std::partition(ctx.ExprNodes.begin(), ctx.ExprNodes.end(), std::bind(std::logical_not<bool>(), std::bind(&TExprNode::Dead, std::placeholders::_1)));

        for (auto it = zombies; ctx.ExprNodes.cend() != it; ++it) {
            const auto dead = it->get();
            if (const auto hash = dead->GetHashAbove()) {
                const auto range = ctx.UniqueNodes.equal_range(hash);
                for (auto jt = range.first; range.second != jt;) {
                    if (jt->second == dead) {
                        jt = ctx.UniqueNodes.erase(jt);
                    } else {
                        ++jt;
                    }
                }
            }
        }

        ctx.ExprNodes.erase(zombies, ctx.ExprNodes.cend());
        const auto liveSize = ctx.ExprNodes.size();

        Y_ABORT_UNLESS(liveSize >= ctx.UniqueNodes.size());

        // Update statistic.
        ++ctx.GcConfig.Statistics.CollectCount;
        ctx.GcConfig.Statistics.TotalCollectedNodes += oldSize - liveSize;

        LastGcCount = ctx.NodeAllocationCounter;
        // adjust next treshold
        CurrentThreshold = Max(ctx.GcConfig.Settings.NodeCountThreshold, liveSize);

        if (liveSize > ctx.NodesAllocationLimit) {
            ctx.AddError(YqlIssue(TPosition(), TIssuesIds::CORE_GC_NODES_LIMIT_EXCEEDED, TStringBuilder()
                << "Too many allocated nodes, allowed: " << ctx.NodesAllocationLimit
                << ", current: " << liveSize));
            return TStatus::Error;
        }

        const auto poolSize = ctx.StringPool.MemoryAllocated() + ctx.StringPool.MemoryWaste();
        if (poolSize > ctx.StringsAllocationLimit) {
            ctx.AddError(YqlIssue(TPosition(), TIssuesIds::CORE_GC_STRINGS_LIMIT_EXCEEDED, TStringBuilder()
                << "Too large string pool, allowed: " << ctx.StringsAllocationLimit
                << ", current: " << poolSize));
            return TStatus::Error;
        }
        return TStatus::Ok;
    }

    void Rewind() final {
        LastGcCount = 0;
        CurrentThreshold = 0;
    }

private:
    ui64 LastGcCount = 0;
    ui64 CurrentThreshold = 0;
};

}

TAutoPtr<IGraphTransformer> CreateGcNodeTransformer() {
    return new TGcNodeTransformer();
}

}
