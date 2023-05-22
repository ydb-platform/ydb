#include "yql_optimize.h"

#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <util/generic/hash_set.h>
#include <util/generic/yexception.h>


namespace NYql {

using namespace NNodes;

class TOptimizeTransformerBase::TIgnoreOptimizationContext: public IOptimizationContext {
public:
    TIgnoreOptimizationContext(TOptimizeTransformerBase::TGetParents getParents)
        : GetParents_(std::move(getParents))
    {
    }
    virtual ~TIgnoreOptimizationContext() = default;
    void RemapNode(const TExprNode& src, const TExprNode::TPtr&) final {
        const TParentsMap* parentsMap = GetParents_();
        auto parentsIt = parentsMap->find(&src);
        YQL_ENSURE(parentsIt != parentsMap->cend());
        YQL_ENSURE(parentsIt->second.size() == 1, "Bad usage of local optimizer. Try to switch to global mode");
    }
private:
    TOptimizeTransformerBase::TGetParents GetParents_;
};

class TOptimizeTransformerBase::TRemapOptimizationContext: public IOptimizationContext {
public:
    TRemapOptimizationContext(TNodeOnNodeOwnedMap& remaps)
        : Remaps_(remaps)
    {
    }
    virtual ~TRemapOptimizationContext() = default;
    void RemapNode(const TExprNode& fromNode, const TExprNode::TPtr& toNode) final {
        YQL_ENSURE(Remaps_.emplace(&fromNode, toNode).second, "Duplicate remap of the same node");
    }
    void SetError() {
        HasError_ = true;
    }
    bool CanContinue() const {
        return Remaps_.empty() && !HasError_;
    }
    bool HasError() const {
        return HasError_;
    }
private:
    TNodeOnNodeOwnedMap& Remaps_;
    bool HasError_ = false;
};


TOptimizeTransformerBase::TOptimizeTransformerBase(TTypeAnnotationContext* types, NLog::EComponent logComponent, const TSet<TString>& disabledOpts)
    : Types(types)
    , LogComponent(logComponent)
    , DisabledOpts(disabledOpts)
{
}

IGraphTransformer::TStatus TOptimizeTransformerBase::DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) {
    TOptimizeExprSettings settings(Types);
    IGraphTransformer::TStatus status = IGraphTransformer::TStatus::Ok;

    output = input;
    for (auto& step: Steps) {
        TParentsMap parentsMap;
        bool parentsMapInit = false;
        TGetParents getParents = [&input, &parentsMap, &parentsMapInit] () {
            if (!parentsMapInit) {
                GatherParents(*input, parentsMap);
                parentsMapInit = true;
            }
            return &parentsMap;
        };
        if (step.Global) {
            TNodeOnNodeOwnedMap remaps;
            auto optCtx = TRemapOptimizationContext{remaps};
            VisitExpr(output,
                [&optCtx, &step](const TExprNode::TPtr& node) {
                    return optCtx.CanContinue() && !node->StartsExecution() && !step.ProcessedNodes.contains(node->UniqueId());
                },
                [this, &step, &getParents, &ctx, &optCtx](const TExprNode::TPtr& node) -> bool {
                    if (optCtx.CanContinue() && !node->StartsExecution() && !step.ProcessedNodes.contains(node->UniqueId())) {
                        for (auto& opt: step.Optimizers) {
                            if (opt.Filter(node.Get())) {
                                try {
                                    auto ret = opt.Handler(NNodes::TExprBase(node), ctx, optCtx, getParents);
                                    if (!ret) {
                                        YQL_CVLOG(NLog::ELevel::ERROR, LogComponent) << "Error applying " << opt.OptName;
                                        optCtx.SetError();
                                    } else if (auto retNode = ret.Cast(); retNode.Ptr() != node) {
                                        YQL_CVLOG(NLog::ELevel::INFO, LogComponent) << opt.OptName;
                                        optCtx.RemapNode(*node, retNode.Ptr());
                                    }
                                } catch (...) {
                                    YQL_CVLOG(NLog::ELevel::ERROR, LogComponent) << "Error applying " << opt.OptName << ": " << CurrentExceptionMessage();
                                    throw;
                                }
                            }
                            if (!optCtx.CanContinue()) {
                                break;
                            }
                        }
                        if (optCtx.CanContinue()) {
                            step.ProcessedNodes.insert(node->UniqueId());
                        }
                    }
                    return true;
                }
            );

            if (optCtx.HasError()) {
                status = IGraphTransformer::TStatus::Error;
            } else if (!remaps.empty()) {
                settings.ProcessedNodes = nullptr;
                status = RemapExpr(output, output, remaps, ctx, settings);
            }
        } else {
            settings.ProcessedNodes = &step.ProcessedNodes;
            status = OptimizeExpr(output, output, [this, &step, &getParents](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
                TIgnoreOptimizationContext ignoreOptCtx(getParents);
                for (auto& opt: step.Optimizers) {
                    if (opt.Filter(node.Get())) {
                        try {
                            auto ret = opt.Handler(NNodes::TExprBase(node), ctx, ignoreOptCtx, getParents);
                            if (!ret) {
                                YQL_CVLOG(NLog::ELevel::ERROR, LogComponent) << "Error applying " << opt.OptName;
                                return {};
                            }
                            auto retNode = ret.Cast();
                            if (retNode.Ptr() != node) {
                                YQL_CVLOG(NLog::ELevel::INFO, LogComponent) << opt.OptName;
                                return retNode.Ptr();
                            }
                        } catch (...) {
                            YQL_CVLOG(NLog::ELevel::ERROR, LogComponent) << "Error applying " << opt.OptName << ": " << CurrentExceptionMessage();
                            throw;
                        }
                    }
                }
                return node;
            }, ctx, settings);
        }

        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }
    }

    return status;
}

void TOptimizeTransformerBase::Rewind() {
    for (auto& step: Steps) {
        step.ProcessedNodes.clear();
    }
}

TOptimizeTransformerBase::TFilter TOptimizeTransformerBase::Any() {
    return [] (const TExprNode* node) {
        Y_UNUSED(node);
        return true;
    };
}

TOptimizeTransformerBase::TFilter TOptimizeTransformerBase::Names(std::initializer_list<TStringBuf> names) {
    return [filter = THashSet<TStringBuf>(names)] (const TExprNode* node) {
        return node->IsCallable(filter);
    };
}

TOptimizeTransformerBase::TFilter TOptimizeTransformerBase::Or(std::initializer_list<TOptimizeTransformerBase::TFilter> filters) {
    return [orFilters = TVector<TFilter>(filters)] (const TExprNode* node) {
        for (auto& f: orFilters) {
            if (f(node)) {
                return true;
            }
        }
        return false;
    };
}

void TOptimizeTransformerBase::AddHandler(size_t step, TFilter filter, TStringBuf optName, THandler handler) {
    if (DisabledOpts.contains(optName)) {
        return;
    }

    if (step >= Steps.size()) {
        Steps.resize(step + 1);
    }

    TOptInfo opt;
    opt.OptName = optName;
    opt.Filter = filter;
    opt.Handler = handler;
    Steps[step].Optimizers.push_back(std::move(opt));
}

void TOptimizeTransformerBase::SetGlobal(size_t step) {
    if (step >= Steps.size()) {
        Steps.resize(step + 1);
    }
    Steps[step].Global = true;
}

}
