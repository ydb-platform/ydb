#include "kqp_operator.h"
#include <ydb/core/kqp/opt/rbo/kqp_rbo.h>

namespace NKikimr::NKqp::NOpt {

struct TRBORelOptimizerNode : public TRelOptimizerNode {

    TRBORelOptimizerNode(TVector<TString> labels, TOptimizerStatistics stats, TIntrusivePtr<IOperator> op) :
        TRelOptimizerNode(labels[0], std::move(stats)),
        _Labels(labels),
        Op(op)
        {}

    TVector<TString> Labels() override {
        return _Labels;
    }

    void Print(std::stringstream& stream, int ntabs) override {
        for (int i = 0; i < ntabs; i++) {
            stream << "    ";
        }
        stream << "Rels: ";

        for (auto r : _Labels ) {
            stream << r << ", ";
        }
        stream << "\n";

        for (int i = 0; i < ntabs; i++) {
            stream << "    ";
        }
        stream << Stats << "\n";
    }

    TVector<TString> _Labels;
    TIntrusivePtr<IOperator> Op;
};

struct TRBOProviderContext : public TKqpProviderContext {
    TRBOProviderContext(const TKqpOptimizeContext& kqpCtx, const int optLevel, bool useBlockHashJoin) : TKqpProviderContext(kqpCtx, optLevel, useBlockHashJoin) {}

    virtual bool IsJoinApplicable(
        const std::shared_ptr<IBaseOptimizerNode>& left,
        const std::shared_ptr<IBaseOptimizerNode>& right,
        const TVector<TJoinColumn>& leftJoinKeys,
        const TVector<TJoinColumn>& rightJoinKeys,
        NKqp::EJoinAlgoType joinAlgo,
        EJoinKind joinKind
    ) override {
        if (joinAlgo == NKqp::EJoinAlgoType::LookupJoin || joinAlgo == NKqp::EJoinAlgoType::LookupJoinReverse) {
            return false;
        }
        return TKqpProviderContext::IsJoinApplicable(left, right, leftJoinKeys, rightJoinKeys, joinAlgo, joinKind);
    }
};
}
