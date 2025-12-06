#include "kqp_operator.h"
#include <ydb/core/kqp/opt/rbo/kqp_rbo.h>

namespace NKikimr::NKqp::NOpt {

struct TRBORelOptimizerNode : public NYql::TRelOptimizerNode {

    TRBORelOptimizerNode(TVector<TString> labels, NYql::TOptimizerStatistics stats, std::shared_ptr<IOperator> op) :
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
    std::shared_ptr<IOperator> Op;
};

struct TRBOProviderContext : public TKqpProviderContext {
    TRBOProviderContext(const TKqpOptimizeContext& kqpCtx, const int optLevel) : TKqpProviderContext(kqpCtx, optLevel) {}

    virtual bool IsJoinApplicable(
        const std::shared_ptr<NYql::IBaseOptimizerNode>& left, 
        const std::shared_ptr<NYql::IBaseOptimizerNode>& right, 
        const TVector<NYql::NDq::TJoinColumn>& leftJoinKeys, 
        const TVector<NYql::NDq::TJoinColumn>& rightJoinKeys,
        NYql::EJoinAlgoType joinAlgo,  
        NYql::EJoinKind joinKind
    ) override {
        Y_UNUSED(left);
        Y_UNUSED(right);
        Y_UNUSED(leftJoinKeys);
        Y_UNUSED(rightJoinKeys);
        Y_UNUSED(joinAlgo);
        Y_UNUSED(joinKind);
        return false;
    }
};
}