#pragma once

#include "kqp_operator.h"
#include <ydb/core/kqp/opt/rbo/kqp_rbo.h>

namespace NKikimr::NKqp {

// A CBO column is named by its decimal ID, as in expression members, and
// qualified by the relation of the CBO leaf that outputs it.
inline TJoinColumn MakeCBOColumn(const TString& relation, TInfoUnitId id) {
    return TJoinColumn(relation, ToString(id));
}

inline TInfoUnitId GetCBOColumnId(const TJoinColumn& column) {
    TInfoUnitId id;
    Y_ENSURE(TryFromString(column.AttributeName, id) && id != TUnorderedIUs::InvalidBit,
        "CBO column " << column.RelName << "." << column.AttributeName << " is not an RBO ID");
    return id;
}

} // namespace NKikimr::NKqp

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
    TRBOProviderContext(const TKqpOptimizeContext& kqpCtx, const int optLevel, bool useBlockHashJoin, const TColumnLineage& lineage,
                        const NYql::TTypeAnnotationContext* typeCtx = nullptr)
        : TKqpProviderContext(kqpCtx, optLevel, useBlockHashJoin)
        , Lineage(lineage)
        , TypeCtx(typeCtx)
    {}

    virtual bool IsJoinApplicable(
        const std::shared_ptr<IBaseOptimizerNode>& left,
        const std::shared_ptr<IBaseOptimizerNode>& right,
        const TVector<TJoinColumn>& leftJoinKeys,
        const TVector<TJoinColumn>& rightJoinKeys,
        NKqp::EJoinAlgoType joinAlgo,
        EJoinKind joinKind
    ) override;

    // With cost-based lookup join, a lookup join costs the lookups of its left rows in the 
    //  target IsJoinApplicable chose, instead of the right side
    virtual TOptimizerStatistics ComputeJoinStats(
        const TOptimizerStatistics& leftStats,
        const TOptimizerStatistics& rightStats,
        const TVector<TJoinColumn>& leftJoinKeys,
        const TVector<TJoinColumn>& rightJoinKeys,
        EJoinAlgoType joinAlgo,
        EJoinKind joinKind,
        TCardinalityHints::TCardinalityHint* maybeHint = nullptr) const override;

    const TColumnLineage& Lineage;
    // Column statistics for the lookup costs, if any
    const NYql::TTypeAnnotationContext* TypeCtx;

private:
    // The cost per left row of the target IsJoinApplicable chose, by the right join keys and the join kind. The solver
    // computes the stats of a join right after checking it is applicable, and the target does not depend on the left side.
    THashMap<TString, double> LookupCostPerLeftRow;
};
}
