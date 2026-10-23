#pragma once
#include "kqp_rbo_physical_op_builder.h"
#include <ydb/core/kqp/opt/rbo/kqp_olap_aggregate.h>
#include <yql/essentials/utils/log/log.h>

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

class TPhysicalSourceBuilder: public TPhysicalNullaryOpBuilder {
public:
    TPhysicalSourceBuilder(TOpRead& read, TExprContext& ctx, TPositionHandle pos, const TPhysicalNames& names,
        const TInfoUnitRegistry& registry, const TString& stageGUID, bool isSysView = false, TString carrierColumn = {})
        : TPhysicalNullaryOpBuilder(ctx, pos, names)
        , Read(read)
        , Registry(registry)
        , StageGUID(stageGUID)
        , IsSysView(isSysView)
        , CarrierColumn(std::move(carrierColumn)) {}

    // Compute an aggregate pushed to column shards in the read program. The
    // source then yields the rows of the aggregate instead of the read.
    TPhysicalSourceBuilder& WithOlapAggregate(const TOlapAggregate& aggregate) {
        Y_ENSURE(aggregate.Read == &Read, "A pushed aggregate must compute over this read");
        OlapAggregate = &aggregate;
        return *this;
    }

    // Mark the key of the trivial DISTINCT over this read in the read program,
    // which then yields only the key column. A nonzero `itemsLimit` stops
    // column shards after that many distinct keys.
    TPhysicalSourceBuilder& WithOlapDistinct(const TOlapDistinct& distinct, ui64 itemsLimit) {
        Y_ENSURE(distinct.Read == &Read, "A pushed distinct must mark this read");
        OlapDistinct = &distinct;
        OlapDistinctLimit = itemsLimit;
        return *this;
    }

    TExprNode::TPtr BuildPhysicalOp() override;

private:
    // Program row fields by name.
    using TProgramFields = THashMap<TString, TExprNode::TPtr>;

    // The program row field holding `function` of the aggregation into `output`.
    TString GetOlapAggregateField(TInfoUnitId output, TStringBuf function) const;
    // Replace the program output with the aggregate rows and list their fields.
    TExprNode::TPtr AddOlapAggregate(TExprNode::TPtr processLambda, TVector<TString>& outputFields) const;
    // Mark the DISTINCT key in the program and list its only output field.
    TExprNode::TPtr AddOlapDistinct(TExprNode::TPtr processLambda, TVector<TString>& outputFields) const;
    // A program projection that casts `column` to Double into `field`.
    TExprBase BuildOlapDoubleCast(const TString& column, const TTypeAnnotationNode& columnType, const TString& field) const;
    // Rebuild the aggregate's outputs, its keys and partial states, from the program row.
    TExprNode::TPtr BuildOlapAggregateRow(const TProgramFields& fields) const;
    // The intermediate AVG state built from the sum and count in the program row.
    TExprNode::TPtr BuildOlapAverageState(const TOlapAggregation& aggregation, const TProgramFields& fields) const;

    TOpRead& Read;
    const TInfoUnitRegistry& Registry;
    TString StageGUID;
    bool IsSysView;
    TString CarrierColumn;
    const TOlapAggregate* OlapAggregate = nullptr;
    const TOlapDistinct* OlapDistinct = nullptr;
    ui64 OlapDistinctLimit = 0;
};
