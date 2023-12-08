#include "yql_pg_provider_impl.h"
#include <ydb/library/yql/providers/common/dq/yql_dq_integration_impl.h>
#include <ydb/library/yql/providers/pg/expr_nodes/yql_pg_expr_nodes.h>

namespace NYql {

using namespace NNodes;

namespace {

class TPgDqIntegration: public TDqIntegrationBase {
public:
    TPgDqIntegration(TPgState::TPtr state)
        : State_(state)
    {}

    bool CanRead(const TExprNode& read, TExprContext&, bool) override {
        return TPgReadTable::Match(&read);
    }

    TMaybe<ui64> EstimateReadSize(ui64 /*dataSizePerJob*/, ui32 /*maxTasksPerStage*/, const TVector<const TExprNode*>& read, TExprContext&) override {
        if (AllOf(read, [](const auto val) { return TPgReadTable::Match(val); })) {
            return 0ul;
        }
        return Nothing();
    }

    ui64 Partition(const TDqSettings&, size_t, const TExprNode&, TVector<TString>& partitions, TString*, TExprContext&, bool) override {
        partitions.clear();
        partitions.emplace_back();
        return 0ULL;
    }

private:
    const TPgState::TPtr State_;
};

}

THolder<IDqIntegration> CreatePgDqIntegration(TPgState::TPtr state) {
    return MakeHolder<TPgDqIntegration>(state);
}

}
