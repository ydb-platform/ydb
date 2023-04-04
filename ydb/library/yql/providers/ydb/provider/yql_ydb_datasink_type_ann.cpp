#include "yql_ydb_provider_impl.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/providers/ydb/expr_nodes/yql_ydb_expr_nodes.h>

#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>

#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

using namespace NNodes;

namespace {

class TYdbDataSinkTypeAnnotationTransformer : public TVisitorTransformerBase {
public:
    TYdbDataSinkTypeAnnotationTransformer(TYdbState::TPtr state)
        : TVisitorTransformerBase(true)
        , State_(state)
    {
        using TSelf = TYdbDataSinkTypeAnnotationTransformer;
        AddHandler({TCoCommit::CallableName()}, Hndl(&TSelf::HandleCommit));
        AddHandler({TYdbWriteTable::CallableName()}, Hndl(&TSelf::HandleWriteTable));
        AddHandler({NNodes::TYdbClusterConfig::CallableName()}, Hndl(&TSelf::HandleClusterConfig));
    }

    TStatus HandleCommit(TExprBase input, TExprContext&) {
        const auto commit = input.Cast<TCoCommit>();
        input.Ptr()->SetTypeAnn(commit.World().Ref().GetTypeAnn());
        return TStatus::Ok;
    }

    TStatus HandleWriteTable(TExprBase input, TExprContext&) {
        const auto write = input.Cast<TYdbWriteTable>();
        input.Ptr()->SetTypeAnn(write.World().Ref().GetTypeAnn());
        return TStatus::Ok;
    }

    TStatus HandleClusterConfig(TExprBase input, TExprContext& ctx) {
        const auto config = input.Cast<NNodes::TYdbClusterConfig>();
        if (!EnsureTupleOfAtoms(config.Locators().MutableRef(), ctx)) {
            return TStatus::Error;
        }

        if (!EnsureAtom(config.TvmId().Ref(), ctx)) {
            return TStatus::Error;
        }

        input.Ptr()->SetTypeAnn(ctx.MakeType<TUnitExprType>());
        return TStatus::Ok;
    }
private:
    const TYdbState::TPtr State_;
};

}

THolder<TVisitorTransformerBase> CreateYdbDataSinkTypeAnnotationTransformer(TYdbState::TPtr state) {
    return MakeHolder<TYdbDataSinkTypeAnnotationTransformer>(state);
}

} // namespace NYql
