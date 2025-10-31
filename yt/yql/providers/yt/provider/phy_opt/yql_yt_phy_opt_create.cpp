#include "yql_yt_phy_opt.h"

#include <yql/essentials/providers/common/codec/yql_codec_type_flags.h>

namespace NYql {

using namespace NNodes;

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::Create(TExprBase node, TExprContext& ctx) const {
    const auto& create = node.Cast<TYtCreateTable>();
    const TYtTableInfo tableInfo(create.Table());
    const TYtOutTableInfo outTable(State_->TablesData->GetTable(create.DataSink().Cluster().StringValue(), tableInfo.Name, tableInfo.CommitEpoch).RowSpec);

    return Build<TYtPublish>(ctx, create.Pos())
        .World(create.World())
        .DataSink(create.DataSink())
        .Input()
            .Add<TYtOutput>()
                .Operation<TYtTouch>()
                    .World(create.World())
                    .DataSink(create.DataSink())
                    .Output()
                        .Add(outTable.ToExprNode(ctx, create.Table().Pos()))
                        .Build()
                    .Build()
                .OutIndex().Value(0U).Build()
                .Build()
            .Build()
        .Publish(create.Table())
        .Settings(create.Settings())
        .Done();
}


}  // namespace NYql

