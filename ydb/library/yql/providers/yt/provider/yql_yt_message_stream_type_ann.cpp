#include "yql_yt_message_stream_impl.h"
#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_message_stream_expr_nodes.h>
#include <ydb/library/yql/providers/common/message_stream/provider.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>

namespace NYql {
using namespace NNodes;
namespace {
class TYtMessageStreamTypeAnnotation final : public TVisitorTransformerBase {
public:
    TYtMessageStreamTypeAnnotation() : TVisitorTransformerBase(true) {
        AddHandler({TYtMessageStreamReadTable::CallableName()}, Hndl(&TYtMessageStreamTypeAnnotation::Read));
        AddHandler({TYtMessageStreamSourceSettings::CallableName()}, Hndl(&TYtMessageStreamTypeAnnotation::Settings));
    }
private:
    TStatus Read(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureArgsCount(*input, 5, ctx) || !EnsureWorldType(*input->Child(0), ctx)
            || !EnsureSpecificDataSource(*input->Child(1), YtProviderName, ctx)
            || !EnsureAtom(*input->Child(2), ctx) || !EnsureAtom(*input->Child(4), ctx)) {
            return TStatus::Error;
        }
        input->SetTypeAnn(ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
            input->Child(0)->GetTypeAnn(), ctx.MakeType<TListExprType>(NFq::NMessageStream::MakeRawRowType(ctx))}));
        return TStatus::Ok;
    }
    TStatus Settings(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureArgsCount(*input, 6, ctx) || !EnsureWorldType(*input->Child(0), ctx)
            || !EnsureAtom(*input->Child(1), ctx) || !EnsureAtom(*input->Child(4), ctx)
            || !EnsureAtom(*input->Child(5), ctx)) {
            return TStatus::Error;
        }
        input->SetTypeAnn(ctx.MakeType<TStreamExprType>(ctx.MakeType<TDataExprType>(NUdf::EDataSlot::String)));
        return TStatus::Ok;
    }
};

}
THolder<TVisitorTransformerBase> CreateYtMessageStreamTypeAnnotation() { return MakeHolder<TYtMessageStreamTypeAnnotation>(); }
}
