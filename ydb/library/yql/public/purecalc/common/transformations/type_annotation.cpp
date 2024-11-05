#include "type_annotation.h"

#include <ydb/library/yql/public/purecalc/common/interface.h>
#include <ydb/library/yql/public/purecalc/common/inspect_input.h>
#include <ydb/library/yql/public/purecalc/common/names.h>
#include <ydb/library/yql/public/purecalc/common/transformations/utils.h>

#include <ydb/library/yql/core/type_ann/type_ann_core.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>

#include <util/generic/fwd.h>

using namespace NYql;
using namespace NYql::NPureCalc;

namespace {
    class TTypeAnnotatorBase: public TSyncTransformerBase {
    public:
        using THandler = std::function<TStatus(const TExprNode::TPtr&, TExprNode::TPtr&, TExprContext&)>;

        TTypeAnnotatorBase(TTypeAnnotationContextPtr typeAnnotationContext)
        {
            OriginalTransformer_.reset(CreateExtCallableTypeAnnotationTransformer(*typeAnnotationContext).Release());
        }

        TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
            if (input->Type() == TExprNode::Callable) {
                if (auto handler = Handlers_.FindPtr(input->Content())) {
                    return (*handler)(input, output, ctx);
                }
            }

            auto status = OriginalTransformer_->Transform(input, output, ctx);

            YQL_ENSURE(status.Level != IGraphTransformer::TStatus::Async, "Async type check is not supported");

            return status;
        }

        void Rewind() final {
            OriginalTransformer_->Rewind();
        }

    protected:
        void AddHandler(std::initializer_list<TStringBuf> names, THandler handler) {
            for (auto name: names) {
                YQL_ENSURE(Handlers_.emplace(name, handler).second, "Duplicate handler for " << name);
            }
        }

        template <class TDerived>
        THandler Hndl(TStatus(TDerived::* handler)(const TExprNode::TPtr&, TExprNode::TPtr&, TExprContext&)) {
            return [this, handler] (TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) {
                return (static_cast<TDerived*>(this)->*handler)(input, output, ctx);
            };
        }

        template <class TDerived>
        THandler Hndl(TStatus(TDerived::* handler)(const TExprNode::TPtr&, TExprContext&)) {
            return [this, handler] (TExprNode::TPtr input, TExprNode::TPtr& /*output*/, TExprContext& ctx) {
                return (static_cast<TDerived*>(this)->*handler)(input, ctx);
            };
        }

    private:
        std::shared_ptr<IGraphTransformer> OriginalTransformer_;
        THashMap<TStringBuf, THandler> Handlers_;
    };

    class TTypeAnnotator : public TTypeAnnotatorBase {
    private:
        TTypeAnnotationContextPtr TypeAnnotationContext_;
        const TVector<const TStructExprType*>& InputStructs_;
        TVector<const TStructExprType*>& RawInputTypes_;
        EProcessorMode ProcessorMode_;
        TString InputNodeName_;

    public:
        TTypeAnnotator(
            TTypeAnnotationContextPtr typeAnnotationContext,
            const TVector<const TStructExprType*>& inputStructs,
            TVector<const TStructExprType*>& rawInputTypes,
            EProcessorMode processorMode,
            TString nodeName
        )
            : TTypeAnnotatorBase(typeAnnotationContext)
            , TypeAnnotationContext_(typeAnnotationContext)
            , InputStructs_(inputStructs)
            , RawInputTypes_(rawInputTypes)
            , ProcessorMode_(processorMode)
            , InputNodeName_(std::move(nodeName))
        {
            AddHandler({InputNodeName_}, Hndl(&TTypeAnnotator::HandleInputNode));
            AddHandler({NNodes::TCoTableName::CallableName()}, Hndl(&TTypeAnnotator::HandleTableName));
            AddHandler({NNodes::TCoTablePath::CallableName()}, Hndl(&TTypeAnnotator::HandleTablePath));
            AddHandler({NNodes::TCoHoppingTraits::CallableName()}, Hndl(&TTypeAnnotator::HandleHoppingTraits));
        }

        TTypeAnnotator(TTypeAnnotationContextPtr, TVector<const TStructExprType*>&&, EProcessorMode, TString) = delete;

    private:
        TStatus HandleInputNode(const TExprNode::TPtr& input, TExprContext& ctx) {
            ui32 inputIndex;
            if (!TryFetchInputIndexFromSelf(*input, ctx, InputStructs_.size(), inputIndex)) {
                return IGraphTransformer::TStatus::Error;
            }

            YQL_ENSURE(inputIndex < InputStructs_.size());

            auto itemType = InputStructs_[inputIndex];

            // XXX: Tweak the input expression type, if the spec supports blocks:
            // 1. Add "_yql_block_length" attribute for internal usage.
            // 2. Add block container to wrap the actual item type.
            if (input->IsCallable(PurecalcBlockInputCallableName)) {
                itemType = WrapBlockStruct(itemType, ctx);
            }

            RawInputTypes_[inputIndex] = itemType;

            TColumnOrder columnOrder;
            for (const auto& i : itemType->GetItems()) {
                columnOrder.AddColumn(TString(i->GetName()));
            }

            if (ProcessorMode_ != EProcessorMode::PullList) {
                input->SetTypeAnn(ctx.MakeType<TStreamExprType>(itemType));
            } else {
                input->SetTypeAnn(ctx.MakeType<TListExprType>(itemType));
            }

            TypeAnnotationContext_->SetColumnOrder(*input, columnOrder, ctx);
            return TStatus::Ok;
        }

        TStatus HandleTableName(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
            if (!EnsureMinMaxArgsCount(*input, 1, 2, ctx)) {
                return TStatus::Error;
            }

            if (input->ChildrenSize() > 1) {
                if (!EnsureAtom(input->Tail(), ctx)) {
                    return TStatus::Error;
                }

                if (input->Tail().Content() != PurecalcDefaultService) {
                    ctx.AddError(
                        TIssue(
                            ctx.GetPosition(input->Tail().Pos()),
                            TStringBuilder() << "Unsupported system: " << input->Tail().Content()));
                    return TStatus::Error;
                }
            }

            if (input->Head().IsCallable(NNodes::TCoDependsOn::CallableName())) {
                if (!EnsureArgsCount(input->Head(), 1, ctx)) {
                    return TStatus::Error;
                }

                if (!TryBuildTableNameNode(input->Pos(), input->Head().HeadPtr(), output, ctx)) {
                    return TStatus::Error;
                }
            } else {
                if (!EnsureSpecificDataType(input->Head(), EDataSlot::String, ctx)) {
                    return TStatus::Error;
                }
                output = input->HeadPtr();
            }

            return TStatus::Repeat;
        }

        TStatus HandleTablePath(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
            if (!EnsureArgsCount(*input, 1, ctx)) {
                return TStatus::Error;
            }

            if (!EnsureDependsOn(input->Head(), ctx)) {
                return TStatus::Error;
            }

            if (!EnsureArgsCount(input->Head(), 1, ctx)) {
                return TStatus::Error;
            }

            if (!TryBuildTableNameNode(input->Pos(), input->Head().HeadPtr(), output, ctx)) {
                return TStatus::Error;
            }

            return TStatus::Repeat;
        }

        TStatus HandleHoppingTraits(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
            Y_UNUSED(output);
            if (input->ChildrenSize() == 1) {
                auto children = input->ChildrenList();
                auto falseArg = ctx.Builder(input->Pos())
                    .Atom("false")
                    .Seal()
                    .Build();
                children.emplace_back(falseArg);
                input->ChangeChildrenInplace(std::move(children));
                return TStatus::Repeat;
            }

            return TStatus::Ok;
        }

    private:
        bool TryBuildTableNameNode(
            TPositionHandle position, const TExprNode::TPtr& row, TExprNode::TPtr& result, TExprContext& ctx)
        {
            if (!EnsureStructType(*row, ctx)) {
                return false;
            }

            const auto* structType = row->GetTypeAnn()->Cast<TStructExprType>();

            if (auto pos = structType->FindItem(PurecalcSysColumnTablePath)) {
                if (!EnsureSpecificDataType(row->Pos(), *structType->GetItems()[*pos]->GetItemType(), EDataSlot::String, ctx)) {
                    return false;
                }

                result = ctx.Builder(position)
                    .Callable(NNodes::TCoMember::CallableName())
                        .Add(0, row)
                        .Atom(1, PurecalcSysColumnTablePath)
                    .Seal()
                    .Build();
            } else {
                result = ctx.Builder(position)
                    .Callable(NNodes::TCoString::CallableName())
                        .Atom(0, "")
                    .Seal()
                    .Build();
            }

            return true;
        }
    };
}

TAutoPtr<IGraphTransformer> NYql::NPureCalc::MakeTypeAnnotationTransformer(
    TTypeAnnotationContextPtr typeAnnotationContext,
    const TVector<const TStructExprType*>& inputStructs,
    TVector<const TStructExprType*>& rawInputTypes,
    EProcessorMode processorMode,
    const TString& nodeName
) {
    return new TTypeAnnotator(typeAnnotationContext, inputStructs, rawInputTypes, processorMode, nodeName);
}
