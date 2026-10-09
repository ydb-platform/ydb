#include "ddl_transfer.h"

#include "context.h"
#include "object_processing.h"

#include <yql/essentials/core/sql_types/yql_callable_names.h>

using namespace NYql;

namespace NSQLTranslationV1 {

class TTransfer
    : public TAstListNode,
      protected TObjectOperatorContext {
protected:
    virtual INode::TPtr FillOptions(INode::TPtr options) const = 0;

public:
    explicit TTransfer(TPosition pos, TString id, TString mode, const TObjectOperatorContext& context)
        : TAstListNode(pos)
        , TObjectOperatorContext(context)
        , Id_(std::move(id))
        , Mode_(std::move(mode))
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Scoped_->UseCluster(ServiceId, Cluster);

        auto keys = Y("Key", Q(Y(Q("transfer"), Y("String", BuildQuotedAtom(Pos_, Id_)))));
        auto options = FillOptions(Y(Q(Y(Q("mode"), Q(Mode_)))));

        Add("block", Q(Y(
                         Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos_, ServiceId), Scoped_->WrapCluster(Cluster, ctx))),
                         Y("let", "world", Y(TString(WriteName), "world", "sink", keys, Y("Void"), Q(options))),
                         Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world")))));

        return TAstListNode::DoInit(ctx, src);
    }

    TPtr DoClone() const final {
        return {};
    }

private:
    const TString Id_;
    const TString Mode_;

}; // TTransfer

class TCreateTransfer final: public TTransfer {
public:
    explicit TCreateTransfer(TPosition pos, TString id, TString source, TString target,
                             TString transformLambda,
                             std::map<TString, TNodePtr>&& settings,
                             const TObjectOperatorContext& context)
        : TTransfer(std::move(pos), std::move(id), "create", context)
        , Source_(std::move(source))
        , Target_(std::move(target))
        , TransformLambda_(std::move(transformLambda))
        , Settings_(std::move(settings))
    {
    }

protected:
    INode::TPtr FillOptions(INode::TPtr options) const override {
        options = L(options, Q(Y(Q("source"), Q(Source_))));
        options = L(options, Q(Y(Q("target"), Q(Target_))));
        options = L(options, Q(Y(Q("transformLambda"), Q(TransformLambda_))));

        if (!Settings_.empty()) {
            auto settings = Y();
            for (auto&& [k, v] : Settings_) {
                if (v) {
                    settings = L(settings, Q(Y(BuildQuotedAtom(Pos_, k), v)));
                } else {
                    settings = L(settings, Q(Y(BuildQuotedAtom(Pos_, k))));
                }
            }
            options = L(options, Q(Y(Q("settings"), Q(settings))));
        }

        return options;
    }

private:
    const TString Source_;
    const TString Target_;
    const TString TransformLambda_;
    std::map<TString, TNodePtr> Settings_;

}; // TCreateTransfer

TNodePtr BuildCreateTransfer(TPosition pos, const TString& id, const TString& source, const TString& target,
                             const TString& transformLambda,
                             std::map<TString, TNodePtr>&& settings,
                             const TObjectOperatorContext& context)
{
    return new TCreateTransfer(pos, id, source, target, transformLambda, std::move(settings), context);
}

class TDropTransfer final: public TTransfer {
public:
    explicit TDropTransfer(TPosition pos, const TString& id, bool cascade, const TObjectOperatorContext& context)
        : TTransfer(pos, id, cascade ? "dropCascade" : "drop", context)
    {
    }

protected:
    INode::TPtr FillOptions(INode::TPtr options) const override {
        return options;
    }

}; // TDropTransfer

TNodePtr BuildDropTransfer(TPosition pos, const TString& id, bool cascade, const TObjectOperatorContext& context) {
    return new TDropTransfer(pos, id, cascade, context);
}

class TAlterTransfer final: public TTransfer {
public:
    explicit TAlterTransfer(TPosition pos, const TString& id, std::optional<TString>&& transformLambda,
                            std::map<TString, TNodePtr>&& settings,
                            const TObjectOperatorContext& context)
        : TTransfer(pos, id, "alter", context)
        , TransformLambda_(std::move(transformLambda))
        , Settings_(std::move(settings))
    {
    }

protected:
    INode::TPtr FillOptions(INode::TPtr options) const override {
        options = L(options, Q(Y(Q("transformLambda"), Q(TransformLambda_ ? TransformLambda_.value() : ""))));

        if (!Settings_.empty()) {
            auto settings = Y();
            for (auto&& [k, v] : Settings_) {
                if (v) {
                    settings = L(settings, Q(Y(BuildQuotedAtom(Pos_, k), v)));
                } else {
                    settings = L(settings, Q(Y(BuildQuotedAtom(Pos_, k))));
                }
            }
            options = L(options, Q(Y(Q("settings"), Q(settings))));
        }

        return options;
    }

private:
    const std::optional<TString> TransformLambda_;
    std::map<TString, TNodePtr> Settings_;

}; // TAlterTransfer

TNodePtr BuildAlterTransfer(TPosition pos, const TString& id, std::optional<TString>&& transformLambda,
                            std::map<TString, TNodePtr>&& settings,
                            const TObjectOperatorContext& context)
{
    return new TAlterTransfer(pos, id, std::move(transformLambda), std::move(settings), context);
}

} // namespace NSQLTranslationV1
