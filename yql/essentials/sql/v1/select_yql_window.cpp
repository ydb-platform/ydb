#include "select_yql_window.h"

#include "node.h"

namespace NSQLTranslationV1 {

IYqlWindowLikeNode::IYqlWindowLikeNode(TPosition position)
    : INode(std::move(position))
{
}

bool IYqlWindowLikeNode::SetYqlSelectWindowName(TContext& ctx, TString windowName) {
    Y_UNUSED(ctx);
    WindowName_ = std::move(windowName);
    return true;
}

TString IYqlWindowLikeNode::GetWindowName() const {
    return WindowName_;
}

class TYqlWindow final: public IYqlWindowLikeNode, public TYqlWindowArgs {
public:
    TYqlWindow(TPosition position, TYqlWindowArgs&& args)
        : IYqlWindowLikeNode(std::move(position))
        , TYqlWindowArgs(std::move(args))
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!NSQLTranslationV1::Init(ctx, src, Args)) {
            return false;
        }

        Apply_ = Y("YqlWin", Q(Name), Q(GetWindowName()), Q(Y()), Y("Void"));
        for (const auto& arg : Args) {
            Apply_ = L(std::move(Apply_), arg);
        }

        return true;
    }

    TAstNode* Translate(TContext& ctx) const override {
        return Apply_->Translate(ctx);
    }

    TNodePtr DoClone() const override {
        return new TYqlWindow(*this);
    }

private:
    TNodePtr Apply_;
};

TNodePtr BuildYqlWindow(TPosition position, TYqlWindowArgs&& args) {
    return new TYqlWindow(std::move(position), std::move(args));
}

} // namespace NSQLTranslationV1
