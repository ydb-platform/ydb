#pragma once

#include "node.h"

namespace NSQLTranslationV1 {

class IYqlWindowLikeNode: public INode {
public:
    explicit IYqlWindowLikeNode(TPosition position);

    bool SetYqlSelectWindowName(TContext& ctx, TString windowName) override;

protected:
    TString GetWindowName() const;

private:
    TString WindowName_;
};

struct TYqlWindowArgs {
    TString Name;
    TVector<TNodePtr> Args;
};

TNodePtr BuildYqlWindow(TPosition position, TYqlWindowArgs&& args);

} // namespace NSQLTranslationV1
