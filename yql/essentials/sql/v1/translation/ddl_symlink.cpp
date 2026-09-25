#include "ddl_symlink.h"

#include "context.h"
#include "source.h"

#include <yql/essentials/core/sql_types/yql_callable_names.h>

using namespace NYql;

namespace NSQLTranslationV1 {

class TSymlinkNode final: public TAstListNode {
public:
    TSymlinkNode(TPosition pos, TSymlinkRef link, TMaybe<TDeferredAtom> target, TString mode, TScopedStatePtr scoped)
        : TAstListNode(pos)
        , Link_(std::move(link))
        , Target_(std::move(target))
        , Mode_(std::move(mode))
        , Scoped_(std::move(scoped))
    {
        FakeSource_ = BuildFakeSource(pos);
        Scoped_->UseCluster(Link_.Service, Link_.Cluster);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Y_UNUSED(src);
        auto linkPath = ctx.GetPrefixedPath(Link_.Service, Link_.Cluster, Link_.Path);
        if (!linkPath) {
            return false;
        }
        auto keys = Y("Key", Q(Y(Q("link"), Y("String", linkPath))));
        if (Target_) {
            auto targetPath = ctx.GetPrefixedPath(Link_.Service, Link_.Cluster, *Target_);
            if (!targetPath) {
                return false;
            }
            keys = L(keys, Q(Y(Q("target"), Y("String", targetPath))));
        }
        auto options = Y();
        options = L(options, Q(Y(Q("mode"), Q(Mode_))));
        if (!keys->Init(ctx, FakeSource_.Get())) {
            return false;
        }

        Add("block", Q(Y(
                         Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos_, Link_.Service), Scoped_->WrapCluster(Link_.Cluster, ctx))),
                         Y("let", "world", Y(TString(WriteName), "world", "sink", keys, Y("Void"), Q(options))),
                         Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world")))));

        return TAstListNode::DoInit(ctx, FakeSource_.Get());
    }

    TPtr DoClone() const final {
        return new TSymlinkNode(Pos_, Link_, Target_, Mode_, Scoped_);
    }

private:
    TSymlinkRef Link_;
    TMaybe<TDeferredAtom> Target_;
    TString Mode_;
    TScopedStatePtr Scoped_;
    TSourcePtr FakeSource_;
};

TNodePtr BuildCreateSymlink(TPosition pos, const TSymlinkRef& link, const TDeferredAtom& target, bool existingOk, TScopedStatePtr scoped) {
    return new TSymlinkNode(pos, link, target, existingOk ? "create_symlink_if_not_exists" : "create_symlink", std::move(scoped));
}

TNodePtr BuildDropSymlink(TPosition pos, const TSymlinkRef& link, bool missingOk, TScopedStatePtr scoped) {
    return new TSymlinkNode(pos, link, Nothing(), missingOk ? "drop_symlink_if_exists" : "drop_symlink", std::move(scoped));
}

} // namespace NSQLTranslationV1
