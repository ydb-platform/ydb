#include "ddl_session.h"

#include "context.h"
#include "source.h"

#include <yql/essentials/providers/common/provider/yql_provider_names.h>

#include <utility>

using namespace NYql;

namespace NSQLTranslationV1 {

class TKillSessionNode final: public TAstListNode {
public:
    TKillSessionNode(TPosition pos, TNodePtr sessionId, TString service, TDeferredAtom cluster, TScopedStatePtr scoped)
        : TAstListNode(pos)
        , SessionId_(std::move(sessionId))
        , Service_(std::move(service))
        , Cluster_(std::move(cluster))
        , Scoped_(std::move(scoped))
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (Service_ != KikimrProviderName) {
            ctx.Error(Pos_) << "KILL SESSION is supported only for YDB";
            return false;
        }
        if (Cluster_.Empty()) {
            ctx.Error(Pos_) << "No cluster name given and no default cluster is selected";
            return false;
        }

        auto fakeSource = BuildFakeSource(Pos_);
        if (!SessionId_->Init(ctx, fakeSource.Get())) {
            return false;
        }

        Scoped_->UseCluster(Service_, Cluster_);
        Add("block", Q(Y(
                         Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos_, TString(KikimrProviderName)),
                                            Scoped_->WrapCluster(Cluster_, ctx))),
                         Y("let", "world", Y("KiKillSession!", "world", "sink", SessionId_)),
                         Y("return", AstNode("world")))));
        return TAstListNode::DoInit(ctx, src);
    }

    TPtr DoClone() const final {
        return new TKillSessionNode(Pos_, SafeClone(SessionId_), Service_, Cluster_, Scoped_);
    }

private:
    TNodePtr SessionId_;
    TString Service_;
    TDeferredAtom Cluster_;
    TScopedStatePtr Scoped_;
};

TNodePtr BuildKillSession(TPosition pos, TNodePtr sessionId, TScopedStatePtr scoped) {
    return new TKillSessionNode(pos, std::move(sessionId), scoped->CurrService, scoped->CurrCluster, scoped);
}

} // namespace NSQLTranslationV1
