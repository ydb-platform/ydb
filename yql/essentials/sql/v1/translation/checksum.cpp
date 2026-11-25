#include "checksum.h"

#include "context.h"
#include "source.h"

#include <yql/essentials/core/sql_types/yql_callable_names.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>

#include <util/generic/yexception.h>

#include <utility>

using namespace NYql;

namespace NSQLTranslationV1 {

class TChecksumTableNode final: public TAstListNode {
public:
    TChecksumTableNode(TPosition pos, const TTableRef& table, TScopedStatePtr scoped)
        : TAstListNode(pos)
        , Table_(table)
        , Scoped_(std::move(scoped))
        , FakeSource_(BuildFakeSource(pos))
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Y_UNUSED(src);

        if (Table_.Service != KikimrProviderName && Table_.Service != YdbProviderName) {
            ctx.Error(Pos_) << "CHECKSUM TABLE is unsupported for " << Table_.Service << " tables";
            return false;
        }

        if (Table_.Cluster.Empty()) {
            ctx.Error(Pos_) << "No cluster name given and no default cluster is selected";
            return false;
        }

        Scoped_->UseCluster(Table_.Service, Table_.Cluster);
        auto keys = Table_.Keys->GetTableKeys()->BuildKeys(ctx, ITableKeys::EBuildKeysMode::INPUT);
        if (!keys || !keys->Init(ctx, FakeSource_.Get())) {
            return false;
        }

        auto options = Table_.Options ? Table_.Options : Y();
        if (!options->Init(ctx, FakeSource_.Get())) {
            return false;
        }

        bool asRef = ctx.PragmaRefSelect;
        bool asAutoRef = true;
        if (ctx.PragmaSampleSelect) {
            asRef = false;
            asAutoRef = false;
        }

        auto settings = Y(Q(Y(Q("type"))));
        if (asRef) {
            settings = L(settings, Q(Y(Q("ref"))));
        } else if (asAutoRef) {
            settings = L(settings, Q(Y(Q("autoref"))));
        }

        if (ctx.ResultRowsLimit > 0) {
            settings = L(settings, Q(Y(Q("take"), Q(ToString(ctx.ResultRowsLimit)))));
        }

        auto source = Y("DataSource", BuildQuotedAtom(Pos_, Table_.Service), Scoped_->WrapCluster(Table_.Cluster, ctx));
        auto writeResult = BuildWriteResult(Pos_, "output", settings);
        if (!writeResult->Init(ctx, FakeSource_.Get())) {
            return false;
        }

        Add("block", Q(Y(
                         Y("let", "x", Y("KiChecksumTable!", "world", source, keys, Q(options))),
                         Y("let", "world", Y(TString(LeftName), "x")),
                         Y("let", "output", Y(TString(RightName), "x")),
                         Y("let", "world", writeResult),
                         Y("return", "world"))));

        return TAstListNode::DoInit(ctx, FakeSource_.Get());
    }

    TPtr DoClone() const final {
        ythrow yexception() << "Cloning CHECKSUM TABLE is not supported";
    }

private:
    TTableRef Table_;
    TScopedStatePtr Scoped_;
    TSourcePtr FakeSource_;
};

TNodePtr BuildChecksumTable(TPosition pos, const TTableRef& table, TScopedStatePtr scoped) {
    return new TChecksumTableNode(pos, table, std::move(scoped));
}

} // namespace NSQLTranslationV1
