#include "checksum.h"

#include "context.h"
#include "source.h"

#include <yql/essentials/core/sql_types/yql_callable_names.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>

#include <util/generic/yexception.h>

#include <utility>

using namespace NYql;

namespace NSQLTranslationV1 {

class TTableReadStatementNode final: public TAstListNode {
public:
    TTableReadStatementNode(TPosition pos, const TTableRef& table, TScopedStatePtr scoped, TString indexName = {})
        : TAstListNode(pos)
        , Table_(table)
        , Scoped_(std::move(scoped))
        , IndexName_(std::move(indexName))
        , FakeSource_(BuildFakeSource(pos))
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Y_UNUSED(src);

        if (Table_.Service != KikimrProviderName && Table_.Service != YdbProviderName) {
            ctx.Error(Pos_) << StatementName() << " is unsupported for " << Table_.Service << " tables";
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

        auto read = IndexName_.empty()
            ? Y("KiChecksumTable!", "world", source, keys, Q(options))
            : Y("KiCheckIndex!", "world", source, keys, BuildQuotedAtom(Pos_, IndexName_));
        Add("block", Q(Y(
                         Y("let", "x", read),
                         Y("let", "world", Y(TString(LeftName), "x")),
                         Y("let", "output", Y(TString(RightName), "x")),
                         Y("let", "world", writeResult),
                         Y("return", "world"))));

        return TAstListNode::DoInit(ctx, FakeSource_.Get());
    }

    TPtr DoClone() const final {
        ythrow yexception() << "Cloning " << StatementName() << " is not supported";
    }

private:
    TStringBuf StatementName() const {
        return IndexName_.empty() ? "CHECKSUM TABLE" : "CHECK TABLE INDEX";
    }

    TTableRef Table_;
    TScopedStatePtr Scoped_;
    TString IndexName_;
    TSourcePtr FakeSource_;
};

TNodePtr BuildChecksumTable(TPosition pos, const TTableRef& table, TScopedStatePtr scoped) {
    return new TTableReadStatementNode(pos, table, std::move(scoped));
}

TNodePtr BuildCheckIndex(TPosition pos, const TTableRef& table, TScopedStatePtr scoped, const TString& indexName) {
    return new TTableReadStatementNode(pos, table, std::move(scoped), indexName);
}

} // namespace NSQLTranslationV1
