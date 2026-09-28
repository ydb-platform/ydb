#include "ddl_backup.h"

#include "context.h"
#include "object_processing.h"

#include <yql/essentials/core/sql_types/yql_callable_names.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>

using namespace NYql;

namespace NSQLTranslationV1 {

class TBaseBackupCollectionNode
    : public TAstListNode,
      public TObjectOperatorContext {
    using TBase = TAstListNode;

public:
    TBaseBackupCollectionNode(
        TPosition pos,
        TString prefix,
        TString objectId,
        const TObjectOperatorContext& context)
        : TBase(pos)
        , TObjectOperatorContext(context)
        , Prefix_(std::move(prefix))
        , Id_(std::move(objectId))
    {
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        auto keys = Y("Key");
        keys = L(keys, Q(Y(Q("backupCollection"), Y("String", BuildQuotedAtom(Pos_, Id_)), Y("String", BuildQuotedAtom(Pos_, Prefix_)))));
        auto options = this->FillOptions(ctx, Y());

        Add("block", Q(Y(
                         Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos_, ServiceId), Scoped_->WrapCluster(Cluster, ctx))),
                         Y("let", "world", Y(TString(WriteName), "world", "sink", keys, Y("Void"), Q(options))),
                         Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world")))));

        return TAstListNode::DoInit(ctx, src);
    }

    virtual INode::TPtr FillOptions(TContext& ctx, INode::TPtr options) const = 0;

protected:
    TString Prefix_;
    TString Id_;
};

class TCreateBackupCollectionNode
    : public TBaseBackupCollectionNode {
    using TBase = TBaseBackupCollectionNode;

public:
    TCreateBackupCollectionNode(
        TPosition pos,
        const TString& prefix,
        const TString& objectId,
        TCreateBackupCollectionParameters params,
        const TObjectOperatorContext& context)
        : TBase(pos, prefix, objectId, context)
        , Params_(std::move(params))
    {
    }

    INode::TPtr FillOptions(TContext& ctx, INode::TPtr options) const final {
        options->Add(Q(Y(Q("mode"), Q("create"))));

        auto settings = Y();
        for (auto& [key, value] : Params_.Settings) {
            settings->Add(Q(Y(BuildQuotedAtom(Pos_, key), Y("String", value.Build()))));
        }
        options->Add(Q(Y(Q("settings"), Q(settings))));

        auto entries = Y();
        if (Params_.Database) {
            entries->Add(Q(Y(Q(Y(Q("type"), Q("database"))))));
        }
        for (auto& table : Params_.Tables) {
            auto path = ctx.GetPrefixedPath(ServiceId, Cluster, table);
            entries->Add(Q(Y(Q(Y(Q("type"), Q("table"))), Q(Y(Q("path"), path)))));
        }
        options->Add(Q(Y(Q("entries"), Q(entries))));

        return options;
    }

    TPtr DoClone() const final {
        return new TCreateBackupCollectionNode(GetPos(), Prefix_, Id_, Params_, *this);
    }

private:
    TCreateBackupCollectionParameters Params_;
};

class TAlterBackupCollectionNode
    : public TBaseBackupCollectionNode {
    using TBase = TBaseBackupCollectionNode;

public:
    TAlterBackupCollectionNode(
        TPosition pos,
        const TString& prefix,
        const TString& objectId,
        TAlterBackupCollectionParameters params,
        const TObjectOperatorContext& context)
        : TBase(pos, prefix, objectId, context)
        , Params_(std::move(params))
    {
    }

    INode::TPtr FillOptions(TContext& ctx, INode::TPtr options) const final {
        options->Add(Q(Y(Q("mode"), Q("alter"))));

        auto settings = Y();
        for (auto& [key, value] : Params_.Settings) {
            settings->Add(Q(Y(BuildQuotedAtom(Pos_, key), Y("String", value.Build()))));
        }
        options->Add(Q(Y(Q("settings"), Q(settings))));

        auto resetSettings = Y();
        for (auto& key : Params_.SettingsToReset) {
            resetSettings->Add(BuildQuotedAtom(Pos_, key));
        }
        options->Add(Q(Y(Q("resetSettings"), Q(resetSettings))));

        auto entries = Y();
        if (Params_.Database != TAlterBackupCollectionParameters::EDatabase::Unchanged) {
            entries->Add(Q(Y(Q(Y(Q("type"), Q("database"))), Q(Y(Q("action"), Q(Params_.Database == TAlterBackupCollectionParameters::EDatabase::Add ? "add" : "drop"))))));
        }
        for (auto& table : Params_.TablesToAdd) {
            auto path = ctx.GetPrefixedPath(ServiceId, Cluster, table);
            entries->Add(Q(Y(Q(Y(Q("type"), Q("table"))), Q(Y(Q("path"), path)), Q(Y(Q("action"), Q("add"))))));
        }
        for (auto& table : Params_.TablesToDrop) {
            auto path = ctx.GetPrefixedPath(ServiceId, Cluster, table);
            entries->Add(Q(Y(Q(Y(Q("type"), Q("table"))), Q(Y(Q("path"), path)), Q(Y(Q("action"), Q("drop"))))));
        }
        options->Add(Q(Y(Q("alterEntries"), Q(entries))));

        return options;
    }

    TPtr DoClone() const final {
        return new TAlterBackupCollectionNode(GetPos(), Prefix_, Id_, Params_, *this);
    }

private:
    TAlterBackupCollectionParameters Params_;
};

class TDropBackupCollectionNode
    : public TBaseBackupCollectionNode {
    using TBase = TBaseBackupCollectionNode;

public:
    TDropBackupCollectionNode(
        TPosition pos,
        const TString& prefix,
        const TString& objectId,
        const TDropBackupCollectionParameters&,
        const TObjectOperatorContext& context)
        : TBase(pos, prefix, objectId, context)
    {
    }

    INode::TPtr FillOptions(TContext&, INode::TPtr options) const final {
        options->Add(Q(Y(Q("mode"), Q("drop"))));

        return options;
    }

    TPtr DoClone() const final {
        TDropBackupCollectionParameters params;
        return new TDropBackupCollectionNode(GetPos(), Prefix_, Id_, params, *this);
    }
};

TNodePtr BuildCreateBackupCollection(
    TPosition pos,
    const TString& prefix,
    const TString& id,
    const TCreateBackupCollectionParameters& params,
    const TObjectOperatorContext& context)
{
    return new TCreateBackupCollectionNode(pos, prefix, id, params, context);
}

TNodePtr BuildAlterBackupCollection(
    TPosition pos,
    const TString& prefix,
    const TString& id,
    const TAlterBackupCollectionParameters& params,
    const TObjectOperatorContext& context)
{
    return new TAlterBackupCollectionNode(pos, prefix, id, params, context);
}

TNodePtr BuildDropBackupCollection(
    TPosition pos,
    const TString& prefix,
    const TString& id,
    const TDropBackupCollectionParameters& params,
    const TObjectOperatorContext& context)
{
    return new TDropBackupCollectionNode(pos, prefix, id, params, context);
}

class TBackupNode final
    : public TAstListNode,
      public TObjectOperatorContext {
    using TBase = TAstListNode;

public:
    TBackupNode(
        TPosition pos,
        TString prefix,
        TString id,
        const TBackupParameters& params,
        const TObjectOperatorContext& context)
        : TBase(pos)
        , TObjectOperatorContext(context)
        , Prefix_(std::move(prefix))
        , Id_(std::move(id))
        , Params_(params)
    {
        Y_UNUSED(Params_);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        auto keys = Y("Key");
        keys = L(keys, Q(Y(Q("backup"), Y("String", BuildQuotedAtom(Pos_, Id_)), Y("String", BuildQuotedAtom(Pos_, Prefix_)))));

        auto opts = Y();

        if (Params_.Incremental) {
            opts->Add(Q(Y(Q("mode"), Q("backupIncremental"))));
        } else {
            opts->Add(Q(Y(Q("mode"), Q("backup"))));
        }

        Add("block", Q(Y(
                         Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos_, ServiceId), Scoped_->WrapCluster(Cluster, ctx))),
                         Y("let", "world", Y(TString(WriteName), "world", "sink", keys, Y("Void"), Q(opts))),
                         Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world")))));

        return TAstListNode::DoInit(ctx, src);
    }

    TPtr DoClone() const final {
        return new TBackupNode(GetPos(), Prefix_, Id_, Params_, *this);
    }

private:
    TString Prefix_;
    TString Id_;
    TBackupParameters Params_;
};

TNodePtr BuildBackup(
    TPosition pos,
    const TString& prefix,
    const TString& id,
    const TBackupParameters& params,
    const TObjectOperatorContext& context)
{
    return new TBackupNode(pos, prefix, id, params, context);
}

class TRestoreNode final
    : public TAstListNode,
      public TObjectOperatorContext {
    using TBase = TAstListNode;

public:
    TRestoreNode(
        TPosition pos,
        TString prefix,
        TString id,
        TRestoreParameters params,
        const TObjectOperatorContext& context)
        : TBase(pos)
        , TObjectOperatorContext(context)
        , Prefix_(std::move(prefix))
        , Id_(std::move(id))
        , Params_(std::move(params))
    {
        Y_UNUSED(Params_);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        auto keys = Y("Key");
        keys = L(keys, Q(Y(Q("restore"), Y("String", BuildQuotedAtom(Pos_, Id_)), Y("String", BuildQuotedAtom(Pos_, Prefix_)))));

        auto opts = Y();
        opts->Add(Q(Y(Q("mode"), Q("restore"))));

        if (Params_.At) {
            opts->Add(Q(Y(Q("at"), BuildQuotedAtom(Pos_, Params_.At))));
        }

        Add("block", Q(Y(
                         Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos_, ServiceId), Scoped_->WrapCluster(Cluster, ctx))),
                         Y("let", "world", Y(TString(WriteName), "world", "sink", keys, Y("Void"), Q(opts))),
                         Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world")))));

        return TAstListNode::DoInit(ctx, src);
    }

    TPtr DoClone() const final {
        return new TRestoreNode(GetPos(), Prefix_, Id_, Params_, *this);
    }

private:
    TString Prefix_;
    TString Id_;
    TRestoreParameters Params_;
};

TNodePtr BuildRestore(
    TPosition pos,
    const TString& prefix,
    const TString& id,
    const TRestoreParameters& params,
    const TObjectOperatorContext& context)
{
    return new TRestoreNode(pos, prefix, id, params, context);
}

} // namespace NSQLTranslationV1
