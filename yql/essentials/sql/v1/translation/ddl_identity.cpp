#include "ddl_identity.h"

#include "context.h"

#include <yql/essentials/core/sql_types/yql_callable_names.h>

using namespace NYql;

namespace NSQLTranslationV1 {

class TControlUser final: public TAstListNode {
public:
    TControlUser(TPosition pos, const TString& service, const TDeferredAtom& cluster, TDeferredAtom name, const TMaybe<TUserParameters>& params, TScopedStatePtr scoped, bool IsCreateUser)
        : TAstListNode(pos)
        , Service_(service)
        , Cluster_(cluster)
        , Name_(std::move(name))
        , Params_(params)
        , Scoped_(scoped)
        , IsCreateUser_(IsCreateUser)
    {
        FakeSource_ = BuildFakeSource(pos);
        scoped->UseCluster(service, cluster);
    }

    bool DoInit(TContext& ctx, ISource*) override {
        auto name = Name_.Build();
        TNodePtr password;
        TNodePtr hash;

        if (Params_) {
            if (Params_->Password) {
                password = Params_->Password->Build();
            } else if (Params_->Hash) {
                hash = Params_->Hash->Build();
            }
        }

        TNodePtr cluster = Scoped_->WrapCluster(Cluster_, ctx);

        if (!name->Init(ctx, FakeSource_.Get()) ||
            !cluster->Init(ctx, FakeSource_.Get()) ||
            (password && !password->Init(ctx, FakeSource_.Get())) ||
            (hash && !hash->Init(ctx, FakeSource_.Get())))
        {
            return false;
        }

        auto options = Y(Q(Y(Q("mode"), Q(IsCreateUser_ ? "createUser" : "alterUser"))));

        TVector<TNodePtr> roles;
        if (Params_ && !Params_->Roles.empty()) {
            for (auto& item : Params_->Roles) {
                roles.push_back(item.Build());
                if (!roles.back()->Init(ctx, FakeSource_.Get())) {
                    return false;
                }
            }

            options = L(options, Q(Y(Q("roles"), Q(new TAstListNodeImpl(Pos_, std::move(roles))))));
        }

        if (Params_) {
            if (Params_->IsPasswordEncrypted) {
                options = L(options, Q(Y(Q("passwordEncrypted"))));
            }

            if (Params_->Password) {
                options = L(options, Q(Y(Q("password"), password)));
            } else if (Params_->Hash) {
                options = L(options, Q(Y(Q("hash"), hash)));
            } else if (Params_->IsPasswordNull) {
                options = L(options, Q(Y(Q("nullPassword"))));
            }

            if (Params_->CanLogin.has_value()) {
                options = L(options, Q(Y(Q(Params_->CanLogin.value() ? "login" : "noLogin"))));
            }
        }

        Add("block", Q(Y(
                         Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos_, Service_), cluster)),
                         Y("let", "world", Y(TString(WriteName), "world", "sink", Y("Key", Q(Y(Q("role"), Y("String", name)))), Y("Void"), Q(options))),
                         Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world")))));

        return TAstListNode::DoInit(ctx, FakeSource_.Get());
    }

    TPtr DoClone() const final {
        return {};
    }

private:
    const TString Service_;
    TDeferredAtom Cluster_;
    TDeferredAtom Name_;
    const TMaybe<TUserParameters> Params_;
    TScopedStatePtr Scoped_;
    TSourcePtr FakeSource_;
    bool IsCreateUser_;
};

TNodePtr BuildControlUser(TPosition pos,
                          const TString& service,
                          const TDeferredAtom& cluster,
                          const TDeferredAtom& name,
                          const TMaybe<TUserParameters>& params,
                          TScopedStatePtr scoped,
                          bool isCreateUser)
{
    return new TControlUser(pos, service, cluster, name, params, scoped, isCreateUser);
}

class TCreateGroup final: public TAstListNode {
public:
    TCreateGroup(TPosition pos, const TString& service, const TDeferredAtom& cluster, TDeferredAtom name, const TMaybe<TCreateGroupParameters>& params, TScopedStatePtr scoped)
        : TAstListNode(pos)
        , Service_(service)
        , Cluster_(cluster)
        , Name_(std::move(name))
        , Params_(params)
        , Scoped_(scoped)
    {
        FakeSource_ = BuildFakeSource(pos);
        scoped->UseCluster(service, cluster);
    }

    bool DoInit(TContext& ctx, ISource*) override {
        auto options = Y(Q(Y(Q("mode"), Q("createGroup"))));

        TVector<TNodePtr> roles;
        if (Params_ && !Params_->Roles.empty()) {
            for (auto& item : Params_->Roles) {
                roles.push_back(item.Build());
                if (!roles.back()->Init(ctx, FakeSource_.Get())) {
                    return false;
                }
            }

            options = L(options, Q(Y(Q("roles"), Q(new TAstListNodeImpl(Pos_, std::move(roles))))));
        }

        TNodePtr cluster = Scoped_->WrapCluster(Cluster_, ctx);

        Add("block", Q(Y(
                         Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos_, Service_), cluster)),
                         Y("let", "world", Y(TString(WriteName), "world", "sink", Y("Key", Q(Y(Q("role"), Y("String", Name_.Build())))), Y("Void"), Q(options))),
                         Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world")))));

        return TAstListNode::DoInit(ctx, FakeSource_.Get());
    }

    TPtr DoClone() const final {
        return {};
    }

private:
    const TString Service_;
    TDeferredAtom Cluster_;
    TDeferredAtom Name_;
    const TMaybe<TCreateGroupParameters> Params_;
    TScopedStatePtr Scoped_;
    TSourcePtr FakeSource_;
};

TNodePtr BuildCreateGroup(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TDeferredAtom& name, const TMaybe<TCreateGroupParameters>& params, TScopedStatePtr scoped) {
    return new TCreateGroup(pos, service, cluster, name, params, scoped);
}

class TRenameRole final: public TAstListNode {
public:
    TRenameRole(TPosition pos, bool isUser, const TString& service, const TDeferredAtom& cluster, TDeferredAtom name, TDeferredAtom newName, TScopedStatePtr scoped)
        : TAstListNode(pos)
        , IsUser_(isUser)
        , Service_(service)
        , Cluster_(cluster)
        , Name_(std::move(name))
        , NewName_(std::move(newName))
        , Scoped_(scoped)
    {
        FakeSource_ = BuildFakeSource(pos);
        scoped->UseCluster(service, cluster);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Y_UNUSED(src);
        auto name = Name_.Build();
        auto newName = NewName_.Build();
        TNodePtr cluster = Scoped_->WrapCluster(Cluster_, ctx);

        if (!name->Init(ctx, FakeSource_.Get()) ||
            !newName->Init(ctx, FakeSource_.Get()) ||
            !cluster->Init(ctx, FakeSource_.Get()))
        {
            return false;
        }

        auto options = Y(Q(Y(Q("mode"), Q(IsUser_ ? "renameUser" : "renameGroup"))));
        options = L(options, Q(Y(Q("newName"), newName)));

        Add("block", Q(Y(
                         Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos_, Service_), cluster)),
                         Y("let", "world", Y(TString(WriteName), "world", "sink", Y("Key", Q(Y(Q("role"), Y("String", name)))), Y("Void"), Q(options))),
                         Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world")))));

        return TAstListNode::DoInit(ctx, FakeSource_.Get());
    }

    TPtr DoClone() const final {
        return {};
    }

private:
    const bool IsUser_;
    const TString Service_;
    TDeferredAtom Cluster_;
    TDeferredAtom Name_;
    TDeferredAtom NewName_;
    TScopedStatePtr Scoped_;
    TSourcePtr FakeSource_;
};

TNodePtr BuildRenameUser(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TDeferredAtom& name, const TDeferredAtom& newName, TScopedStatePtr scoped) {
    const bool isUser = true;
    return new TRenameRole(pos, isUser, service, cluster, name, newName, scoped);
}

TNodePtr BuildRenameGroup(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TDeferredAtom& name, const TDeferredAtom& newName, TScopedStatePtr scoped) {
    const bool isUser = false;
    return new TRenameRole(pos, isUser, service, cluster, name, newName, scoped);
}

class TAlterGroup final: public TAstListNode {
public:
    TAlterGroup(TPosition pos, const TString& service, const TDeferredAtom& cluster, TDeferredAtom name, const TVector<TDeferredAtom>& toChange, bool isDrop, TScopedStatePtr scoped)
        : TAstListNode(pos)
        , Service_(service)
        , Cluster_(cluster)
        , Name_(std::move(name))
        , ToChange_(toChange)
        , IsDrop_(isDrop)
        , Scoped_(scoped)
    {
        FakeSource_ = BuildFakeSource(pos);
        scoped->UseCluster(service, cluster);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Y_UNUSED(src);
        auto name = Name_.Build();
        TNodePtr cluster = Scoped_->WrapCluster(Cluster_, ctx);

        if (!name->Init(ctx, FakeSource_.Get()) || !cluster->Init(ctx, FakeSource_.Get())) {
            return false;
        }

        TVector<TNodePtr> toChange;
        for (auto& item : ToChange_) {
            toChange.push_back(item.Build());
            if (!toChange.back()->Init(ctx, FakeSource_.Get())) {
                return false;
            }
        }

        auto options = Y(Q(Y(Q("mode"), Q(IsDrop_ ? "dropUsersFromGroup" : "addUsersToGroup"))));
        options = L(options, Q(Y(Q("roles"), Q(new TAstListNodeImpl(Pos_, std::move(toChange))))));

        Add("block", Q(Y(
                         Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos_, Service_), cluster)),
                         Y("let", "world", Y(TString(WriteName), "world", "sink", Y("Key", Q(Y(Q("role"), Y("String", name)))), Y("Void"), Q(options))),
                         Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world")))));

        return TAstListNode::DoInit(ctx, FakeSource_.Get());
    }

    TPtr DoClone() const final {
        return {};
    }

private:
    const TString Service_;
    TDeferredAtom Cluster_;
    TDeferredAtom Name_;
    TVector<TDeferredAtom> ToChange_;
    const bool IsDrop_;
    TScopedStatePtr Scoped_;
    TSourcePtr FakeSource_;
};

TNodePtr BuildAlterGroup(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TDeferredAtom& name, const TVector<TDeferredAtom>& toChange, bool isDrop,
                         TScopedStatePtr scoped)
{
    return new TAlterGroup(pos, service, cluster, name, toChange, isDrop, scoped);
}

class TDropRoles final: public TAstListNode {
public:
    TDropRoles(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TVector<TDeferredAtom>& toDrop, bool isUser, bool missingOk, TScopedStatePtr scoped)
        : TAstListNode(pos)
        , Service_(service)
        , Cluster_(cluster)
        , ToDrop_(toDrop)
        , IsUser_(isUser)
        , MissingOk_(missingOk)
        , Scoped_(scoped)
    {
        FakeSource_ = BuildFakeSource(pos);
        scoped->UseCluster(service, cluster);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Y_UNUSED(src);
        TNodePtr cluster = Scoped_->WrapCluster(Cluster_, ctx);

        if (!cluster->Init(ctx, FakeSource_.Get())) {
            return false;
        }

        const char* mode = IsUser_
                               ? (MissingOk_ ? "dropUserIfExists" : "dropUser")
                               : (MissingOk_ ? "dropGroupIfExists" : "dropGroup");

        auto options = Y(Q(Y(Q("mode"), Q(mode))));

        auto block = Y(Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos_, Service_), cluster)));
        for (auto& item : ToDrop_) {
            auto name = item.Build();
            if (!name->Init(ctx, FakeSource_.Get())) {
                return false;
            }

            block = L(block, Y("let", "world", Y(TString(WriteName), "world", "sink", Y("Key", Q(Y(Q("role"), Y("String", name)))), Y("Void"), Q(options))));
        }
        block = L(block, Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world")));
        Add("block", Q(block));

        return TAstListNode::DoInit(ctx, FakeSource_.Get());
    }

    TPtr DoClone() const final {
        return {};
    }

private:
    const TString Service_;
    TDeferredAtom Cluster_;
    TVector<TDeferredAtom> ToDrop_;
    const bool IsUser_;
    const bool MissingOk_;
    TScopedStatePtr Scoped_;
    TSourcePtr FakeSource_;
};

TNodePtr BuildDropRoles(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TVector<TDeferredAtom>& toDrop, bool isUser, bool missingOk, TScopedStatePtr scoped) {
    return new TDropRoles(pos, service, cluster, toDrop, isUser, missingOk, scoped);
}

class TPermissionsAction final: public TAstListNode {
public:
    struct TPermissionParameters {
        TString PermissionAction;
        TVector<TDeferredAtom> Permissions;
        TVector<TDeferredAtom> SchemaPaths;
        TVector<TDeferredAtom> RoleNames;
    };

    TPermissionsAction(TPosition pos, const TString& service, const TDeferredAtom& cluster, TPermissionParameters parameters, TScopedStatePtr scoped)
        : TAstListNode(pos)
        , Service_(service)
        , Cluster_(cluster)
        , Parameters_(std::move(parameters))
        , Scoped_(scoped)
    {
        FakeSource_ = BuildFakeSource(pos);
        scoped->UseCluster(service, cluster);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Y_UNUSED(src);

        TNodePtr cluster = Scoped_->WrapCluster(Cluster_, ctx);
        TNodePtr permissionAction = TDeferredAtom(Pos_, Parameters_.PermissionAction).Build();

        if (!permissionAction->Init(ctx, FakeSource_.Get()) ||
            !cluster->Init(ctx, FakeSource_.Get())) {
            return false;
        }

        TVector<TNodePtr> paths;
        paths.reserve(Parameters_.SchemaPaths.size());
        for (auto& item : Parameters_.SchemaPaths) {
            paths.push_back(item.Build());
            if (!paths.back()->Init(ctx, FakeSource_.Get())) {
                return false;
            }
        }
        auto options = Y(Q(Y(Q("paths"), Q(new TAstListNodeImpl(Pos_, std::move(paths))))));

        TVector<TNodePtr> permissions;
        permissions.reserve(Parameters_.Permissions.size());
        for (auto& item : Parameters_.Permissions) {
            permissions.push_back(item.Build());
            if (!permissions.back()->Init(ctx, FakeSource_.Get())) {
                return false;
            }
        }
        options = L(options, Q(Y(Q("permissions"), Q(new TAstListNodeImpl(Pos_, std::move(permissions))))));

        TVector<TNodePtr> roles;
        roles.reserve(Parameters_.RoleNames.size());
        for (auto& item : Parameters_.RoleNames) {
            roles.push_back(item.Build());
            if (!roles.back()->Init(ctx, FakeSource_.Get())) {
                return false;
            }
        }
        options = L(options, Q(Y(Q("roles"), Q(new TAstListNodeImpl(Pos_, std::move(roles))))));

        auto block = Y(Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos_, Service_), cluster)));
        block = L(block, Y("let", "world", Y(TString(WriteName), "world", "sink", Y("Key", Q(Y(Q("permission"), Y("String", permissionAction)))), Y("Void"), Q(options))));
        block = L(block, Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world")));
        Add("block", Q(block));

        return TAstListNode::DoInit(ctx, FakeSource_.Get());
    }

    TPtr DoClone() const final {
        return {};
    }

private:
    const TString Service_;
    TDeferredAtom Cluster_;
    TPermissionParameters Parameters_;
    TScopedStatePtr Scoped_;
    TSourcePtr FakeSource_;
};

TNodePtr BuildGrantPermissions(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TVector<TDeferredAtom>& permissions, const TVector<TDeferredAtom>& schemaPaths, const TVector<TDeferredAtom>& roleNames, TScopedStatePtr scoped) {
    return new TPermissionsAction(pos,
                                  service,
                                  cluster,
                                  {.PermissionAction = "grant",
                                   .Permissions = permissions,
                                   .SchemaPaths = schemaPaths,
                                   .RoleNames = roleNames},
                                  scoped);
}

TNodePtr BuildRevokePermissions(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TVector<TDeferredAtom>& permissions, const TVector<TDeferredAtom>& schemaPaths, const TVector<TDeferredAtom>& roleNames, TScopedStatePtr scoped) {
    return new TPermissionsAction(pos,
                                  service,
                                  cluster,
                                  {.PermissionAction = "revoke",
                                   .Permissions = permissions,
                                   .SchemaPaths = schemaPaths,
                                   .RoleNames = roleNames},
                                  scoped);
}

} // namespace NSQLTranslationV1
