#pragma once

#include "ddl_identity.h"
#include "sql_translation.h"

namespace NSQLTranslationV1 {

class TIdentityTranslation final: public TSqlTranslation {
public:
    TIdentityTranslation(TContext& ctx, NSQLTranslation::ESqlMode mode)
        : TSqlTranslation(ctx, mode)
    {
    }

    TNodePtr Build(const TRule_create_user_stmt& node);
    TNodePtr Build(const TRule_alter_user_stmt& node);
    TNodePtr Build(const TRule_create_group_stmt& node);
    TNodePtr Build(const TRule_alter_group_stmt& node);
    TNodePtr Build(const TRule_drop_role_stmt& node);
    TNodePtr Build(const TRule_grant_permissions_stmt& node);
    TNodePtr Build(const TRule_revoke_permissions_stmt& node);

private:
    bool PasswordParameter(const TRule_password_option& passwordOption, TUserParameters& result);
    bool HashParameter(const TRule_hash_option& hashOption, TUserParameters& result);
    void LoginParameter(const TRule_login_option& loginOption, std::optional<bool>& canLogin);
    bool UserParameters(const std::vector<TRule_user_option>& optionsList, TUserParameters& result, bool isCreateUser);
    bool PermissionNameClause(const TRule_permission_name_target& node, TVector<TDeferredAtom>& result, bool withGrantOption);
    bool PermissionNameClause(const TRule_permission_name& node, TDeferredAtom& result);
    bool PermissionNameClause(const TRule_permission_id& node, TDeferredAtom& result);
};

} // namespace NSQLTranslationV1
