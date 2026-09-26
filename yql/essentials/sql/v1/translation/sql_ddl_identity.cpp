#include "sql_ddl_identity.h"

#include "antlr_token.h"

namespace NSQLTranslationV1 {

bool TIdentityTranslation::PasswordParameter(const TRule_password_option& passwordOption, TUserParameters& result) {
    // password_option: ENCRYPTED? PASSWORD password_value;
    // password_value: STRING_VALUE | NULL;

    const auto& token = passwordOption.GetRule_password_value3().GetToken1();
    TString stringValue(Ctx_.Token(token));

    if (to_lower(stringValue) == "null") {
        result.IsPasswordNull = true;
    } else {
        auto password = StringContent(Ctx_, Ctx_.Pos(), stringValue);

        if (!password) {
            Error() << "Password should be enclosed into quotation marks.";
            return false;
        }

        result.Password = TDeferredAtom(Ctx_.Pos(), password->Content);
    }

    result.IsPasswordEncrypted = passwordOption.HasBlock1();

    return true;
}

bool TIdentityTranslation::HashParameter(const TRule_hash_option& hashOption, TUserParameters& result) {
    // hash_option: HASH STRING_VALUE;

    const auto& token = hashOption.GetToken2();
    TString stringValue(Ctx_.Token(token));

    auto hash = StringContent(Ctx_, Ctx_.Pos(), stringValue);

    if (!hash) {
        Error() << "Hash should be enclosed into quotation marks.";
        return false;
    }

    result.Hash = TDeferredAtom(Ctx_.Pos(), hash->Content);

    return true;
}

void TIdentityTranslation::LoginParameter(const TRule_login_option& loginOption, std::optional<bool>& canLogin) {
    // login_option: LOGIN | NOLOGIN;

    auto token = loginOption.GetToken1().GetId();
    if (IS_TOKEN(token, LOGIN)) {
        canLogin = true;
    } else if (IS_TOKEN(token, NOLOGIN)) {
        canLogin = false;
    } else {
        YQL_ENSURE(false, "Unreachable");
    }
}

bool TIdentityTranslation::UserParameters(const std::vector<TRule_user_option>& optionsList, TUserParameters& result, bool isCreateUser) {
    enum class EUserOption {
        Login,
        Authentication
    };

    std::set<EUserOption> used;

    auto ParseUserOption = [&used, this](const TRule_user_option& option, TUserParameters& result) -> bool {
        // user_option: authentication_option | login_option;
        //      authentication_option: password_option | hash_option;

        switch (option.Alt_case()) {
            case TRule_user_option::kAltUserOption1: {
                if (used.contains(EUserOption::Authentication)) {
                    Error() << "Conflicting or redundant options";
                    return false;
                }

                used.insert(EUserOption::Authentication);

                const auto& authenticationOption = option.GetAlt_user_option1().GetRule_authentication_option1();

                switch (authenticationOption.Alt_case()) {
                    case TRule_authentication_option::kAltAuthenticationOption1: {
                        if (!PasswordParameter(authenticationOption.GetAlt_authentication_option1().GetRule_password_option1(), result)) {
                            return false;
                        }

                        break;
                    }
                    case TRule_authentication_option::kAltAuthenticationOption2: {
                        if (!HashParameter(authenticationOption.GetAlt_authentication_option2().GetRule_hash_option1(), result)) {
                            return false;
                        }

                        break;
                    }
                    case TRule_authentication_option::ALT_NOT_SET:
                        YQL_ENSURE(false, "Unreachable");
                }

                break;
            }
            case TRule_user_option::kAltUserOption2: {
                if (used.contains(EUserOption::Login)) {
                    Error() << "Conflicting or redundant options";
                    return false;
                }

                used.insert(EUserOption::Login);

                LoginParameter(option.GetAlt_user_option2().GetRule_login_option1(), result.CanLogin);

                break;
            }
            case TRule_user_option::ALT_NOT_SET:
                YQL_ENSURE(false, "Unreachable");
        }

        return true;
    };

    if (isCreateUser) {
        result.CanLogin = true;
        result.IsPasswordNull = true;
    }

    for (const auto& option : optionsList) {
        if (!ParseUserOption(option, result)) {
            return false;
        }
    }

    return true;
}

bool TIdentityTranslation::PermissionNameClause(const TRule_permission_id& node, TDeferredAtom& result) {
    // permission_id:
    //   CONNECT
    // | LIST
    // | INSERT
    // | MANAGE
    // | DROP
    // | GRANT
    // | MODIFY (TABLES | ATTRIBUTES)
    // | (UPDATE | ERASE) ROW
    // | (REMOVE | DESCRIBE | ALTER) SCHEMA
    // | SELECT (TABLES | ATTRIBUTES | ROW)?
    // | (USE | FULL) LEGACY?
    // | CREATE (DIRECTORY | TABLE | QUEUE)?

    auto handleOneIdentifier = [&result, this](const auto& permissionNameKeyword) {
        result = TDeferredAtom(Ctx_.Pos(), GetIdentifier(*this, permissionNameKeyword).Name);
    };

    auto handleTwoIdentifiers = [&result, this](const auto& permissionNameKeyword) {
        const auto& token1 = permissionNameKeyword.GetToken1();
        const auto& token2 = permissionNameKeyword.GetToken2();
        TString identifierName = TIdentifier(TPosition(token1.GetColumn(), token1.GetLine()), Identifier(token1)).Name +
                                 "_" +
                                 TIdentifier(TPosition(token2.GetColumn(), token2.GetLine()), Identifier(token2)).Name;
        result = TDeferredAtom(Ctx_.Pos(), identifierName);
    };

    auto handleOneOrTwoIdentifiers = [&result, this](const auto& permissionNameKeyword) {
        TString identifierName = GetIdentifier(*this, permissionNameKeyword).Name;
        if (permissionNameKeyword.HasBlock2()) {
            identifierName += "_" + GetIdentifier(*this, permissionNameKeyword.GetBlock2()).Name;
        }
        result = TDeferredAtom(Ctx_.Pos(), identifierName);
    };

    switch (node.GetAltCase()) {
        case TRule_permission_id::kAltPermissionId1: {
            // CONNECT
            handleOneIdentifier(node.GetAlt_permission_id1());
            break;
        }
        case TRule_permission_id::kAltPermissionId2: {
            // LIST
            handleOneIdentifier(node.GetAlt_permission_id2());
            break;
        }
        case TRule_permission_id::kAltPermissionId3: {
            // INSERT
            handleOneIdentifier(node.GetAlt_permission_id3());
            break;
        }
        case TRule_permission_id::kAltPermissionId4: {
            // MANAGE
            handleOneIdentifier(node.GetAlt_permission_id4());
            break;
        }
        case TRule_permission_id::kAltPermissionId5: {
            // DROP
            handleOneIdentifier(node.GetAlt_permission_id5());
            break;
        }
        case TRule_permission_id::kAltPermissionId6: {
            // GRANT
            handleOneIdentifier(node.GetAlt_permission_id6());
            break;
        }
        case TRule_permission_id::kAltPermissionId7: {
            // MODIFY (TABLES | ATTRIBUTES)
            handleTwoIdentifiers(node.GetAlt_permission_id7());
            break;
        }
        case TRule_permission_id::kAltPermissionId8: {
            // (UPDATE | ERASE) ROW
            handleTwoIdentifiers(node.GetAlt_permission_id8());
            break;
        }
        case TRule_permission_id::kAltPermissionId9: {
            // (REMOVE | DESCRIBE | ALTER) SCHEMA
            handleTwoIdentifiers(node.GetAlt_permission_id9());
            break;
        }
        case TRule_permission_id::kAltPermissionId10: {
            // SELECT (TABLES | ATTRIBUTES | ROW)?
            handleOneOrTwoIdentifiers(node.GetAlt_permission_id10());
            break;
        }
        case TRule_permission_id::kAltPermissionId11: {
            // (USE | FULL) LEGACY?
            handleOneOrTwoIdentifiers(node.GetAlt_permission_id11());
            break;
        }
        case TRule_permission_id::kAltPermissionId12: {
            // CREATE (DIRECTORY | TABLE | QUEUE)?
            handleOneOrTwoIdentifiers(node.GetAlt_permission_id12());
            break;
        }
        case TRule_permission_id::ALT_NOT_SET:
            YQL_ENSURE(false, "Unreachable");
    }
    return true;
}

bool TIdentityTranslation::PermissionNameClause(const TRule_permission_name& node, TDeferredAtom& result) {
    // permission_name: permission_id | STRING_VALUE;
    switch (node.Alt_case()) {
        case TRule_permission_name::kAltPermissionName1: {
            return PermissionNameClause(node.GetAlt_permission_name1().GetRule_permission_id1(), result);
            break;
        }
        case TRule_permission_name::kAltPermissionName2: {
            const TString stringValue(Ctx_.Token(node.GetAlt_permission_name2().GetToken1()));
            auto unescaped = StringContent(Ctx_, Ctx_.Pos(), stringValue);
            if (!unescaped) {
                return false;
            }
            result = TDeferredAtom(Ctx_.Pos(), unescaped->Content);
            break;
        }
        case TRule_permission_name::ALT_NOT_SET:
            YQL_ENSURE(false, "Unreachable");
    }
    return true;
}

bool TIdentityTranslation::PermissionNameClause(const TRule_permission_name_target& node, TVector<TDeferredAtom>& result, bool withGrantOption) {
    // permission_name_target: permission_name (COMMA permission_name)* COMMA? | ALL PRIVILEGES?;
    switch (node.Alt_case()) {
        case TRule_permission_name_target::kAltPermissionNameTarget1: {
            const auto& permissionNameRule = node.GetAlt_permission_name_target1();
            result.emplace_back();
            if (!PermissionNameClause(permissionNameRule.GetRule_permission_name1(), result.back())) {
                return false;
            }
            for (const auto& item : permissionNameRule.GetBlock2()) {
                result.emplace_back();
                if (!PermissionNameClause(item.GetRule_permission_name2(), result.back())) {
                    return false;
                }
            }
            break;
        }
        case TRule_permission_name_target::kAltPermissionNameTarget2: {
            result.emplace_back(Ctx_.Pos(), "all_privileges");
            break;
        }
        case TRule_permission_name_target::ALT_NOT_SET:
            YQL_ENSURE(false, "Unreachable");
    }
    if (withGrantOption) {
        result.emplace_back(Ctx_.Pos(), "grant");
    }
    return true;
}

TNodePtr TIdentityTranslation::Build(const TRule_create_user_stmt& node) {
    // create_user_stmt: CREATE USER role_name (user_option)*;
    Ctx_.BodyPart();

    Ctx_.Token(node.GetToken1());
    const TPosition pos = Ctx_.Pos();

    TString service = Ctx_.Scoped->CurrService;
    TDeferredAtom cluster = Ctx_.Scoped->CurrCluster;
    if (cluster.Empty()) {
        Error() << "USE statement is missing - no default cluster is selected";
        return {};
    }

    TDeferredAtom roleName;
    bool allowSystemRoles = false;
    if (!RoleNameClause(node.GetRule_role_name3(), roleName, allowSystemRoles)) {
        return {};
    }

    TMaybe<TUserParameters> createUserParams;
    const auto& options = node.GetBlock4();

    createUserParams.ConstructInPlace();
    std::vector<TRule_user_option> opts;
    opts.reserve(options.size());
    for (const auto& opt : options) {
        opts.push_back(opt.GetRule_user_option1());
    }

    bool isCreateUser = true;
    if (!UserParameters(opts, *createUserParams, isCreateUser)) {
        return {};
    }

    return BuildControlUser(pos, service, cluster, roleName, createUserParams, Ctx_.Scoped, isCreateUser);
}

TNodePtr TIdentityTranslation::Build(const TRule_alter_user_stmt& node) {
    // alter_user_stmt: ALTER USER role_name (WITH? user_option+ | RENAME TO role_name);
    Ctx_.BodyPart();

    Ctx_.Token(node.GetToken1());
    const TPosition pos = Ctx_.Pos();

    TString service = Ctx_.Scoped->CurrService;
    TDeferredAtom cluster = Ctx_.Scoped->CurrCluster;
    if (cluster.Empty()) {
        Error() << "USE statement is missing - no default cluster is selected";
        return {};
    }

    TDeferredAtom roleName;
    {
        bool allowSystemRoles = true;
        if (!RoleNameClause(node.GetRule_role_name3(), roleName, allowSystemRoles)) {
            return {};
        }
    }

    TNodePtr stmt;
    switch (node.GetBlock4().Alt_case()) {
        case TRule_alter_user_stmt_TBlock4::kAlt1: {
            TUserParameters alterUserParams;

            auto options = node.GetBlock4().GetAlt1().GetBlock2();
            std::vector<TRule_user_option> opts;
            opts.reserve(options.size());
            for (const auto& opt : options) {
                opts.push_back(opt.GetRule_user_option1());
            }

            bool isCreateUser = false;
            if (!UserParameters(opts, alterUserParams, isCreateUser)) {
                return {};
            }
            stmt = BuildControlUser(pos, service, cluster, roleName, alterUserParams, Ctx_.Scoped, isCreateUser);
            break;
        }
        case TRule_alter_user_stmt_TBlock4::kAlt2: {
            TDeferredAtom tgtRoleName;
            bool allowSystemRoles = false;
            if (!RoleNameClause(node.GetBlock4().GetAlt2().GetRule_role_name3(), tgtRoleName, allowSystemRoles)) {
                return {};
            }
            stmt = BuildRenameUser(pos, service, cluster, roleName, tgtRoleName, Ctx_.Scoped);
            break;
        }
        case TRule_alter_user_stmt_TBlock4::ALT_NOT_SET:
            YQL_ENSURE(false, "Unreachable");
    }

    return stmt;
}

TNodePtr TIdentityTranslation::Build(const TRule_create_group_stmt& node) {
    // create_group_stmt: CREATE GROUP role_name (WITH USER role_name (COMMA role_name)* COMMA?)?;
    Ctx_.BodyPart();

    Ctx_.Token(node.GetToken1());
    const TPosition pos = Ctx_.Pos();

    TString service = Ctx_.Scoped->CurrService;
    TDeferredAtom cluster = Ctx_.Scoped->CurrCluster;
    if (cluster.Empty()) {
        Error() << "USE statement is missing - no default cluster is selected";
        return {};
    }

    TDeferredAtom roleName;
    bool allowSystemRoles = false;
    if (!RoleNameClause(node.GetRule_role_name3(), roleName, allowSystemRoles)) {
        return {};
    }

    TCreateGroupParameters createGroupParams;
    if (node.HasBlock4()) {
        auto& addDropNode = node.GetBlock4();
        bool allowSystemRoles = false;
        createGroupParams.Roles.emplace_back();
        if (!RoleNameClause(addDropNode.GetRule_role_name3(), createGroupParams.Roles.back(), allowSystemRoles)) {
            return {};
        }

        for (auto& item : addDropNode.GetBlock4()) {
            createGroupParams.Roles.emplace_back();
            if (!RoleNameClause(item.GetRule_role_name2(), createGroupParams.Roles.back(), allowSystemRoles)) {
                return {};
            }
        }
    }

    return BuildCreateGroup(pos, service, cluster, roleName, createGroupParams, Ctx_.Scoped);
}

TNodePtr TIdentityTranslation::Build(const TRule_alter_group_stmt& node) {
    // alter_group_stmt: ALTER GROUP role_name ((ADD|DROP) USER role_name (COMMA role_name)* COMMA? | RENAME TO role_name);
    Ctx_.BodyPart();

    Ctx_.Token(node.GetToken1());
    const TPosition pos = Ctx_.Pos();

    TString service = Ctx_.Scoped->CurrService;
    TDeferredAtom cluster = Ctx_.Scoped->CurrCluster;
    if (cluster.Empty()) {
        Error() << "USE statement is missing - no default cluster is selected";
        return {};
    }

    TDeferredAtom roleName;
    {
        bool allowSystemRoles = true;
        if (!RoleNameClause(node.GetRule_role_name3(), roleName, allowSystemRoles)) {
            return {};
        }
    }

    TNodePtr stmt;
    switch (node.GetBlock4().Alt_case()) {
        case TRule_alter_group_stmt_TBlock4::kAlt1: {
            auto& addDropNode = node.GetBlock4().GetAlt1();
            const bool isDrop = IS_TOKEN(addDropNode.GetToken1().GetId(), DROP);
            TVector<TDeferredAtom> roles;
            bool allowSystemRoles = false;
            roles.emplace_back();
            if (!RoleNameClause(addDropNode.GetRule_role_name3(), roles.back(), allowSystemRoles)) {
                return {};
            }

            for (auto& item : addDropNode.GetBlock4()) {
                roles.emplace_back();
                if (!RoleNameClause(item.GetRule_role_name2(), roles.back(), allowSystemRoles)) {
                    return {};
                }
            }

            stmt = BuildAlterGroup(pos, service, cluster, roleName, roles, isDrop, Ctx_.Scoped);
            break;
        }
        case TRule_alter_group_stmt_TBlock4::kAlt2: {
            TDeferredAtom tgtRoleName;
            bool allowSystemRoles = false;
            if (!RoleNameClause(node.GetBlock4().GetAlt2().GetRule_role_name3(), tgtRoleName, allowSystemRoles)) {
                return {};
            }
            stmt = BuildRenameGroup(pos, service, cluster, roleName, tgtRoleName, Ctx_.Scoped);
            break;
        }
        case TRule_alter_group_stmt_TBlock4::ALT_NOT_SET:
            YQL_ENSURE(false, "Unreachable");
    }

    return stmt;
}

TNodePtr TIdentityTranslation::Build(const TRule_drop_role_stmt& node) {
    // drop_role_stmt: DROP (USER|GROUP) (IF EXISTS)? role_name (COMMA role_name)* COMMA?;
    Ctx_.BodyPart();

    Ctx_.Token(node.GetToken1());
    const TPosition pos = Ctx_.Pos();

    TString service = Ctx_.Scoped->CurrService;
    TDeferredAtom cluster = Ctx_.Scoped->CurrCluster;
    if (cluster.Empty()) {
        Error() << "USE statement is missing - no default cluster is selected";
        return {};
    }

    const bool isUser = IS_TOKEN(node.GetToken2().GetId(), USER);
    bool missingOk = false;
    if (node.HasBlock3()) { // IF EXISTS
        missingOk = true;
        Y_DEBUG_ABORT_UNLESS(
            IS_TOKEN(node.GetBlock3().GetToken1().GetId(), IF) &&
            IS_TOKEN(node.GetBlock3().GetToken2().GetId(), EXISTS));
    }

    TVector<TDeferredAtom> roles;
    bool allowSystemRoles = true;
    roles.emplace_back();
    if (!RoleNameClause(node.GetRule_role_name4(), roles.back(), allowSystemRoles)) {
        return {};
    }

    for (auto& item : node.GetBlock5()) {
        roles.emplace_back();
        if (!RoleNameClause(item.GetRule_role_name2(), roles.back(), allowSystemRoles)) {
            return {};
        }
    }

    return BuildDropRoles(pos, service, cluster, roles, isUser, missingOk, Ctx_.Scoped);
}

TNodePtr TIdentityTranslation::Build(const TRule_grant_permissions_stmt& node) {
    // GRANT permission_name_target ON an_id_schema (COMMA an_id_schema)* TO role_name (COMMA role_name)* COMMA? (WITH GRANT OPTION)?;
    Ctx_.BodyPart();

    Ctx_.Token(node.GetToken1());
    const TPosition pos = Ctx_.Pos();

    TString service = Ctx_.Scoped->CurrService;
    TDeferredAtom cluster = Ctx_.Scoped->CurrCluster;
    if (cluster.Empty()) {
        Error() << "USE statement is missing - no default cluster is selected";
        return {};
    }

    TVector<TDeferredAtom> permissions;
    if (!PermissionNameClause(node.GetRule_permission_name_target2(), permissions, node.has_block10())) {
        return {};
    }

    TVector<TDeferredAtom> schemaPaths;
    schemaPaths.emplace_back(Ctx_.Pos(), Id(node.GetRule_an_id_schema4(), *this));
    for (const auto& item : node.GetBlock5()) {
        schemaPaths.emplace_back(Ctx_.Pos(), Id(item.GetRule_an_id_schema2(), *this));
    }

    TVector<TDeferredAtom> roleNames;
    const bool allowSystemRoles = false;
    roleNames.emplace_back();
    if (!RoleNameClause(node.GetRule_role_name7(), roleNames.back(), allowSystemRoles)) {
        return {};
    }
    for (const auto& item : node.GetBlock8()) {
        roleNames.emplace_back();
        if (!RoleNameClause(item.GetRule_role_name2(), roleNames.back(), allowSystemRoles)) {
            return {};
        }
    }

    return BuildGrantPermissions(pos, service, cluster, permissions, schemaPaths, roleNames, Ctx_.Scoped);
}

TNodePtr TIdentityTranslation::Build(const TRule_revoke_permissions_stmt& node) {
    // REVOKE (GRANT OPTION FOR)? permission_name_target ON an_id_schema (COMMA an_id_schema)* FROM role_name (COMMA role_name)*;
    Ctx_.BodyPart();

    Ctx_.Token(node.GetToken1());
    const TPosition pos = Ctx_.Pos();

    TString service = Ctx_.Scoped->CurrService;
    TDeferredAtom cluster = Ctx_.Scoped->CurrCluster;
    if (cluster.Empty()) {
        Error() << "USE statement is missing - no default cluster is selected";
        return {};
    }

    TVector<TDeferredAtom> permissions;
    if (!PermissionNameClause(node.GetRule_permission_name_target3(), permissions, node.HasBlock2())) {
        return {};
    }

    TVector<TDeferredAtom> schemaPaths;
    schemaPaths.emplace_back(Ctx_.Pos(), Id(node.GetRule_an_id_schema5(), *this));
    for (const auto& item : node.GetBlock6()) {
        schemaPaths.emplace_back(Ctx_.Pos(), Id(item.GetRule_an_id_schema2(), *this));
    }

    TVector<TDeferredAtom> roleNames;
    const bool allowSystemRoles = false;
    roleNames.emplace_back();
    if (!RoleNameClause(node.GetRule_role_name8(), roleNames.back(), allowSystemRoles)) {
        return {};
    }
    for (const auto& item : node.GetBlock9()) {
        roleNames.emplace_back();
        if (!RoleNameClause(item.GetRule_role_name2(), roleNames.back(), allowSystemRoles)) {
            return {};
        }
    }

    return BuildRevokePermissions(pos, service, cluster, permissions, schemaPaths, roleNames, Ctx_.Scoped);
}

} // namespace NSQLTranslationV1
