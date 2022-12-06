#include "common.h"

namespace NKikimr::NMetadataInitializer {

ITableModifier::TPtr TACLModifierConstructor::BuildModifier() const {
    return std::make_shared<NMetadataInitializer::TGenericTableModifier<NInternal::NRequest::TDialogModifyPermissions>>(Request, Id);
}

NKikimr::NMetadataInitializer::TACLModifierConstructor TACLModifierConstructor::GetNoAccessModifier(const TString& path, const TString& id) {
    TACLModifierConstructor result(path, id);
    result->set_clear_permissions(true);
    result->set_interrupt_inheritance(true);
    return result;
}

NKikimr::NMetadataInitializer::TACLModifierConstructor TACLModifierConstructor::GetReadOnlyModifier(const TString& path, const TString& id) {
    TACLModifierConstructor result(path, id);
    result->set_clear_permissions(true);
    result->set_interrupt_inheritance(true);
    auto* permission = result->add_actions();
    permission->mutable_grant()->set_subject(AppData()->AllAuthenticatedUsers ? AppData()->AllAuthenticatedUsers : "USERS");
    permission->mutable_grant()->add_permission_names("ydb.tables.read");
    permission->mutable_grant()->add_permission_names("ydb.deprecated.describe_schema");
    return result;
}

}
