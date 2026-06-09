#include "common.h"

namespace NKikimr::NMetadata::NInitializer {

ITableModifier::TPtr TACLModifierConstructor::BuildModifier() const {
    return std::make_shared<TGenericTableModifier<NRequest::TDialogModifyPermissions>>(Request, Id);
}

TACLModifierConstructor TACLModifierConstructor::GetNoAccessModifier(const TString& path, const TString& id) {
    TACLModifierConstructor result(path, id);
    result->set_clear_permissions(true);
    result->set_interrupt_inheritance(true);
    return result;
}

TACLModifierConstructor TACLModifierConstructor::GetReadOnlyModifier(const TString& path, const TString& id) {
    TACLModifierConstructor result(path, id);
    result->set_clear_permissions(true);
    result->set_interrupt_inheritance(true);
    const TString subject = AppData()->AllAuthenticatedUsers ? AppData()->AllAuthenticatedUsers : "USERS";
    auto* readPermission = result->add_actions();
    readPermission->mutable_grant()->set_subject(subject);
    readPermission->mutable_grant()->add_permission_names("ydb.tables.read");
    auto* describePermission = result->add_actions();
    describePermission->mutable_grant()->set_subject(subject);
    describePermission->mutable_grant()->add_permission_names("ydb.deprecated.describe_schema");
    return result;
}

}
