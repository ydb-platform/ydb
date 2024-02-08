#include "acl.h"

#include <yt/yt/core/yson/pull_parser_deserialize.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/serialize.h>

namespace NYT::NSecurityClient {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TSerializableAccessControlEntry::TSerializableAccessControlEntry() = default;

TSerializableAccessControlEntry::TSerializableAccessControlEntry(
    ESecurityAction action,
    std::vector<TString> subjects,
    EPermissionSet permissions,
    EAceInheritanceMode inheritanceMode)
    : Action(action)
    , Subjects(std::move(subjects))
    , Permissions(permissions)
    , InheritanceMode(inheritanceMode)
{ }

// NB(levysotsky): We don't use TYsonStruct here
// because we want to mirror the TAccessControlList structure,
// and a vector of TYsonStruct-s cannot be declared (as it has no move constructor).
void Serialize(const TSerializableAccessControlEntry& ace, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("action").Value(ace.Action)
            .Item("subjects").Value(ace.Subjects)
            .Item("permissions").Value(ace.Permissions)
            // TODO(max42): YT-16347.
            // Do not serialize this field by default
            .Item("inheritance_mode").Value(ace.InheritanceMode)
            .OptionalItem("subject_tag_filter", ace.SubjectTagFilter)
            .OptionalItem("columns", ace.Columns)
            .OptionalItem("vital", ace.Vital)
        .EndMap();
}

static void EnsureCorrect(const TSerializableAccessControlEntry& ace)
{
    if (ace.Action == ESecurityAction::Undefined) {
        THROW_ERROR_EXCEPTION("%Qlv action is not allowed",
            ESecurityAction::Undefined);
    }

    // Currently, we allow empty permissions with columns. They seem to be no-op.
    bool onlyReadOrEmpty = None(ace.Permissions & ~EPermission::Read);
    if (ace.Columns && !onlyReadOrEmpty) {
        THROW_ERROR_EXCEPTION("ACE specifying columns may contain only %Qlv permission; found %Qlv",
            EPermission::Read,
            ace.Permissions);
    }

    bool hasRegisterQueueConsumer = Any(ace.Permissions & EPermission::RegisterQueueConsumer);
    bool onlyRegisterQueueConsumer = ace.Permissions == EPermission::RegisterQueueConsumer;

    if (hasRegisterQueueConsumer && !ace.Vital) {
        THROW_ERROR_EXCEPTION("Permission %Qlv requires vitality to be specified",
            EPermission::RegisterQueueConsumer);
    }
    if (ace.Vital && !onlyRegisterQueueConsumer) {
        THROW_ERROR_EXCEPTION("ACE specifying vitality must contain a single %Qlv permission; found %Qlv",
            EPermission::RegisterQueueConsumer,
            ace.Permissions);
    }
}

void Deserialize(TSerializableAccessControlEntry& ace, NYTree::INodePtr node)
{
    using NYTree::Deserialize;

    auto mapNode = node->AsMap();

    Deserialize(ace.Action, mapNode->GetChildOrThrow("action"));
    Deserialize(ace.Subjects, mapNode->GetChildOrThrow("subjects"));
    Deserialize(ace.Permissions, mapNode->GetChildOrThrow("permissions"));
    if (auto inheritanceModeNode = mapNode->FindChild("inheritance_mode")) {
        Deserialize(ace.InheritanceMode, inheritanceModeNode);
    } else {
        ace.InheritanceMode = EAceInheritanceMode::ObjectAndDescendants;
    }
    if (auto tagFilterNode = mapNode->FindChild("subject_tag_filter")) {
        Deserialize(ace.SubjectTagFilter, tagFilterNode);
    } else {
        ace.SubjectTagFilter = {};
    }
    if (auto columnsNode = mapNode->FindChild("columns")) {
        Deserialize(ace.Columns, columnsNode);
    } else {
        ace.Columns.reset();
    }
    if (auto vitalNode = mapNode->FindChild("vital")) {
        Deserialize(ace.Vital, vitalNode);
    } else {
        ace.Vital.reset();
    }
    EnsureCorrect(ace);
}

void Deserialize(TSerializableAccessControlEntry& ace, NYson::TYsonPullParserCursor* cursor)
{
    auto HasAction = false;
    auto HasSubjects = false;
    auto HasPermissions = false;
    ace.InheritanceMode = EAceInheritanceMode::ObjectAndDescendants;
    ace.SubjectTagFilter = {};
    cursor->ParseMap([&] (TYsonPullParserCursor* cursor) {
        auto key = cursor->GetCurrent().UncheckedAsString();
        if (key == TStringBuf("action")) {
            cursor->Next();
            HasAction = true;
            Deserialize(ace.Action, cursor);
        } else if (key == TStringBuf("subjects")) {
            cursor->Next();
            HasSubjects = true;
            Deserialize(ace.Subjects, cursor);
        } else if (key == TStringBuf("permissions")) {
            cursor->Next();
            HasPermissions = true;
            Deserialize(ace.Permissions, cursor);
        } else if (key == TStringBuf("inheritance_mode")) {
            cursor->Next();
            Deserialize(ace.InheritanceMode, cursor);
        } else if (key == TStringBuf("subject_tag_filter")) {
            cursor->Next();
            Deserialize(ace.SubjectTagFilter, cursor);
        } else if (key == TStringBuf("columns")) {
            cursor->Next();
            Deserialize(ace.Columns, cursor);
        } else if (key == TStringBuf("vital")) {
            cursor->Next();
            Deserialize(ace.Vital, cursor);
        } else {
            cursor->Next();
            cursor->SkipComplexValue();
        }
    });
    if (!(HasAction && HasSubjects && HasPermissions)) {
        THROW_ERROR_EXCEPTION("Error parsing ACE: \"action\", \"subject\" and \"permissions\" fields are required");
    }
    EnsureCorrect(ace);
}

void TSerializableAccessControlEntry::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Action);
    Persist(context, Subjects);
    Persist(context, Permissions);
    Persist(context, InheritanceMode);
    // COMPAT(vovamelnikov)
    if (context.IsLoad() && context.GetVersion() < 301305) {
        SubjectTagFilter =  {};
    } else {
        Persist(context, SubjectTagFilter);
    }
    // NB: Columns and Vital are not persisted since this method is intended only for use in controller.
}

bool operator == (const TSerializableAccessControlList& lhs, const TSerializableAccessControlList& rhs)
{
    return lhs.Entries == rhs.Entries;
}

void Serialize(const TSerializableAccessControlList& acl, NYson::IYsonConsumer* consumer)
{
    NYTree::Serialize(acl.Entries, consumer);
}

void Deserialize(TSerializableAccessControlList& acl, NYTree::INodePtr node)
{
    NYTree::Deserialize(acl.Entries, node);
}

void TSerializableAccessControlList::Persist(const TStreamPersistenceContext& context)
{
    NYT::Persist(context, Entries);
}

void Deserialize(TSerializableAccessControlList& acl, NYson::TYsonPullParserCursor* cursor)
{
    Deserialize(acl.Entries, cursor);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient
