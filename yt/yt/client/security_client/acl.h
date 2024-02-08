#pragma once

#include "public.h"

#include <yt/yt/core/misc/arithmetic_formula.h>

#include <yt/yt/core/yson/consumer.h>

#include <yt/yt/core/ytree/permission.h>

#include <vector>

namespace NYT::NSecurityClient {

////////////////////////////////////////////////////////////////////////////////

struct TSerializableAccessControlEntry
{
    ESecurityAction Action = ESecurityAction::Undefined;
    std::vector<TString> Subjects;
    NYTree::EPermissionSet Permissions;
    EAceInheritanceMode InheritanceMode = EAceInheritanceMode::ObjectAndDescendants;
    TString SubjectTagFilter;
    std::optional<std::vector<TString>> Columns;
    std::optional<bool> Vital;

    TSerializableAccessControlEntry(
        ESecurityAction action,
        std::vector<TString> subjects,
        NYTree::EPermissionSet permissions,
        EAceInheritanceMode inheritanceMode = EAceInheritanceMode::ObjectAndDescendants);

    // Use only for deserialization.
    TSerializableAccessControlEntry();

    // Used only for persistence in operation controller. Does not work with Columns and Vital fields.
    void Persist(const TStreamPersistenceContext& context);

    bool operator==(const TSerializableAccessControlEntry& other) const = default;
};

void Serialize(const TSerializableAccessControlEntry& ace, NYson::IYsonConsumer* consumer);
void Deserialize(TSerializableAccessControlEntry& ace, NYTree::INodePtr node);
void Deserialize(TSerializableAccessControlEntry& ace, NYson::TYsonPullParserCursor* cursor);

struct TSerializableAccessControlList
{
    std::vector<TSerializableAccessControlEntry> Entries;

    void Persist(const TStreamPersistenceContext& context);
};

bool operator == (const TSerializableAccessControlList& lhs, const TSerializableAccessControlList& rhs);

void Serialize(const TSerializableAccessControlList& acl, NYson::IYsonConsumer* consumer);
void Deserialize(TSerializableAccessControlList& acl, NYTree::INodePtr node);
void Deserialize(TSerializableAccessControlList& acl, NYson::TYsonPullParserCursor* cursor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient
