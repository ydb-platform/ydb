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
    std::vector<std::string> Subjects;
    NYTree::EPermissionSet Permissions;
    EAceInheritanceMode InheritanceMode = EAceInheritanceMode::ObjectAndDescendants;
    std::optional<TBooleanFormula> SubjectTagFilter;
    std::optional<std::vector<std::string>> Columns;
    std::optional<bool> Vital;
    std::optional<std::string> Expression;
    std::optional<EInapplicableExpressionMode> InapplicableExpressionMode;

    TSerializableAccessControlEntry(
        ESecurityAction action,
        std::vector<std::string> subjects,
        NYTree::EPermissionSet permissions,
        EAceInheritanceMode inheritanceMode = EAceInheritanceMode::ObjectAndDescendants);

    // Use only for deserialization.
    TSerializableAccessControlEntry();

    // Used only for persistence in operation controller. Does not work with Columns and Vital fields.
    void Persist(const TStreamPersistenceContext& context);

    bool operator==(const TSerializableAccessControlEntry& other) const = default;

    static constexpr TStringBuf ExpressionKey = "expression";
    static constexpr TStringBuf InapplicableExpressionModeKey = "inapplicable_expression_mode";
};

void Serialize(const TSerializableAccessControlEntry& ace, NYson::IYsonConsumer* consumer);
void Deserialize(TSerializableAccessControlEntry& ace, NYTree::INodePtr node);
void Deserialize(TSerializableAccessControlEntry& ace, NYson::TYsonPullParserCursor* cursor);

template <class TAce>
[[nodiscard]] TError CheckAceCorrect(const TAce& ace);

struct TSerializableAccessControlList
{
    std::vector<TSerializableAccessControlEntry> Entries;

    void Persist(const TStreamPersistenceContext& context);
};

bool operator == (const TSerializableAccessControlList& lhs, const TSerializableAccessControlList& rhs);

void Serialize(const TSerializableAccessControlList& acl, NYson::IYsonConsumer* consumer);
void Deserialize(TSerializableAccessControlList& acl, NYTree::INodePtr node);
void Deserialize(TSerializableAccessControlList& acl, NYson::TYsonPullParserCursor* cursor);

//! A small container to allow reader to selectively apply expressions based on
//! their InapplicableExpressionMode.
//! NB: You may encounter occurences of an "RL ACE" term.
//! It stands for Row-Level Access Control Entry.
struct TRowLevelAccessControlEntry
{
    std::string Expression;
    EInapplicableExpressionMode InapplicableExpressionMode;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient

#define ACL_H
#include "acl-inl.h"
#undef ACL_H
