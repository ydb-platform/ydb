#pragma once

#include "public.h"

#include <yt/yt/core/misc/arithmetic_formula.h>

#include <yt/yt/core/phoenix/context.h>
#include <yt/yt/core/phoenix/type_decl.h>

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
    std::optional<std::string> RowAccessPredicate;
    std::optional<EInapplicableRowAccessPredicateMode> InapplicableRowAccessPredicateMode;

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

    static constexpr TStringBuf RowAccessPredicateKey = "row_access_predicate";
    static constexpr TStringBuf InapplicableRowAccessPredicateModeKey = "inapplicable_row_access_predicate_mode";
};

void Serialize(const TSerializableAccessControlEntry& ace, NYson::IYsonConsumer* consumer);
void Deserialize(TSerializableAccessControlEntry& ace, NYTree::INodePtr node);
void Deserialize(TSerializableAccessControlEntry& ace, NYson::TYsonPullParserCursor* cursor);

template <class TAce>
TError CheckAceCorrect(const TAce& ace);

template <class TAce>
void ValidateAceCorrect(const TAce& ace);

struct TSerializableAccessControlList
{
    std::vector<TSerializableAccessControlEntry> Entries;

    void Persist(const TStreamPersistenceContext& context);
};

bool operator==(const TSerializableAccessControlList& lhs, const TSerializableAccessControlList& rhs);

void Serialize(const TSerializableAccessControlList& acl, NYson::IYsonConsumer* consumer);
void Deserialize(TSerializableAccessControlList& acl, NYTree::INodePtr node);
void Deserialize(TSerializableAccessControlList& acl, NYson::TYsonPullParserCursor* cursor);

//! A small container to allow reader to selectively apply row access predicates based on
//! their InapplicableRowAccessPredicateMode.
//! NB: You may encounter occurences of an "RL ACE" term.
//! It stands for Row-Level Access Control Entry.
struct TRowLevelAccessControlEntry
{
    std::string RowAccessPredicate;
    EInapplicableRowAccessPredicateMode InapplicableRowAccessPredicateMode = EInapplicableRowAccessPredicateMode::Fail;

    using TLoadContext = NPhoenix::TLoadContext;
    using TSaveContext = NPhoenix::TSaveContext;

    PHOENIX_DECLARE_TYPE(TRowLevelAccessControlEntry, 0x01201ace);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient

#define ACL_H
#include "acl-inl.h"
#undef ACL_H
