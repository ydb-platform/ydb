#include "aclib.h"
#include <util/stream/str.h>
#include <util/string/vector.h>
#include <algorithm>
#include <util/string/split.h>
#include <library/cpp/protobuf/util/is_equal.h>

namespace NACLib {

std::pair<ui32, ui32>& operator |=(std::pair<ui32, ui32>& a, const std::pair<ui32, ui32>& b) {
    a.first |= b.first;
    a.second |= b.second;
    return a;
}

void TUserToken::SetGroupSIDs(const TVector<TString>& groupSIDs) {
    auto& hashTable = *MutableGroupSIDs();
    auto& hashBuckets = *hashTable.MutableBuckets();
    int size(groupSIDs.size()); // we are targeting for load factor of ~1.0
    hashBuckets.Reserve(size);
    for (int i = 0; i < size; ++i) {
        hashBuckets.Add();
    }
    for (const TSID& sid : groupSIDs) {
        int index = hash<TSID>()(sid) % size;
        hashBuckets.Mutable(index)->AddValues(sid);
    }
}

TUserToken::TUserToken(TUserToken::TUserTokenInitFields fields) {
    if (!fields.OriginalUserToken.empty()) {
        SetOriginalUserToken(fields.OriginalUserToken);
    }
    SetUserSID(fields.UserSID);
    SetGroupSIDs(fields.GroupSIDs);
    if (fields.AuthType) {
        SetAuthType(fields.AuthType);
    }
}

TUserToken::TUserToken(const TString& originalUserToken, const TSID& userSID, const TVector<TSID>& groupSIDs) {
    if (!originalUserToken.empty()) {
        SetOriginalUserToken(originalUserToken);
    }
    SetUserSID(userSID);
    SetGroupSIDs(groupSIDs);
}

TUserToken::TUserToken(const TSID& userSID, const TVector<TSID>& groupSIDs)
    : TUserToken(TString(), userSID, groupSIDs)
{}

TUserToken::TUserToken(const TVector<TSID>& userAndGroupSIDs)
    : TUserToken(TString(), GetUserFromVector(userAndGroupSIDs), GetGroupsFromVector(userAndGroupSIDs))
{}

TUserToken::TUserToken(const NACLibProto::TUserToken& token)
    :NACLibProto::TUserToken(token)
{}

TUserToken::TUserToken(NACLibProto::TUserToken&& token) {
    Swap(&token);
}

TUserToken::TUserToken(const TString& token) {
    Y_ABORT_UNLESS(ParseFromString(token));
    Serialized_ = token;
}

const TString& TUserToken::GetSerializedToken() const {
    return Serialized_;
}

bool TUserToken::IsExist(const TSID& someSID) const {
    if (NACLibProto::TUserToken::GetUserSID() == someSID)
        return true;
    const auto& hashTable(NACLibProto::TUserToken::GetGroupSIDs());
    if (hashTable.BucketsSize() > 0) {
        int index = hash<TSID>()(someSID) % hashTable.BucketsSize();
        const auto& hashBucket(hashTable.GetBuckets(index).GetValues());
        for (const TSID& sid : hashBucket) {
            if (sid == someSID)
                return true;
        }
    }
    return false;
}

TSID TUserToken::GetUserSID() const {
    return NACLibProto::TUserToken::GetUserSID();
}

TVector<TSID> TUserToken::GetGroupSIDs() const {
    TVector<TSID> groupSIDs;
    for (const auto& bucket : NACLibProto::TUserToken::GetGroupSIDs().GetBuckets()) {
        for (const TString& value : bucket.GetValues()) {
            groupSIDs.emplace_back(value);
        }
    }
    return groupSIDs;
}

TString TUserToken::GetOriginalUserToken() const {
    return NACLibProto::TUserToken::GetOriginalUserToken();
}

TString TUserToken::SerializeAsString() const {
    return NACLibProto::TUserToken::SerializeAsString();
}

void TUserToken::SaveSerializationInfo() {
    Serialized_ = SerializeAsString();
}

TSID TUserToken::GetUserFromVector(const TVector<TSID>& userAndGroupSIDs) {
    return userAndGroupSIDs.empty() ? TSID() : userAndGroupSIDs.front();
}

TVector<TSID> TUserToken::GetGroupsFromVector(const TVector<TSID>& userAndGroupSIDs) {
    return (userAndGroupSIDs.size() < 2) ? TVector<TSID>() : TVector<TSID>(std::next(userAndGroupSIDs.begin()), userAndGroupSIDs.end());
}

void TUserToken::AddGroupSID(const TSID& groupSID) {
    if (NACLibProto::TUserToken::GetGroupSIDs().BucketsSize() == 0) {
        MutableGroupSIDs()->AddBuckets();
    }
    int index = hash<TSID>()(groupSID) % NACLibProto::TUserToken::GetGroupSIDs().BucketsSize();
    auto& bucket = *MutableGroupSIDs()->MutableBuckets(index);
    for (const TString& value : bucket.GetValues()) {
        if (value == groupSID) {
            return;
        }
    }
    bucket.AddValues(groupSID);
}

bool TUserToken::IsSystemUser() const {
    return GetUserSID().EndsWith("@" BUILTIN_SYSTEM_DOMAIN);
}

TSecurityObject::TSecurityObject(const NACLibProto::TSecurityObject& protoSecObj, bool isContainer)
    : NACLibProto::TSecurityObject(protoSecObj)
    , IsContainer(isContainer)
{}

TSecurityObject::TSecurityObject(const TSID& owner, bool isContainer)
    : IsContainer(isContainer)
{
    SetOwnerSID(owner);
}

ui32 TSecurityObject::GetEffectiveAccessRights(const TUserToken& user) const {
    if (user.IsSystemUser()) {
        return EAccessRights::GenericFull; // the system always has access
    }
    if (HasOwnerSID() && user.IsExist(GetOwnerSID()))
        return EAccessRights::GenericFull; // the owner always has access
    ui32 deniedAccessRights = EAccessRights::NoAccess;
    ui32 allowedAccessRights = EAccessRights::NoAccess;
    if (HasACL()) {
        for (const NACLibProto::TACE& ace : GetACL().GetACE()) {
            if ((ace.GetInheritanceType() & EInheritanceType::InheritOnly) == 0) {
                if (user.IsExist(ace.GetSID())) {
                    switch(static_cast<EAccessType>(ace.GetAccessType())) {
                    case EAccessType::Deny:
                        deniedAccessRights |= ace.GetAccessRight();
                        break;
                    case EAccessType::Allow:
                        allowedAccessRights |= ace.GetAccessRight();
                        break;
                    }
                }
            }
        }
    }
    return allowedAccessRights & (~deniedAccessRights);
}

bool TSecurityObject::CheckAccess(ui32 access, const TUserToken& user) const {
    if (user.IsSystemUser()) {
        return true; // the system always has access
    }
    if (HasOwnerSID() && user.IsExist(GetOwnerSID()))
        return true; // the owner always has access
    if (HasACL()) {
        ui32 accessRightsLeft = access;
        for (const NACLibProto::TACE& ace : GetACL().GetACE()) {
            if ((ace.GetInheritanceType() & EInheritanceType::InheritOnly) == 0) {
                if (user.IsExist(ace.GetSID())) {
                    switch(static_cast<EAccessType>(ace.GetAccessType())) {
                    case EAccessType::Deny:
                        if (access && ace.GetAccessRight() != 0)
                            return false; // deny entries have precedence over allow entries
                        break;
                    case EAccessType::Allow:
                        accessRightsLeft &= ~(accessRightsLeft & ace.GetAccessRight()); // some rights are allowed
                        break;
                    }
                    if (accessRightsLeft == 0) // all rights have been allowed
                        return true;
                }
            }
        }
    }
    return false; // empty ACL is always-deny ACL
}

bool TSecurityObject::CheckGrantAccess(const NACLibProto::TDiffACL& diffACL, const TUserToken& user) const Y_NO_SANITIZE("undefined") {
    ui32 effectiveAccess = GetEffectiveAccessRights(user);
    NACLibProto::TACL copyACL = GetACL();
    std::pair<ui32, ui32> modifiedAccess = static_cast<TACL&>(copyACL).ApplyDiff(diffACL);
    return ((modifiedAccess.first & effectiveAccess) == modifiedAccess.first)
            && ((modifiedAccess.second & effectiveAccess) == modifiedAccess.second);
}

TSecurityObject TSecurityObject::MergeWithParent(const NACLibProto::TSecurityObject& parent) const {
    if (GetACL().GetInterruptInheritance()) {
        return *this;
    }
    TSecurityObject result(NACLibProto::TSecurityObject(), IsContainer);
    if (HasOwnerSID()) {
        result.SetOwnerSID(GetOwnerSID());
    }
    auto inheritance = IsContainer ? EInheritanceType::InheritContainer : EInheritanceType::InheritObject;
    if (parent.HasACL()) {
        for (const NACLibProto::TACE& ace : parent.GetACL().GetACE()) {
            if (static_cast<EAccessType>(ace.GetAccessType()) == EAccessType::Deny
                    && ((ace.GetInheritanceType() & inheritance) != 0)) {
                NACLibProto::TACE& resultACE(*result.MutableACL()->AddACE());
                resultACE.CopyFrom(ace);
                resultACE.SetInherited(true);
                resultACE.SetInheritanceType(resultACE.GetInheritanceType() & ~EInheritanceType::InheritOnly);
            }
        }
    }
    {
        for (const NACLibProto::TACE& ace : GetACL().GetACE()) {
            if (static_cast<EAccessType>(ace.GetAccessType()) == EAccessType::Deny) {
                result.MutableACL()->AddACE()->CopyFrom(ace);
            }
        }
    }
    if (parent.HasACL()) {
        for (const NACLibProto::TACE& ace : parent.GetACL().GetACE()) {
            if (static_cast<EAccessType>(ace.GetAccessType()) != EAccessType::Deny
                    && (ace.GetInheritanceType() & inheritance) != 0) {
                NACLibProto::TACE& resultACE(*result.MutableACL()->AddACE());
                resultACE.CopyFrom(ace);
                resultACE.SetInherited(true);
                resultACE.SetInheritanceType(resultACE.GetInheritanceType() & ~EInheritanceType::InheritOnly);
            }
        }
    }
    {
        for (const NACLibProto::TACE& ace : GetACL().GetACE()) {
            if (static_cast<EAccessType>(ace.GetAccessType()) != EAccessType::Deny) {
                result.MutableACL()->AddACE()->CopyFrom(ace);
            }
        }
    }
    return result;
}

void TSecurityObject::AddAccess(NACLib::EAccessType type, ui32 access, const NACLib::TSID& sid, ui32 inheritance) Y_NO_SANITIZE("undefined") {
    static_cast<TACL*>(MutableACL())->AddAccess(type, access, sid, inheritance);
}

void TSecurityObject::RemoveAccess(NACLib::EAccessType type, ui32 access, const NACLib::TSID& sid, ui32 inheritance) Y_NO_SANITIZE("undefined") {
    static_cast<TACL*>(MutableACL())->RemoveAccess(type, access, sid, inheritance);
}

void TSecurityObject::ClearAccess() Y_NO_SANITIZE("undefined") {
    static_cast<TACL*>(MutableACL())->ClearAccess();
}

NACLibProto::TACL TSecurityObject::GetImmediateACL() const Y_NO_SANITIZE("undefined") {
    return static_cast<const TACL&>(GetACL()).GetImmediateACL();
}

TSecurityObject::TSecurityObject()
    : IsContainer(false)
{}

TInstant TSecurityObject::GetExpireTime() const {
    return TInstant::MilliSeconds(static_cast<const NACLibProto::TSecurityObject&>(*this).GetExpireTime());
}

TString TSecurityObject::ToString() const Y_NO_SANITIZE("undefined") {
    return static_cast<const TACL&>(GetACL()).ToString();
}

void TSecurityObject::FromString(const TString& string) Y_NO_SANITIZE("undefined") {
    static_cast<TACL*>(MutableACL())->FromString(string);
}

void TSecurityObject::ApplyDiff(const NACLibProto::TDiffACL& diffACL) Y_NO_SANITIZE("undefined") {
    static_cast<TACL*>(MutableACL())->ApplyDiff(diffACL);
}

bool TSecurityObject::operator ==(const NACLib::TSecurityObject& rhs) const {
    return IsContainer == rhs.IsContainer && NProtoBuf::IsEqual(*this, rhs);
}

bool TSecurityObject::operator !=(const NACLib::TSecurityObject& rhs) const {
    return !(*this == rhs);
}

std::pair<ui32, ui32> TACL::AddAccess(NACLib::EAccessType type, ui32 access, const NACLib::TSID& sid, ui32 inheritance) Y_NO_SANITIZE("undefined") {
    for (int i = 0; i < static_cast<int>(ACESize()); ++i) {
        const NACLibProto::TACE& ace(GetACE(i));
        if (ace.GetSID() == sid) {
            if (ace.GetAccessType() == static_cast<ui32>(type)
                    && ace.GetAccessRight() == access
                    && ace.GetInheritanceType() == inheritance) {
                return {};
            }
        }
    }
    NACLibProto::TACE* ace(AddACE());
    ace->SetAccessType(static_cast<ui32>(type));
    ace->SetAccessRight(access);
    ace->SetSID(sid);
    ace->SetInheritanceType(inheritance);
    SortACL();
    switch (type) {
    case NACLib::EAccessType::Allow:
        return std::pair<ui32, ui32>(access, NACLib::EAccessRights::NoAccess);
    case NACLib::EAccessType::Deny:
        return std::pair<ui32, ui32>(NACLib::EAccessRights::NoAccess, access);
    }
    return {};
}

std::pair<ui32, ui32> TACL::RemoveAccess(NACLib::EAccessType type, ui32 access, const NACLib::TSID& sid, ui32 inheritance) {
    NACLibProto::TACE filterACE;
    filterACE.SetAccessType(static_cast<ui32>(type));
    filterACE.SetAccessRight(static_cast<ui32>(access));
    filterACE.SetSID(sid);
    filterACE.SetInheritanceType(static_cast<ui32>(inheritance));
    return RemoveAccess(filterACE);
}

std::pair<ui32, ui32> TACL::ClearAccess() {
    NACLibProto::TACE filterACE;
    return RemoveAccess(filterACE);
}

std::pair<ui32, ui32> TACL::RemoveAccess(const NACLibProto::TACE& filter) {
    std::pair<ui32, ui32> modified = {};
    auto* ACL = MutableACE();
    auto matchACE = [&filter](const NACLibProto::TACE& ace) {
        if (filter.HasAccessType() && ace.GetAccessType() != filter.GetAccessType()) {
            return false;
        }
        if (filter.HasAccessRight() && ace.GetAccessRight() != filter.GetAccessRight()) {
            return false;
        }
        if (filter.HasSID() && ace.GetSID() != filter.GetSID()) {
            return false;
        }
        if (filter.HasInheritanceType() && ace.GetInheritanceType() != filter.GetInheritanceType()) {
            return false;
        }
        return true;
    };

    auto newEnd = std::remove_if(ACL->begin(), ACL->end(), std::move(matchACE));
    while (newEnd != ACL->end()) {
        switch (static_cast<NACLib::EAccessType>(ACL->rbegin()->GetAccessType())) {
        case NACLib::EAccessType::Allow:
            modified |= std::pair<ui32, ui32>(ACL->rbegin()->GetAccessRight(), NACLib::EAccessRights::NoAccess);
            break;
        case NACLib::EAccessType::Deny:
            modified |= std::pair<ui32, ui32>(NACLib::EAccessRights::NoAccess, ACL->rbegin()->GetAccessRight());
            break;
        }
        ACL->RemoveLast();
    }
    return modified;
}

void TACL::SortACL() {
    Sort(*(MutableACE()), [](const NACLibProto::TACE& a, const NACLibProto::TACE& b) -> bool { return a.GetAccessType() < b.GetAccessType(); });
}

TACL::TACL(const TString& string) {
    Y_ABORT_UNLESS(ParseFromString(string));
}

std::pair<ui32, ui32> TACL::ApplyDiff(const NACLibProto::TDiffACL& diffACL) Y_NO_SANITIZE("undefined") {
    std::pair<ui32, ui32> modified = {};
    if (diffACL.HasInterruptInheritance()) {
        SetInterruptInheritance(diffACL.GetInterruptInheritance());
    }
    for (const NACLibProto::TDiffACE& diffACE : diffACL.GetDiffACE()) {
        const NACLibProto::TACE& ace = diffACE.GetACE();
        switch (static_cast<EDiffType>(diffACE.GetDiffType())) {
        case EDiffType::Add:
            modified |= AddAccess(static_cast<NACLib::EAccessType>(ace.GetAccessType()), ace.GetAccessRight(), ace.GetSID(), ace.GetInheritanceType());
            break;
        case EDiffType::Remove:
            modified |= RemoveAccess(ace);
            break;
        }
    }
    return modified;
}

NACLibProto::TACL TACL::GetImmediateACL() const {
    NACLibProto::TACL immediateACL;
    for (const NACLibProto::TACE& ace : GetACE()) {
        if (!ace.GetInherited()) {
            immediateACL.AddACE()->CopyFrom(ace);
        }
    }
    return immediateACL;
}

TString TACL::ToString(const NACLibProto::TACE& ace) {
    TStringStream str;
    switch (static_cast<EAccessType>(ace.GetAccessType())) {
        case EAccessType::Allow:
            str << '+';
            break;
        case EAccessType::Deny:
            str << '-';
            break;
        default:
            str << '?';
            break;
    }
    auto ar = ace.GetAccessRight();
    switch (ar) {
        case EAccessRights::GenericList:
            str << 'L';
            break;
        case EAccessRights::GenericRead:
            str << 'R';
            break;
        case EAccessRights::GenericWrite:
            str << 'W';
            break;
        case EAccessRights::GenericFullLegacy:
            str << "FL";
            break;
        case EAccessRights::GenericFull:
            str << 'F';
            break;
        case EAccessRights::GenericManage:
            str << 'M';
            break;
        case EAccessRights::GenericUseLegacy:
            str << "UL";
            break;
        case EAccessRights::GenericUse:
            str << 'U';
            break;
        default: {
            TVector<TStringBuf> rights;
            if (ar & EAccessRights::SelectRow)
                rights.emplace_back("SR");
            if (ar & EAccessRights::UpdateRow)
                rights.emplace_back("UR");
            if (ar & EAccessRights::EraseRow)
                rights.emplace_back("ER");
            if (ar & EAccessRights::ReadAttributes)
                rights.emplace_back("RA");
            if (ar & EAccessRights::WriteAttributes)
                rights.emplace_back("WA");
            if (ar & EAccessRights::CreateDirectory)
                rights.emplace_back("CD");
            if (ar & EAccessRights::CreateTable)
                rights.emplace_back("CT");
            if (ar & EAccessRights::CreateQueue)
                rights.emplace_back("CQ");
            if (ar & EAccessRights::RemoveSchema)
                rights.emplace_back("RS");
            if (ar & EAccessRights::DescribeSchema)
                rights.emplace_back("DS");
            if (ar & EAccessRights::AlterSchema)
                rights.emplace_back("AS");
            if (ar & EAccessRights::CreateDatabase)
                rights.emplace_back("CDB");
            if (ar & EAccessRights::DropDatabase)
                rights.emplace_back("DDB");
            if (ar & EAccessRights::GrantAccessRights)
                rights.emplace_back("GAR");
            if (ar & EAccessRights::WriteUserAttributes)
                rights.emplace_back("WUA");
            if (ar & EAccessRights::ConnectDatabase)
                rights.emplace_back("ConnDB");
            str << '(';
            for (auto jt = rights.begin(); jt != rights.end(); ++jt) {
                if (jt != rights.begin()) {
                    str << '|';
                }
                str << *jt;
            }
            str << ')';
            break;
        }
    }
    str << ':';
    str << ace.GetSID();
    auto inh = ace.GetInheritanceType();
    if (inh != (EInheritanceType::InheritContainer | EInheritanceType::InheritObject)) {
        str << ':';
        if (inh == EInheritanceType::InheritNone)
            str << '-';
        if (inh & EInheritanceType::InheritContainer)
            str << 'C';
        if (inh & EInheritanceType::InheritObject)
            str << 'O';
        if (inh & EInheritanceType::InheritOnly)
            str << '+';
    }
    return str.Str();
}

TString TACL::ToString() const {
    TStringStream str;
    bool inherited = false;
    const auto& acl = GetACE();
    for (auto it = acl.begin(); it != acl.end(); ++it) {
        const NACLibProto::TACE& ace = *it;
        if (it != acl.begin()) {
            str << ';';
        }
        if (inherited != ace.GetInherited()) {
            if (inherited) {
                str << '}';
            } else {
                str << '{';
            }
            inherited = ace.GetInherited();
        }
        str << ToString(ace);
    }
    if (inherited) {
        str << '}';
    }
    return str.Str();
}

ui32 TACL::SpecialRightsFromString(const TString& string) {
    TVector<TString> rights;
    ui32 result = 0;
    StringSplitter(string).Split('|').SkipEmpty().Collect(&rights);
    for (const TString& r : rights) {
        if (r == "SR")
            result |= EAccessRights::SelectRow;
        if (r == "UR")
            result |= EAccessRights::UpdateRow;
        if (r == "ER")
            result |= EAccessRights::EraseRow;
        if (r == "RA")
            result |= EAccessRights::ReadAttributes;
        if (r == "WA")
            result |= EAccessRights::WriteAttributes;
        if (r == "CD")
            result |= EAccessRights::CreateDirectory;
        if (r == "CT")
            result |= EAccessRights::CreateTable;
        if (r == "CQ")
            result |= EAccessRights::CreateQueue;
        if (r == "RS")
            result |= EAccessRights::RemoveSchema;
        if (r == "DS")
            result |= EAccessRights::DescribeSchema;
        if (r == "AS")
            result |= EAccessRights::AlterSchema;
        if (r == "CDB")
            result |= EAccessRights::CreateDatabase;
        if (r == "DDB")
            result |= EAccessRights::DropDatabase;
        if (r == "GAR")
            result |= EAccessRights::GrantAccessRights;
        if (r == "ConnDB")
            result |= EAccessRights::ConnectDatabase;
    }
    return result;
}

void TACL::FromString(NACLibProto::TACE& ace, const TString& string) {
    auto it = string.begin();
    switch (*it) {
        case '+':
            ace.SetAccessType(static_cast<ui32>(EAccessType::Allow));
            break;
        case '-':
            ace.SetAccessType(static_cast<ui32>(EAccessType::Deny));
            break;
        case '*':
            break;
        default:
            throw yexception() << "Invalid access type";
    }
    ++it;
    if (it == string.end()) {
        throw yexception() << "Invalid acl - no access rights";
    }
    switch (*it) {
        case 'L':
            ace.SetAccessRight(EAccessRights::GenericList);
            break;
        case 'R':
            ace.SetAccessRight(EAccessRights::GenericRead);
            break;
        case 'W':
            ace.SetAccessRight(EAccessRights::GenericWrite);
            break;
        case 'F':
            ++it;
            if (it != string.end() && *it == 'L') {
                ace.SetAccessRight(EAccessRights::GenericFullLegacy);
            } else {
                ace.SetAccessRight(EAccessRights::GenericFull);
                --it;
            }
            break;
        case 'M':
            ace.SetAccessRight(EAccessRights::GenericManage);
            break;
        case 'U':
            ++it;
            if (it != string.end() && *it == 'L') {
                ace.SetAccessRight(EAccessRights::GenericUseLegacy);
            } else {
                ace.SetAccessRight(EAccessRights::GenericUse);
                --it;
            }
            break;
        case '(': {
            ++it;
            TString specialRights = string.substr(it - string.begin(), string.find(')', it - string.begin()) - (it - string.begin()));
            ace.SetAccessRight(SpecialRightsFromString(specialRights));
            it = it + specialRights.size();
            break;
        }
        case '*':
            break;
        default:
            // TODO: more access rights
            throw yexception() << "Invalid access rights";
    }
    ++it;
    if (it == string.end()) {
        throw yexception() << "Invalid acl - no security id";
    }
    if (*it != ':') {
        throw yexception() << "Invalid acl - invalid format";
    }
    ++it;
    if (it == string.end()) {
        throw yexception() << "Invalid acl - no security id";
    }
    auto start_pos = it - string.begin();
    auto end_pos = string.find(':', start_pos);
    ace.SetSID(string.substr(start_pos, end_pos == TString::npos ? end_pos : end_pos - start_pos));
    if (end_pos == TString::npos) {
        ace.SetInheritanceType(EInheritanceType::InheritContainer | EInheritanceType::InheritObject);
        return;
    }
    ui32 inheritanceType = 0;
    if (string[end_pos] == ':') {
        while (++end_pos < string.size()) {
            switch(string[end_pos]) {
            case '-':
                inheritanceType |= EInheritanceType::InheritNone;
                break;
            case 'C':
                inheritanceType |= EInheritanceType::InheritContainer;
                break;
            case 'O':
                inheritanceType |= EInheritanceType::InheritObject;
                break;
            case '+':
                inheritanceType |= EInheritanceType::InheritOnly;
                break;
            default:
                throw yexception() << "Invalid acl - invalid inheritance flags";
            }
        }
        ace.SetInheritanceType(inheritanceType);
    }
}

void TACL::FromString(const TString& string) {
    auto& acl = *MutableACE();
    acl.Clear();
    for (auto it = string.begin(); it != string.end(); ++it) {
        auto& ace = *acl.Add();
        auto start_pos = it - string.begin();
        auto end_pos = string.find(';', start_pos);
        FromString(ace, string.substr(start_pos, end_pos == TString::npos ? end_pos : end_pos - start_pos));
        if (end_pos == TString::npos)
            break;
        it = string.begin() + end_pos;
    }
}

TDiffACL::TDiffACL(const TString& string) {
    Y_ABORT_UNLESS(ParseFromString(string));
}

void TDiffACL::AddAccess(EAccessType type, ui32 access, const TSID& sid, ui32 inheritance) {
    NACLibProto::TDiffACE* diffACE(AddDiffACE());
    diffACE->SetDiffType(static_cast<ui32>(EDiffType::Add));
    NACLibProto::TACE* ace(diffACE->MutableACE());
    ace->SetAccessType(static_cast<ui32>(type));
    ace->SetAccessRight(access);
    ace->SetSID(sid);
    ace->SetInheritanceType(inheritance);
}

void TDiffACL::RemoveAccess(NACLib::EAccessType type, ui32 access, const NACLib::TSID& sid, ui32 inheritance) {
    NACLibProto::TDiffACE* diffACE(AddDiffACE());
    diffACE->SetDiffType(static_cast<ui32>(EDiffType::Remove));
    NACLibProto::TACE* ace(diffACE->MutableACE());
    ace->SetAccessType(static_cast<ui32>(type));
    ace->SetAccessRight(access);
    ace->SetSID(sid);
    ace->SetInheritanceType(inheritance);
}

void TDiffACL::AddAccess(const NACLibProto::TACE& access) {
    NACLibProto::TDiffACE* diffACE(AddDiffACE());
    diffACE->SetDiffType(static_cast<ui32>(EDiffType::Add));
    NACLibProto::TACE* ace(diffACE->MutableACE());
    ace->CopyFrom(access);
}

void TDiffACL::RemoveAccess(const NACLibProto::TACE& access) {
    NACLibProto::TDiffACE* diffACE(AddDiffACE());
    diffACE->SetDiffType(static_cast<ui32>(EDiffType::Remove));
    NACLibProto::TACE* ace(diffACE->MutableACE());
    ace->CopyFrom(access);
}

void TDiffACL::ClearAccess() {
    NACLibProto::TDiffACE* diffACE(AddDiffACE());
    diffACE->SetDiffType(static_cast<ui32>(EDiffType::Remove));
}

void TDiffACL::ClearAccessForSid(const NACLib::TSID& sid) {
    NACLibProto::TDiffACE* diffACE(AddDiffACE());
    diffACE->SetDiffType(static_cast<ui32>(EDiffType::Remove));
    diffACE->MutableACE()->SetSID(sid);
}

TString AccessRightsToString(ui32 accessRights) {
    switch (accessRights) {
    case EAccessRights::GenericFullLegacy: return "FullLegacy";
    case EAccessRights::GenericFull: return "Full";
    case EAccessRights::GenericWrite: return "Write";
    case EAccessRights::GenericRead: return "Read";
    case EAccessRights::GenericList: return "List";
    case EAccessRights::GenericManage: return "Manage";
    case EAccessRights::GenericUseLegacy: return "UseLegacy";
    case EAccessRights::GenericUse: return "Use";
    }
    TVector<TStringBuf> rights;
    if (accessRights & EAccessRights::SelectRow)
        rights.emplace_back("SelectRow");
    if (accessRights & EAccessRights::UpdateRow)
        rights.emplace_back("UpdateRow");
    if (accessRights & EAccessRights::EraseRow)
        rights.emplace_back("EraseRow");
    if (accessRights & EAccessRights::ReadAttributes)
        rights.emplace_back("ReadAttributes");
    if (accessRights & EAccessRights::WriteAttributes)
        rights.emplace_back("WriteAttributes");
    if (accessRights & EAccessRights::CreateDirectory)
        rights.emplace_back("CreateDirectory");
    if (accessRights & EAccessRights::CreateTable)
        rights.emplace_back("CreateTable");
    if (accessRights & EAccessRights::CreateQueue)
        rights.emplace_back("CreateQueue");
    if (accessRights & EAccessRights::RemoveSchema)
        rights.emplace_back("RemoveSchema");
    if (accessRights & EAccessRights::DescribeSchema)
        rights.emplace_back("DescribeSchema");
    if (accessRights & EAccessRights::AlterSchema)
        rights.emplace_back("AlterSchema");
    if (accessRights & EAccessRights::CreateDatabase)
        rights.emplace_back("CreateDatabase");
    if (accessRights & EAccessRights::DropDatabase)
        rights.emplace_back("DropDatabase");
    if (accessRights & EAccessRights::GrantAccessRights)
        rights.emplace_back("GrantAccessRights");
    if (accessRights & EAccessRights::WriteUserAttributes)
        rights.emplace_back("WriteUserAttributes");
    if (accessRights & EAccessRights::ConnectDatabase)
        rights.emplace_back("ConnectDatabase");
    TString result;
    for (auto it = rights.begin(); it != rights.end(); ++it) {
        if (it != rights.begin()) {
            result += '|';
        }
        result += *it;
    }
    return result;
}

const NACLib::TUserToken& TSystemUsers::Metadata() {
    static TUserToken GlobalMetadataUser = TUserToken(BUILTIN_ACL_METADATA, {});
    return GlobalMetadataUser;
}

}
