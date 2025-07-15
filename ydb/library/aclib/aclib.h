#pragma once
#include <util/generic/vector.h>
#include <util/generic/hash_set.h>
#include <util/datetime/base.h>
#include <ydb/library/aclib/protos/aclib.pb.h>

namespace NACLib {

#define BUILTIN_ACL_DOMAIN "builtin"
#define BUILTIN_ACL_ROOT "root@" BUILTIN_ACL_DOMAIN
#define BUILTIN_ERROR_DOMAIN "error"
#define BUILTIN_SYSTEM_DOMAIN "system"

#define BUILTIN_ACL_METADATA "metadata@" BUILTIN_SYSTEM_DOMAIN
class TUserToken;
class TSystemUsers {
public:
    static const TUserToken& Metadata();
};

enum EAccessRights : ui32 { // bitmask
    NoAccess = 0x00000000,
    SelectRow = 0x00000001, // select row from table
    UpdateRow = 0x00000002, // update row in table
    EraseRow = 0x00000004, // erase row from table
    ReadAttributes = 0x00000008, // read attributes / ACL
    WriteAttributes = 0x00000010, // modify attributes / ACL
    CreateDirectory = 0x00000020, // create subdirectory
    CreateTable = 0x00000040, // create table
    CreateQueue = 0x00000080, // create queue
    RemoveSchema = 0x00000100, // remove object
    DescribeSchema = 0x00000200, // describe schema object
    AlterSchema = 0x00000400, // modify schema
    CreateDatabase = 0x00000800, // create database
    DropDatabase = 0x00001000, // drop database
    GrantAccessRights = 0x00002000, // grant access rights (only own access rights)
    WriteUserAttributes = 0x00004000, // modify user attributes / KV
    ConnectDatabase = 0x00008000, // any type of request to DB
    ReadStream = 0x00010000, // reading streams
    WriteStream = 0x00020000, // writing streams
    ReadTopic = 0x00040000, // reading topics
    WriteTopic = 0x00080000, // writing topics

    GenericList = ReadAttributes | DescribeSchema,
    GenericRead = SelectRow | GenericList,
    GenericWrite = UpdateRow | EraseRow | WriteAttributes | CreateDirectory | CreateTable | CreateQueue | RemoveSchema | AlterSchema | WriteUserAttributes,
    GenericUseLegacy = GenericRead | GenericWrite | GrantAccessRights,
    GenericUse = GenericUseLegacy | ConnectDatabase,
    GenericManage = CreateDatabase | DropDatabase,
    GenericFullLegacy = GenericUseLegacy | GenericManage,
    GenericFull = GenericUse | GenericManage,
};

TString AccessRightsToString(ui32 accessRights);

enum class EAccessType : ui32 {
    Deny,
    Allow,
};

enum EInheritanceType : ui32 { // bitmask
    InheritNone = 0x00,
    InheritObject = 0x01, // this ACE will inherit on child objects
    InheritContainer = 0x02, // this ACE will inherit on child containers
    InheritOnly = 0x04, // this ACE will not be used for access checking but for inheritance only
};

enum class EDiffType : ui32 {
    Add,
    Remove,
};

using TSID = TString;

class TUserToken : protected NACLibProto::TUserToken, public TThrRefBase {
public:
    struct TUserTokenInitFields {
        TString OriginalUserToken;
        TString UserSID;
        TVector<TString> GroupSIDs;
        TString AuthType;
    };

    TUserToken(TUserTokenInitFields fields);
    TUserToken(const TVector<TSID>& userAndGroupSIDs);
    TUserToken(const TSID& userSID, const TVector<TSID>& groupSIDs);
    TUserToken(const TString& originalUserToken, const TSID& userSID, const TVector<TSID>& groupSIDs);
    TUserToken(const NACLibProto::TUserToken& token);
    TUserToken(NACLibProto::TUserToken&& token);
    explicit TUserToken(const TString& token);
    bool IsExist(const TSID& someSID) const; // check for presence of SID specified in the token
    TSID GetUserSID() const;
    using NACLibProto::TUserToken::GetSanitizedToken;
    using NACLibProto::TUserToken::SetSanitizedToken;
    using NACLibProto::TUserToken::GetSubjectType;
    using NACLibProto::TUserToken::SetSubjectType;
    TVector<TSID> GetGroupSIDs() const;
    TString GetOriginalUserToken() const;
    TString SerializeAsString() const;
    void SaveSerializationInfo();
    void AddGroupSID(const TSID& groupSID);
    bool IsSystemUser() const;
    const TString& GetSerializedToken() const;

    using NACLibProto::TUserToken::ShortDebugString;

protected:
    static TSID GetUserFromVector(const TVector<TSID>& userAndGroupSIDs);
    static TVector<TSID> GetGroupsFromVector(const TVector<TSID>& userAndGroupSIDs);
    void SetGroupSIDs(const TVector<TString>& groupSIDs);
private:
    TString Serialized_;
};

class TACL : public NACLibProto::TACL {
public:
    TACL() = default;
    TACL(const TString& string); // proto format
    std::pair<ui32, ui32> AddAccess(EAccessType type, ui32 access, const TSID& sid, ui32 inheritance = InheritObject | InheritContainer);
    std::pair<ui32, ui32> RemoveAccess(NACLib::EAccessType type, ui32 access, const NACLib::TSID& sid, ui32 inheritance = InheritObject | InheritContainer);
    std::pair<ui32, ui32> RemoveAccess(const NACLibProto::TACE& filter);
    std::pair<ui32, ui32> ClearAccess();
    std::pair<ui32, ui32> ApplyDiff(const NACLibProto::TDiffACL& diffACL);
    NACLibProto::TACL GetImmediateACL() const;
    TString ToString() const; // simple text format
    void FromString(const TString& string); // simple text format
    static TString ToString(const NACLibProto::TACE& ace);
    static void FromString(NACLibProto::TACE& ace, const TString& string);

protected:
    static ui32 SpecialRightsFromString(const TString& string);

    void SortACL();
};

class TDiffACL : public NACLibProto::TDiffACL {
public:
    TDiffACL() = default;
    TDiffACL(const TString& string);
    void AddAccess(EAccessType type, ui32 access, const TSID& sid, ui32 inheritance = InheritObject | InheritContainer);
    void RemoveAccess(NACLib::EAccessType type, ui32 access, const NACLib::TSID& sid, ui32 inheritance = InheritObject | InheritContainer);
    void AddAccess(const NACLibProto::TACE& access);
    void RemoveAccess(const NACLibProto::TACE& access);
    void ClearAccess();
    void ClearAccessForSid(const NACLib::TSID& sid);
};

class TSecurityObject : public NACLibProto::TSecurityObject {
public:
    static_assert(sizeof(TACL) == sizeof(NACLibProto::TACL), "TACL class should be empty to cast from proto type");

    TSecurityObject(const NACLibProto::TSecurityObject& protoSecObj, bool isContainer); // creating from existing object
    TSecurityObject(const TSID& owner, bool isContainer); // creating new object
    TSecurityObject(); // creating new object for ACL manipulation only
    bool CheckAccess(ui32 access, const TUserToken& user) const; // check for access requested
    bool CheckGrantAccess(const NACLibProto::TDiffACL& diffACL, const TUserToken& user) const; // check for valid diffACL
    ui32 GetEffectiveAccessRights(const TUserToken& user) const;
    TSecurityObject MergeWithParent(const NACLibProto::TSecurityObject& parent) const; // returns effective ACL as result of merging parent with this
    NACLibProto::TACL GetImmediateACL() const;
    void AddAccess(EAccessType type, ui32 access, const TSID& sid, ui32 inheritance = InheritObject | InheritContainer);
    void RemoveAccess(NACLib::EAccessType type, ui32 access, const NACLib::TSID& sid, ui32 inheritance = InheritObject | InheritContainer);
    void ApplyDiff(const NACLibProto::TDiffACL& diffACL);
    void ClearAccess();
    TInstant GetExpireTime() const;
    TString ToString() const; // simple text format
    void FromString(const TString& string); // simple text format
    bool operator == (const TSecurityObject& rhs) const;
    bool operator != (const TSecurityObject& rhs) const;

protected:
    bool IsContainer;
};






}
