#pragma once

#include "schemeshard_types.h"
#include "schemeshard_effective_acl.h"
#include "schemeshard_user_attr_limits.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/util/yverify_stream.h>
#include <ydb/library/aclib/aclib.h>

#include <library/cpp/json/json_reader.h>

#include <util/generic/map.h>
#include <util/generic/ptr.h>
#include <util/string/cast.h>

namespace NKikimr {
namespace NSchemeShard {

class TPath;

constexpr TStringBuf ATTR_PREFIX = "__";
constexpr TStringBuf ATTR_VOLUME_SPACE_LIMIT = "__volume_space_limit";
constexpr TStringBuf ATTR_VOLUME_SPACE_LIMIT_HDD = "__volume_space_limit_hdd";
constexpr TStringBuf ATTR_VOLUME_SPACE_LIMIT_SSD = "__volume_space_limit_ssd";
constexpr TStringBuf ATTR_VOLUME_SPACE_LIMIT_SSD_NONREPL = "__volume_space_limit_ssd_nonrepl";
constexpr TStringBuf ATTR_VOLUME_SPACE_LIMIT_SSD_SYSTEM = "__volume_space_limit_ssd_system";
constexpr TStringBuf ATTR_EXTRA_PATH_SYMBOLS_ALLOWED = "__extra_path_symbols_allowed";
constexpr TStringBuf ATTR_DOCUMENT_API_VERSION = "__document_api_version";
constexpr TStringBuf ATTR_ASYNC_REPLICATION = "__async_replication";

inline bool WeakCheck(char c) {
    // 33: ! " # $ % & ' ( ) * + , - . /
    // 48: 0 1 2 3 4 5 6 7 8 9 : ; < = > ? @
    // 65: A B C D E F G H I J K L M N O P Q R S T U V W X Y Z
    // 91: [ \ ] ^ _ `
    // 97: a b c d e f g h i j k l m n o p q r s t u v w x y z
    // 123: { | } ~
    if (c >= 33 && c <= 126)
        return true;
    return false;
}

inline bool IsValidPathName_WeakCheck(const TString& name) {
    for (auto c: name) {
        if (!WeakCheck(c) || c == '/') {
            return false;
        }
    }
    return true;
}

enum class EAttribute {
    USER,
    UNKNOWN,
    VOLUME_SPACE_LIMIT,
    VOLUME_SPACE_LIMIT_HDD,
    VOLUME_SPACE_LIMIT_SSD,
    EXTRA_PATH_SYMBOLS_ALLOWED, // deprecated
    VOLUME_SPACE_LIMIT_SSD_NONREPL,
    DOCUMENT_API_VERSION,
    VOLUME_SPACE_LIMIT_SSD_SYSTEM,
    ASYNC_REPLICATION,
};

struct TVolumeSpace {
    ui64 Raw = 0;
    ui64 SSD = 0;
    ui64 HDD = 0;
    ui64 SSDNonrepl = 0;
    ui64 SSDSystem = 0;
};

struct TVolumeSpaceLimits {
    ui64 Allocated = 0;
    ui64 Limit = Max<ui64>();
};

enum class EUserAttributesOp {
    InitRoot,
    MkDir,
    AlterUserAttrs,
    CreateTable,
    CreateSubDomain,
    CreateExtSubDomain,
    SyncUpdateTenants,
    CreateChangefeed,
};

struct TUserAttributes: TSimpleRefCount<TUserAttributes> {
    using TPtr = TIntrusivePtr<TUserAttributes>;
    using TAttrs = TMap<TString, TString>;

    TAttrs Attrs;
    ui64 AlterVersion;
    TPtr AlterData;

    explicit TUserAttributes(ui64 version)
        : AlterVersion(version)
    {}

    TUserAttributes(const TUserAttributes&) = default;

    TPtr CreateNextVersion() {
        auto result = new TUserAttributes(*this);
        ++result->AlterVersion;
        return result;
    }

    static EAttribute ParseName(TStringBuf name) {
        if (name.StartsWith(ATTR_PREFIX)) {
            #define HANDLE_ATTR(attr) \
                if (name == ATTR_ ## attr) { \
                    return EAttribute::attr; \
                }
                HANDLE_ATTR(VOLUME_SPACE_LIMIT);
                HANDLE_ATTR(VOLUME_SPACE_LIMIT_HDD);
                HANDLE_ATTR(VOLUME_SPACE_LIMIT_SSD);
                HANDLE_ATTR(VOLUME_SPACE_LIMIT_SSD_NONREPL);
                HANDLE_ATTR(VOLUME_SPACE_LIMIT_SSD_SYSTEM);
                HANDLE_ATTR(EXTRA_PATH_SYMBOLS_ALLOWED);
                HANDLE_ATTR(DOCUMENT_API_VERSION);
                HANDLE_ATTR(ASYNC_REPLICATION);
            #undef HANDLE_ATTR
            return EAttribute::UNKNOWN;
        }

        return EAttribute::USER;
    }

    bool ApplyPatch(EUserAttributesOp op, const NKikimrSchemeOp::TAlterUserAttributes& patch, TString& errStr) {
        return ApplyPatch(op, patch.GetUserAttributes(), errStr);
    }

    template <class TContainer>
    bool ApplyPatch(EUserAttributesOp op, const TContainer& patch, TString& errStr) {
        for (auto& item: patch) {
            const auto& name = item.GetKey();

            if (item.HasValue()) {
                const auto& value = item.GetValue();
                if (!CheckAttribute(op, name, value, errStr)) {
                    return false;
                }

                Attrs[name] = value;
            } else {
                if (!CheckAttributeRemove(op, name, errStr)) {
                    return false;
                }

                Attrs.erase(name);
            }
        }

        return true;
    }

    void Set(const TString& name, const TString& value) {
        Attrs[name] = value;
    }

    ui32 Size() const {
        return Attrs.size();
    }

    ui64 Bytes() const {
        ui64 bytes = 0;

        for (const auto& [key, value] : Attrs) {
            bytes += key.size();
            bytes += value.size();
        }

        return bytes;
    }

    bool CheckLimits(TString& errStr) const {
        const ui64 bytes = Bytes();
        if (bytes > TUserAttributesLimits::MaxBytes) {
            errStr = Sprintf("UserAttributes::CheckLimits: user attributes too big: %" PRIu64, bytes);
            return false;
        }

        return true;
    }

    static bool CheckAttribute(EUserAttributesOp op, const TString& name, const TString& value, TString& errStr) {
        if (op == EUserAttributesOp::SyncUpdateTenants) {
            // Migration, must never fail
            return true;
        }

        if (name.size() > TUserAttributesLimits::MaxNameLen) {
            errStr = Sprintf("UserAttributes: name too long, name# '%s' value# '%s'"
                             , name.c_str(), value.c_str());
            return false;
        }

        if (value.size() > TUserAttributesLimits::MaxValueLen) {
            errStr = Sprintf("UserAttributes: value too long, name# '%s' value# '%s'"
                             , name.c_str(), value.c_str());
            return false;
        }

        switch (ParseName(name)) {
            case EAttribute::USER:
                return true;
            case EAttribute::UNKNOWN:
                errStr = Sprintf("UserAttributes: unsupported attribute '%s'", name.c_str());
                return false;
            case EAttribute::VOLUME_SPACE_LIMIT:
            case EAttribute::VOLUME_SPACE_LIMIT_HDD:
            case EAttribute::VOLUME_SPACE_LIMIT_SSD:
            case EAttribute::VOLUME_SPACE_LIMIT_SSD_NONREPL:
            case EAttribute::VOLUME_SPACE_LIMIT_SSD_SYSTEM:
                return CheckAttributeUint64(name, value, errStr);
            case EAttribute::EXTRA_PATH_SYMBOLS_ALLOWED:
                return CheckAttributeStringWithWeakCheck(name, value, errStr);
            case EAttribute::DOCUMENT_API_VERSION:
                if (op != EUserAttributesOp::CreateTable) {
                    errStr = Sprintf("UserAttributes: attribute '%s' can only be set during CreateTable", name.c_str());
                    return false;
                }
                return CheckAttributeUint64(name, value, errStr, /* minValue = */ 1);
            case EAttribute::ASYNC_REPLICATION:
                if (op != EUserAttributesOp::CreateChangefeed) {
                    errStr = Sprintf("UserAttributes: attribute '%s' can only be set during CreateChangefeed", name.c_str());
                    return false;
                }
                return CheckAttributeJson(name, value, errStr);
        }

        Y_UNREACHABLE();
    }

    static bool CheckAttributeRemove(EUserAttributesOp op, const TString& name, TString& errStr) {
        if (op == EUserAttributesOp::SyncUpdateTenants) {
            // Migration, must never fail
            return true;
        }

        switch (ParseName(name)) {
            case EAttribute::USER:
                return true;
            case EAttribute::UNKNOWN:
                errStr = Sprintf("UserAttributes: unsupported attribute '%s'", name.c_str());
                return false;
            case EAttribute::VOLUME_SPACE_LIMIT:
            case EAttribute::VOLUME_SPACE_LIMIT_HDD:
            case EAttribute::VOLUME_SPACE_LIMIT_SSD:
            case EAttribute::VOLUME_SPACE_LIMIT_SSD_NONREPL:
            case EAttribute::VOLUME_SPACE_LIMIT_SSD_SYSTEM:
            case EAttribute::EXTRA_PATH_SYMBOLS_ALLOWED:
                return true;
            case EAttribute::DOCUMENT_API_VERSION:
                if (op != EUserAttributesOp::CreateTable) {
                    errStr = Sprintf("UserAttributes: attribute '%s' can only be set during CreateTable", name.c_str());
                    return false;
                }
                return true;
            case EAttribute::ASYNC_REPLICATION:
                if (op != EUserAttributesOp::CreateChangefeed) {
                    errStr = Sprintf("UserAttributes: attribute '%s' can only be set during CreateChangefeed", name.c_str());
                    return false;
                }
                return true;
        }

        Y_UNREACHABLE();
    }

    static bool CheckAttributeStringWithWeakCheck(const TString& name, const TString& value, TString& errStr) {
        if (!IsValidPathName_WeakCheck(value)) {
            errStr = Sprintf("UserAttributes: attribute '%s' has invalid value '%s', forbidden symbols are found",
                                name.c_str(), value.c_str());
            return false;
        }
        return true;
    }

    static bool CheckAttributeUint64(const TString& name, const TString& value, TString& errStr, ui64 minValue = 0, ui64 maxValue = Max<ui64>()) {
        ui64 parsed;
        if (!TryFromString(value, parsed)) {
            errStr = Sprintf("UserAttributes: attribute '%s' has invalid value '%s'",
                name.c_str(), value.c_str());
            return false;
        }
        if (parsed < minValue) {
            errStr = Sprintf("UserAttributes: attribute '%s' has invalid value '%s' < %" PRIu64,
                name.c_str(), value.c_str(), minValue);
            return false;
        }
        if (parsed > maxValue) {
            errStr = Sprintf("UserAttributes: attribute '%s' has invalid value '%s' > %" PRIu64,
                name.c_str(), value.c_str(), maxValue);
            return false;
        }
        return true;
    }

    static bool CheckAttributeUnknown(const std::pair<const TString, TString>& item, bool& ok, TString& errStr) {
        Y_UNUSED(item);
        ok = false;
        errStr = Sprintf("UserAttributes::CheckLimits: unsupported attribute '%s'", item.first.c_str());
        return true;
    }

    static bool CheckAttributeJson(const TString& name, const TString& value, TString& errStr) {
        NJson::TJsonValue unused;
        if (!NJson::ReadJsonTree(value, &unused)) {
            errStr = Sprintf("UserAttributes: attribute '%s' has invalid value '%s'",
                name.c_str(), value.c_str());
            return false;
        }
        return true;
    }
};

struct TPathElement : TSimpleRefCount<TPathElement> {
    using TPtr = TIntrusivePtr<TPathElement>;
    using TChildrenCont = TMap<TString, TPathId>;
    using EPathType = NKikimrSchemeOp::EPathType;
    using EPathSubType = NKikimrSchemeOp::EPathSubType;
    using EPathState = NKikimrSchemeOp::EPathState;

    static constexpr TLocalPathId RootPathId = 1;

    TPathId PathId = InvalidPathId;
    TPathId ParentPathId = InvalidPathId;
    TPathId DomainPathId = InvalidPathId;

    TString Name;
    TString Owner;
    TString ACL;

    EPathType PathType = EPathType::EPathTypeDir;
    EPathState PathState = EPathState::EPathStateNotExist;

    TStepId StepCreated = InvalidStepId;
    TTxId CreateTxId = InvalidTxId;
    TStepId StepDropped = InvalidStepId;
    TTxId DropTxId = InvalidTxId;
    TTxId LastTxId = InvalidTxId;

    ui64 DirAlterVersion = 0;
    ui64 ACLVersion = 0;

    TUserAttributes::TPtr UserAttrs;

    TString PreSerializedChildrenListing;

    TEffectiveACL CachedEffectiveACL;
    ui64 CachedEffectiveACLVersion = 0;

    TString ExtraPathSymbolsAllowed; // it's better to move it in TSubDomainInfo like SchemeLimits

    TVolumeSpaceLimits VolumeSpaceRaw;
    TVolumeSpaceLimits VolumeSpaceSSD;
    TVolumeSpaceLimits VolumeSpaceHDD;
    TVolumeSpaceLimits VolumeSpaceSSDNonrepl;
    TVolumeSpaceLimits VolumeSpaceSSDSystem;
    ui64 DocumentApiVersion = 0;
    NJson::TJsonValue AsyncReplication;

    // Number of references to this path element in the database
    size_t DbRefCount = 0;
    size_t AllChildrenCount = 0;

private:
    ui64 AliveChildrenCount = 0;
    ui64 BackupChildrenCount = 0;
    ui64 ShardsInsideCount = 0;
    TChildrenCont Children;
public:
    TPathElement(TPathId pathId, TPathId parentPathId, TPathId domainPathId, const TString& name, const TString& owner);
    ui64 GetAliveChildren() const;
    void SetAliveChildren(ui64 val);
    ui64 GetBackupChildren() const;
    void IncAliveChildren(ui64 delta = 1, bool isBackup = false);
    void DecAliveChildren(ui64 delta = 1, bool isBackup = false);
    ui64 GetShardsInside() const;
    void SetShardsInside(ui64 val);
    void IncShardsInside(ui64 delta = 1);
    void DecShardsInside(ui64 delta = 1);
    bool IsRoot() const;
    bool IsDirectory() const;
    bool IsTableIndex() const;
    bool IsCdcStream() const;
    bool IsTable() const;
    bool IsSolomon() const;
    bool IsPQGroup() const;
    bool IsDomainRoot() const;
    bool IsSubDomainRoot() const;
    bool IsExternalSubDomainRoot() const;
    bool IsRtmrVolume() const;
    bool IsBlockStoreVolume() const;
    bool IsFileStore() const;
    bool IsKesus() const;
    bool IsOlapStore() const;
    bool IsColumnTable() const;
    bool IsSequence() const;
    bool IsReplication() const;
    bool IsBlobDepot() const;
    bool IsContainer() const;
    bool IsLikeDirectory() const;
    bool HasActiveChanges() const;
    bool IsCreateFinished() const;
    TGlobalTimestamp GetCreateTS() const;
    TGlobalTimestamp GetDropTS() const;
    void SetDropped(TStepId step, TTxId txId);
    bool NormalState() const;
    bool Dropped() const;
    bool IsMigrated() const;
    bool IsUnderMoving() const;
    bool IsUnderCreating() const;
    bool PlannedToCreate() const;
    bool PlannedToDrop() const;
    bool AddChild(const TString& name, TPathId pathId, bool replace = false);
    bool RemoveChild(const TString& name, TPathId pathId);
    TPathId* FindChild(const TString& name);
    const TChildrenCont& GetChildren() const;
    void SwapChildren(TChildrenCont& container);
    void ApplyACL(const TString& acl);
    void ApplySpecialAttributes();
    void HandleAttributeValue(const TString& value, TString& target);
    void HandleAttributeValue(const TString& value, ui64& target);
    void HandleAttributeValue(const TString& value, NJson::TJsonValue& target);
    void ChangeVolumeSpaceBegin(TVolumeSpace newSpace, TVolumeSpace oldSpace);
    void ChangeVolumeSpaceCommit(TVolumeSpace newSpace, TVolumeSpace oldSpace);
    bool CheckVolumeSpaceChange(TVolumeSpace newSpace, TVolumeSpace oldSpace, TString& errStr);
    bool HasRuntimeAttrs() const;
    void SerializeRuntimeAttrs(google::protobuf::RepeatedPtrField<NKikimrSchemeOp::TUserAttribute>* userAttrs) const;
};
}
}
