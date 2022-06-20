#pragma once

#include "schemeshard_types.h"
#include "schemeshard_effective_acl.h"
#include "schemeshard_user_attr_limits.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/library/aclib/aclib.h>

#include <util/generic/map.h>
#include <util/generic/ptr.h>
#include <util/string/cast.h>

#include <ydb/core/util/yverify_stream.h>

namespace NKikimr {
namespace NSchemeShard {

constexpr TStringBuf ATTR_PREFIX = "__";
constexpr TStringBuf ATTR_VOLUME_SPACE_LIMIT = "__volume_space_limit";
constexpr TStringBuf ATTR_VOLUME_SPACE_LIMIT_HDD = "__volume_space_limit_hdd";
constexpr TStringBuf ATTR_VOLUME_SPACE_LIMIT_SSD = "__volume_space_limit_ssd";
constexpr TStringBuf ATTR_VOLUME_SPACE_LIMIT_SSD_NONREPL = "__volume_space_limit_ssd_nonrepl";
constexpr TStringBuf ATTR_VOLUME_SPACE_LIMIT_SSD_SYSTEM = "__volume_space_limit_ssd_system";
constexpr TStringBuf ATTR_EXTRA_PATH_SYMBOLS_ALLOWED = "__extra_path_symbols_allowed";
constexpr TStringBuf ATTR_DOCUMENT_API_VERSION = "__document_api_version";

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
            errStr = Sprintf("UserArttibutes::CheckLimits: user attributes too big: %" PRIu64, bytes);
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
            errStr = Sprintf("UserArttibutes: name too long, name# '%s' value# '%s'"
                             , name.c_str(), value.c_str());
            return false;
        }

        if (value.size() > TUserAttributesLimits::MaxValueLen) {
            errStr = Sprintf("UserArttibutes: value too long, name# '%s' value# '%s'"
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
        }

        Y_UNREACHABLE();
    }

    static bool CheckAttributeStringWithWeakCheck(const TString& name, const TString& value, TString& errStr) {
        if (!IsValidPathName_WeakCheck(value)) {
            errStr = Sprintf("UserArttibutes: attribute '%s' has invalid value '%s', forbidden symbols are found",
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

    // Number of references to this path element in the database
    size_t DbRefCount = 0;
    size_t AllChildrenCount = 0;

private:
    ui64 AliveChildrenCount = 0;
    ui64 ShardsInsideCount = 0;
    TChildrenCont Children;

public:
    TPathElement(TPathId pathId, TPathId parentPathId, TPathId domainPathId, const TString& name, const TString& owner)
        : PathId(pathId)
        , ParentPathId(parentPathId)
        , DomainPathId(domainPathId)
        , Name(name)
        , Owner(owner)
        , UserAttrs(new TUserAttributes(1))
    {}

    ui64 GetAliveChildren() const {
        return AliveChildrenCount;
    }

    void SetAliveChildren(ui64 val) {
        AliveChildrenCount = val;
    }

    void IncAliveChildren(ui64 delta = 1) {
        Y_VERIFY(Max<ui64>() - AliveChildrenCount >= delta);
        AliveChildrenCount += delta;
    }

    void DecAliveChildren(ui64 delta = 1) {
        Y_VERIFY(AliveChildrenCount >= delta);
        AliveChildrenCount -= delta;
    }

    ui64 GetShardsInside() const {
        return ShardsInsideCount;
    }

    void SetShardsInside(ui64 val) {
        ShardsInsideCount = val;
    }

    void IncShardsInside(ui64 delta = 1) {
        Y_VERIFY(Max<ui64>() - ShardsInsideCount >= delta);
        ShardsInsideCount += delta;
    }

    void DecShardsInside(ui64 delta = 1) {
        Y_VERIFY(ShardsInsideCount >= delta);
        ShardsInsideCount -= delta;
    }

    bool IsRoot() const {
        return PathId.LocalPathId == RootPathId;
    }

    bool IsDirectory() const {
        return PathType == EPathType::EPathTypeDir;
    }

    bool IsTableIndex() const {
        return PathType == EPathType::EPathTypeTableIndex;
    }

    bool IsCdcStream() const {
        return PathType == EPathType::EPathTypeCdcStream;
    }

    bool IsTable() const {
        return PathType == EPathType::EPathTypeTable;
    }

    bool IsSolomon() const {
        return PathType == EPathType::EPathTypeSolomonVolume;
    }

    bool IsPQGroup() const {
        return PathType == EPathType::EPathTypePersQueueGroup;
    }

    bool IsDomainRoot() const {
        return IsSubDomainRoot() || IsExternalSubDomainRoot();
    }

    bool IsSubDomainRoot() const {
        return PathType == EPathType::EPathTypeSubDomain || IsRoot();
    }

    bool IsExternalSubDomainRoot() const {
        return PathType == EPathType::EPathTypeExtSubDomain;
    }

    bool IsRtmrVolume() const {
        return PathType == EPathType::EPathTypeRtmrVolume;
    }

    bool IsBlockStoreVolume() const {
        return PathType == EPathType::EPathTypeBlockStoreVolume;
    }

    bool IsFileStore() const {
        return PathType == EPathType::EPathTypeFileStore;
    }

    bool IsKesus() const {
        return PathType == EPathType::EPathTypeKesus;
    }

    bool IsOlapStore() const {
        return PathType == EPathType::EPathTypeColumnStore;
    }

    bool IsOlapTable() const {
        return PathType == EPathType::EPathTypeColumnTable;
    }

    bool IsSequence() const {
        return PathType == EPathType::EPathTypeSequence;
    }

    bool IsReplication() const {
        return PathType == EPathType::EPathTypeReplication;
    }

    bool IsBlobDepot() const {
        return PathType == EPathType::EPathTypeBlobDepot;
    }

    bool IsContainer() const {
        return PathType == EPathType::EPathTypeDir || PathType == EPathType::EPathTypeSubDomain
            || PathType == EPathType::EPathTypeColumnStore;
    }

    bool IsLikeDirectory() const {
        return IsDirectory() || IsDomainRoot() || IsOlapStore();
    }

    bool HasActiveChanges() const {
        // there are old clusters where Root node has CreateTxId == 0
        return (!IsRoot() && !CreateTxId) || (PathState != EPathState::EPathStateNoChanges);
    }

    bool IsCreateFinished() const {
        return (IsRoot() && CreateTxId) || StepCreated;
    }

    TGlobalTimestamp GetCreateTS() const {
        return TGlobalTimestamp(StepCreated, CreateTxId);
    }

    TGlobalTimestamp GetDropTS() const {
        return TGlobalTimestamp(StepDropped, DropTxId);
    }

    void SetDropped(TStepId step, TTxId txId) {
        PathState = EPathState::EPathStateNotExist;
        StepDropped = step;
        DropTxId = txId;
    }

    bool NormalState() const {
        return PathState == EPathState::EPathStateNoChanges;
    }

    bool Dropped() const {
        if (StepDropped) {
            Y_VERIFY_DEBUG_S(PathState == EPathState::EPathStateNotExist,
                             "Non consistent PathState and StepDropped."
                                 << " PathState: " << NKikimrSchemeOp::EPathState_Name(PathState)
                                 << ", StepDropped: " << StepDropped
                                 << ", PathId: " << PathId);
        }
        return bool(StepDropped);
    }

    bool IsMigrated() const {
        return PathState == EPathState::EPathStateMigrated;
    }

    bool IsUnderMoving() const {
        return PathState == EPathState::EPathStateMoving;
    }

    bool IsUnderCreating() const {
        return PathState == EPathState::EPathStateCreate;
    }

    bool PlannedToCreate() const {
        return PathState == EPathState::EPathStateCreate;
    }

    bool PlannedToDrop() const {
        return PathState == EPathState::EPathStateDrop;
    }

    bool AddChild(const TString& name, TPathId pathId, bool replace = false) {
        TPathId* ptr = FindChild(name);
        if (ptr && !replace)
            return false;
        Children[name] = pathId;
        return true;
    }

    bool RemoveChild(const TString& name, TPathId pathId) {
        auto it = Children.find(name);
        if (it != Children.end() && it->second == pathId) {
            Children.erase(it);
            return true;
        }
        return false;
    }

    TPathId* FindChild(const TString& name) {
        return Children.FindPtr(name);
    }

    const TChildrenCont& GetChildren() const {
        return Children;
    }

    void SwapChildren(TChildrenCont& container) {
        container.swap(Children);
    }

    void ApplyACL(const TString& acl) {
        NACLib::TACL secObj(ACL);
        NACLib::TDiffACL diffACL(acl);
        secObj.ApplyDiff(diffACL);
        ACL = secObj.SerializeAsString();
    }

    void ApplySpecialAttributes() {
        VolumeSpaceRaw.Limit = Max<ui64>();
        VolumeSpaceSSD.Limit = Max<ui64>();
        VolumeSpaceHDD.Limit = Max<ui64>();
        VolumeSpaceSSDNonrepl.Limit = Max<ui64>();
        VolumeSpaceSSDSystem.Limit = Max<ui64>();
        ExtraPathSymbolsAllowed = TString();
        for (const auto& item : UserAttrs->Attrs) {
            switch (TUserAttributes::ParseName(item.first)) {
                case EAttribute::VOLUME_SPACE_LIMIT:
                    HandleAttributeValue(item.second, VolumeSpaceRaw.Limit);
                    break;
                case EAttribute::VOLUME_SPACE_LIMIT_SSD:
                    HandleAttributeValue(item.second, VolumeSpaceSSD.Limit);
                    break;
                case EAttribute::VOLUME_SPACE_LIMIT_HDD:
                    HandleAttributeValue(item.second, VolumeSpaceHDD.Limit);
                    break;
                case EAttribute::VOLUME_SPACE_LIMIT_SSD_NONREPL:
                    HandleAttributeValue(item.second, VolumeSpaceSSDNonrepl.Limit);
                    break;
                case EAttribute::VOLUME_SPACE_LIMIT_SSD_SYSTEM:
                    HandleAttributeValue(item.second, VolumeSpaceSSDSystem.Limit);
                    break;
                case EAttribute::EXTRA_PATH_SYMBOLS_ALLOWED:
                    HandleAttributeValue(item.second, ExtraPathSymbolsAllowed);
                    break;
                case EAttribute::DOCUMENT_API_VERSION:
                    HandleAttributeValue(item.second, DocumentApiVersion);
                    break;
                default:
                    break;
            }
        }
    }

    void HandleAttributeValue(const TString& value, TString& target) {
        target = value;
    }

    void HandleAttributeValue(const TString& value, ui64& target) {
        ui64 parsed;
        if (TryFromString(value, parsed)) {
            target = parsed;
        }
    }

    void ChangeVolumeSpaceBegin(TVolumeSpace newSpace, TVolumeSpace oldSpace) {
        auto update = [](TVolumeSpaceLimits& limits, ui64 newValue, ui64 oldValue) {
            if (newValue > oldValue) {
                // Volume space increase is handled at tx begin
                limits.Allocated += newValue - oldValue;
            }
        };
        update(VolumeSpaceRaw, newSpace.Raw, oldSpace.Raw);
        update(VolumeSpaceSSD, newSpace.SSD, oldSpace.SSD);
        update(VolumeSpaceHDD, newSpace.HDD, oldSpace.HDD);
        update(VolumeSpaceSSDNonrepl, newSpace.SSDNonrepl, oldSpace.SSDNonrepl);
        update(VolumeSpaceSSDSystem, newSpace.SSDSystem, oldSpace.SSDSystem);
    }

    void ChangeVolumeSpaceCommit(TVolumeSpace newSpace, TVolumeSpace oldSpace) {
        auto update = [](TVolumeSpaceLimits& limits, ui64 newValue, ui64 oldValue) {
            if (newValue < oldValue) {
                // Volume space decrease is handled at tx commit
                ui64 diff = oldValue - newValue;
                Y_VERIFY(limits.Allocated >= diff);
                limits.Allocated -= diff;
            }
        };
        update(VolumeSpaceRaw, newSpace.Raw, oldSpace.Raw);
        update(VolumeSpaceSSD, newSpace.SSD, oldSpace.SSD);
        update(VolumeSpaceHDD, newSpace.HDD, oldSpace.HDD);
        update(VolumeSpaceSSDNonrepl, newSpace.SSDNonrepl, oldSpace.SSDNonrepl);
        update(VolumeSpaceSSDSystem, newSpace.SSDSystem, oldSpace.SSDSystem);
    }

    bool CheckVolumeSpaceChange(TVolumeSpace newSpace, TVolumeSpace oldSpace, TString& errStr) {
        auto check = [&errStr](const TVolumeSpaceLimits& limits, ui64 newValue, ui64 oldValue, const char* suffix) -> bool {
            if (newValue > oldValue) {
                ui64 newAllocated = limits.Allocated + newValue - oldValue;
                if (newAllocated > limits.Limit) {
                    errStr = TStringBuilder()
                        << "New volume space is over a limit" << suffix
                        << ": " << newAllocated << " > " << limits.Limit;
                    return false;
                }
            }
            return true;
        };
        return (check(VolumeSpaceRaw, newSpace.Raw, oldSpace.Raw, "") &&
                check(VolumeSpaceSSD, newSpace.SSD, oldSpace.SSD, " (ssd)") &&
                check(VolumeSpaceHDD, newSpace.HDD, oldSpace.HDD, " (hdd)") &&
                check(VolumeSpaceSSDNonrepl, newSpace.SSDNonrepl, oldSpace.SSDNonrepl, " (ssd_nonrepl)") &&
                check(VolumeSpaceSSDSystem, newSpace.SSDSystem, oldSpace.SSDSystem, " (ssd_system)"));
    }

    bool HasRuntimeAttrs() const {
        return (VolumeSpaceRaw.Allocated > 0 ||
                VolumeSpaceSSD.Allocated > 0 ||
                VolumeSpaceHDD.Allocated > 0 ||
                VolumeSpaceSSDNonrepl.Allocated > 0 ||
                VolumeSpaceSSDSystem.Allocated > 0);
    }

    void SerializeRuntimeAttrs(
            google::protobuf::RepeatedPtrField<NKikimrSchemeOp::TUserAttribute>* userAttrs) const
    {
        auto process = [userAttrs](const TVolumeSpaceLimits& limits, const char* name) {
            if (limits.Allocated > 0) {
                auto* attr = userAttrs->Add();
                attr->SetKey(name);
                attr->SetValue(TStringBuilder() << limits.Allocated);
            }
        };
        process(VolumeSpaceRaw, "__volume_space_allocated");
        process(VolumeSpaceSSD, "__volume_space_allocated_ssd");
        process(VolumeSpaceHDD, "__volume_space_allocated_hdd");
        process(VolumeSpaceSSDNonrepl, "__volume_space_allocated_ssd_nonrepl");
        process(VolumeSpaceSSDSystem, "__volume_space_allocated_ssd_system");
    }

};
}
}
