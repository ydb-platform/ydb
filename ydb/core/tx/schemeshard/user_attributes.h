#pragma once

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <util/generic/map.h>
#include <util/generic/ptr.h>

namespace NKikimr::NSchemeShard {

constexpr TStringBuf ATTR_PREFIX = "__";
constexpr TStringBuf ATTR_VOLUME_SPACE_LIMIT = "__volume_space_limit";
constexpr TStringBuf ATTR_VOLUME_SPACE_LIMIT_HDD = "__volume_space_limit_hdd";
constexpr TStringBuf ATTR_VOLUME_SPACE_LIMIT_SSD = "__volume_space_limit_ssd";
constexpr TStringBuf ATTR_VOLUME_SPACE_LIMIT_SSD_NONREPL = "__volume_space_limit_ssd_nonrepl";
constexpr TStringBuf ATTR_VOLUME_SPACE_LIMIT_SSD_SYSTEM = "__volume_space_limit_ssd_system";
constexpr TStringBuf ATTR_FILESTORE_SPACE_LIMIT_SSD = "__filestore_space_limit_ssd";
constexpr TStringBuf ATTR_FILESTORE_SPACE_LIMIT_HDD = "__filestore_space_limit_hdd";
constexpr TStringBuf ATTR_EXTRA_PATH_SYMBOLS_ALLOWED = "__extra_path_symbols_allowed";
constexpr TStringBuf ATTR_DOCUMENT_API_VERSION = "__document_api_version";
constexpr TStringBuf ATTR_ASYNC_REPLICATION = "__async_replication";
constexpr TStringBuf ATTR_ASYNC_REPLICA = "__async_replica";
constexpr TStringBuf ATTR_INCREMENTAL_BACKUP = "__incremental_backup";

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
    FILESTORE_SPACE_LIMIT_SSD,
    FILESTORE_SPACE_LIMIT_HDD,
    ASYNC_REPLICA,
    INCREMENTAL_BACKUP,
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

    TMap<TString, TString> Attrs;
    ui64 AlterVersion;
    TPtr AlterData;

    explicit TUserAttributes(ui64 version);
    TUserAttributes(const TUserAttributes&) = default;

    TPtr CreateNextVersion() const;

    static EAttribute ParseName(TStringBuf name);

    bool ApplyPatch(EUserAttributesOp op, const NKikimrSchemeOp::TAlterUserAttributes& patch, TString& errStr);

    template <class TContainer>
    bool ApplyPatch(EUserAttributesOp op, const TContainer& patch, TString& errStr) {
        for (const auto& item : patch) {
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

    void Set(const TString& name, const TString& value);
    ui32 Size() const;
    ui64 Bytes() const;
    bool CheckLimits(TString& errStr) const;

    static bool CheckAttribute(EUserAttributesOp op, const TString& name, const TString& value, TString& errStr);
    static bool CheckAttributeRemove(EUserAttributesOp op, const TString& name, TString& errStr);
    static bool CheckValueStringWeak(const TString& name, const TString& value, TString& errStr);
    static bool CheckValueUint64(const TString& name, const TString& value, TString& errStr, ui64 minValue = 0, ui64 maxValue = Max<ui64>());
    static bool CheckValueJson(const TString& name, const TString& value, TString& errStr);
};

}
