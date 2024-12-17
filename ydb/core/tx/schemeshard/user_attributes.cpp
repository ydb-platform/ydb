#include "user_attributes.h"
#include "schemeshard_user_attr_limits.h"

#include <library/cpp/json/json_reader.h>

#include <util/string/cast.h>
#include <util/string/printf.h>

namespace NKikimr::NSchemeShard {

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
    for (auto c : name) {
        if (!WeakCheck(c) || c == '/') {
            return false;
        }
    }
    return true;
}

    TUserAttributes::TUserAttributes(ui64 version)
        : AlterVersion(version)
    {}

    TUserAttributes::TPtr TUserAttributes::CreateNextVersion() const {
        auto result = new TUserAttributes(*this);
        ++result->AlterVersion;
        return result;
    }

    EAttribute TUserAttributes::ParseName(TStringBuf name) {
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
                HANDLE_ATTR(FILESTORE_SPACE_LIMIT_SSD);
                HANDLE_ATTR(FILESTORE_SPACE_LIMIT_HDD);
                HANDLE_ATTR(EXTRA_PATH_SYMBOLS_ALLOWED);
                HANDLE_ATTR(DOCUMENT_API_VERSION);
                HANDLE_ATTR(ASYNC_REPLICATION);
                HANDLE_ATTR(ASYNC_REPLICA);
                HANDLE_ATTR(INCREMENTAL_BACKUP);

            #undef HANDLE_ATTR
            return EAttribute::UNKNOWN;
        }

        return EAttribute::USER;
    }

    bool TUserAttributes::ApplyPatch(EUserAttributesOp op, const NKikimrSchemeOp::TAlterUserAttributes& patch, TString& errStr) {
        return ApplyPatch(op, patch.GetUserAttributes(), errStr);
    }

    void TUserAttributes::Set(const TString& name, const TString& value) {
        Attrs[name] = value;
    }

    ui32 TUserAttributes::Size() const {
        return Attrs.size();
    }

    ui64 TUserAttributes::Bytes() const {
        ui64 bytes = 0;

        for (const auto& [key, value] : Attrs) {
            bytes += key.size();
            bytes += value.size();
        }

        return bytes;
    }

    bool TUserAttributes::CheckLimits(TString& errStr) const {
        const ui64 bytes = Bytes();
        if (bytes > TUserAttributesLimits::MaxBytes) {
            errStr = Sprintf("UserAttributes::CheckLimits: user attributes too big: %" PRIu64, bytes);
            return false;
        }

        return true;
    }

    bool TUserAttributes::CheckAttribute(EUserAttributesOp op, const TString& name, const TString& value, TString& errStr) {
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
            case EAttribute::FILESTORE_SPACE_LIMIT_SSD:
            case EAttribute::FILESTORE_SPACE_LIMIT_HDD:
                return CheckValueUint64(name, value, errStr);
            case EAttribute::EXTRA_PATH_SYMBOLS_ALLOWED:
                return CheckValueStringWeak(name, value, errStr);
            case EAttribute::DOCUMENT_API_VERSION:
                if (op != EUserAttributesOp::CreateTable) {
                    errStr = Sprintf("UserAttributes: attribute '%s' can only be set during CreateTable", name.c_str());
                    return false;
                }
                return CheckValueUint64(name, value, errStr, /* minValue = */ 1);
            case EAttribute::ASYNC_REPLICATION:
                if (op != EUserAttributesOp::CreateChangefeed) {
                    errStr = Sprintf("UserAttributes: attribute '%s' can only be set during CreateChangefeed", name.c_str());
                    return false;
                }
                return CheckValueJson(name, value, errStr);
            case EAttribute::ASYNC_REPLICA:
                errStr = Sprintf("UserAttributes: attribute '%s' cannot be set", name.c_str());
                return false;
            case EAttribute::INCREMENTAL_BACKUP:
                // TODO(enjection): check ops
                return CheckValueJson(name, value, errStr);
        }

        Y_UNREACHABLE();
    }

    bool TUserAttributes::CheckAttributeRemove(EUserAttributesOp op, const TString& name, TString& errStr) {
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
            case EAttribute::FILESTORE_SPACE_LIMIT_SSD:
            case EAttribute::FILESTORE_SPACE_LIMIT_HDD:
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
            case EAttribute::ASYNC_REPLICA:
                errStr = Sprintf("UserAttributes: attribute '%s' cannot be set", name.c_str());
                return false;
            case EAttribute::INCREMENTAL_BACKUP:
                // TODO(enjection): check ops
                return true;
        }

        Y_UNREACHABLE();
    }

    bool TUserAttributes::CheckValueStringWeak(const TString& name, const TString& value, TString& errStr) {
        if (!IsValidPathName_WeakCheck(value)) {
            errStr = Sprintf("UserAttributes: attribute '%s' has invalid value '%s', forbidden symbols are found",
                                name.c_str(), value.c_str());
            return false;
        }
        return true;
    }

    bool TUserAttributes::CheckValueUint64(const TString& name, const TString& value, TString& errStr, ui64 minValue, ui64 maxValue) {
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

    bool TUserAttributes::CheckValueJson(const TString& name, const TString& value, TString& errStr) {
        NJson::TJsonValue unused;
        if (!NJson::ReadJsonTree(value, &unused)) {
            errStr = Sprintf("UserAttributes: attribute '%s' has invalid value '%s'",
                name.c_str(), value.c_str());
            return false;
        }
        return true;
    }

}
