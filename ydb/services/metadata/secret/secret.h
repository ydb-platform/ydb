#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/base/appdata.h>

#include <ydb/services/metadata/abstract/decoder.h>
#include <ydb/services/metadata/manager/object.h>
#include <ydb/services/metadata/manager/preparation_controller.h>

namespace NKikimr::NMetadata::NSecret {

class TSecretId {
private:
    YDB_READONLY_PROTECT_DEF(TString, OwnerUserId);
    YDB_READONLY_PROTECT_DEF(TString, SecretId);
public:
    TSecretId() = default;
    TSecretId(const TString& ownerUserId, const TString& secretId)
        : OwnerUserId(ownerUserId)
        , SecretId(secretId) {
    }

    TSecretId(const TStringBuf ownerUserId, const TStringBuf secretId)
        : OwnerUserId(ownerUserId)
        , SecretId(secretId) {
    }

    TString SerializeToString() const;

    template <class TProto>
    TString BuildSecretAccessString(const TProto& proto, const TString& defaultOwnerId) {
        if (proto.HasValue()) {
            return proto.GetValue();
        } else {
            return TStringBuilder() << "USId:" << (proto.GetSecretOwnerId() ? proto.GetSecretOwnerId() : defaultOwnerId) << ":" << SecretId;
        }
    }

    bool operator<(const TSecretId& item) const {
        return std::tie(OwnerUserId, SecretId) < std::tie(item.OwnerUserId, item.SecretId);
    }
    bool operator==(const TSecretId& item) const {
        return std::tie(OwnerUserId, SecretId) == std::tie(item.OwnerUserId, item.SecretId);
    }
};

class TSecretIdOrValue {
private:
    YDB_READONLY_DEF(std::optional<TSecretId>, SecretId);
    YDB_READONLY_DEF(std::optional<TString>, Value);
    TSecretIdOrValue() = default;

    bool DeserializeFromStringImpl(const TString& info, const TString& defaultUserId) {
        static const TString prefixWithUser = "USId:";
        static const TString prefixNoUser = "SId:";
        if (info.StartsWith(prefixWithUser)) {
            TStringBuf sb(info.data(), info.size());
            sb.Skip(prefixWithUser.size());
            TStringBuf uId;
            TStringBuf sId;
            if (!sb.TrySplit(':', uId, sId)) {
                return false;
            }
            if (!uId || !sId) {
                return false;
            }
            SecretId = TSecretId(uId, sId);
        } else if (info.StartsWith(prefixNoUser)) {
            TStringBuf sb(info.data(), info.size());
            sb.Skip(prefixNoUser.size());
            SecretId = TSecretId(defaultUserId, TString(sb));
            if (!sb || !defaultUserId) {
                return false;
            }
        } else {
            Value = info;
        }
        return true;
    }
public:
    TSecretIdOrValue(const TSecretId& id)
        : SecretId(id) {

    }

    TSecretIdOrValue(const TString& value)
        : Value(value) {

    }

    static std::optional<TSecretIdOrValue> DeserializeFromString(const TString& info, const TString& defaultOwnerId = Default<TString>()) {
        TSecretIdOrValue result;
        if (!result.DeserializeFromStringImpl(info, defaultOwnerId)) {
            return {};
        } else {
            return result;
        }
    }

    TString SerializeToString() const {
        if (SecretId) {
            return SecretId->SerializeToString();
        } else if (Value) {
            return *Value;
        }
        Y_VERIFY(false);
        return "";
    }
};

class TSecret: public TSecretId, public NModifications::TObject<TSecret> {
private:
    using TBase = TSecretId;
    YDB_ACCESSOR_DEF(TString, Value);
public:
    static IClassBehaviour::TPtr GetBehaviour();

    class TDecoder: public NInternal::TDecoderBase {
    private:
        YDB_ACCESSOR(i32, OwnerUserIdIdx, -1);
        YDB_ACCESSOR(i32, SecretIdIdx, -1);
        YDB_ACCESSOR(i32, ValueIdx, -1);
    public:
        static inline const TString OwnerUserId = "ownerUserId";
        static inline const TString SecretId = "secretId";
        static inline const TString Value = "value";

        TDecoder(const Ydb::ResultSet& rawData) {
            OwnerUserIdIdx = GetFieldIndex(rawData, OwnerUserId);
            SecretIdIdx = GetFieldIndex(rawData, SecretId);
            ValueIdx = GetFieldIndex(rawData, Value);
        }
    };

    using TBase::TBase;

    bool DeserializeFromRecord(const TDecoder& decoder, const Ydb::Value& rawValue);
    NInternal::TTableRecord SerializeToRecord() const;
    static TString GetTypeId() {
        return "SECRET";
    }

};

}
