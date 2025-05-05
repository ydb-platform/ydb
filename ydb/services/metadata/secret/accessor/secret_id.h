#pragma once
#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/library/accessor/accessor.h>
#include <util/generic/overloaded.h>

namespace NKikimr::NMetadata::NSecret {

class TSecretId {
private:
    YDB_READONLY_PROTECT_DEF(TString, OwnerUserId);
    YDB_READONLY_PROTECT_DEF(TString, SecretId);

public:
    inline static const TString PrefixWithUser = "USId:";

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
            return TStringBuilder() << PrefixWithUser << (proto.GetSecretOwnerId() ? proto.GetSecretOwnerId() : defaultOwnerId) << ":" << SecretId;
        }
    }

    bool operator<(const TSecretId& item) const {
        return std::tie(OwnerUserId, SecretId) < std::tie(item.OwnerUserId, item.SecretId);
    }
    bool operator==(const TSecretId& item) const {
        return std::tie(OwnerUserId, SecretId) == std::tie(item.OwnerUserId, item.SecretId);
    }
};

class TSecretName {
private:
    YDB_READONLY_DEF(TString, SecretId);

public:
    inline static const TString PrefixNoUser = "SId:";

    TSecretName() = default;
    TSecretName(const TString& secretId) : SecretId(secretId) {}

    TString SerializeToString() const {
        return TStringBuilder() << "SId:" << SecretId;
    }

    bool DeserializeFromString(const TString& secretString) {
        if (secretString.StartsWith(PrefixNoUser)) {
            SecretId = secretString.substr(PrefixNoUser.size());
            return true;
        }
        return false;
    }
};

class TSecretIdOrValue {
private:
    using TState = std::variant<std::monostate, TSecretId, TSecretName, TString>;
    YDB_READONLY_DEF(TState, State);

private:
    TSecretIdOrValue() = default;

    bool DeserializeFromStringImpl(const TString& info, const TString& defaultUserId = "") {
        if (info.StartsWith(TSecretId::PrefixWithUser)) {
            TStringBuf sb(info.data(), info.size());
            sb.Skip(TSecretId::PrefixWithUser.size());
            TStringBuf uId;
            TStringBuf sId;
            if (!sb.TrySplit(':', uId, sId)) {
                return false;
            }
            if (!uId || !sId) {
                return false;
            }
            State = TSecretId(uId, sId);
        } else if (info.StartsWith(TSecretName::PrefixNoUser)) {
            TStringBuf sb(info.data(), info.size());
            sb.Skip(TSecretName::PrefixNoUser.size());
            if (!sb) {
                return false;
            }
            if (defaultUserId) {
                State = TSecretId(defaultUserId, TString(sb));
            } else {
                State = TSecretName(TString(sb));
            }
        } else {
            State = info;
        }
        return true;
    }

    explicit TSecretIdOrValue(const TSecretId& id)
        : State(id) {
    }
    explicit TSecretIdOrValue(const TSecretName& id)
        : State(id) {
    }
    explicit TSecretIdOrValue(const TString& value)
        : State(value) {
    }

public:
    bool operator!() const {
        return std::holds_alternative<std::monostate>(State);
    }

    static TSecretIdOrValue BuildAsValue(const TString& value) {
        return TSecretIdOrValue(value);
    }

    static TSecretIdOrValue BuildEmpty() {
        return TSecretIdOrValue();
    }

    static TSecretIdOrValue BuildAsId(const TSecretId& id) {
        return TSecretIdOrValue(id);
    }

    static TSecretIdOrValue BuildAsId(const TSecretName& id) {
        return TSecretIdOrValue(id);
    }

    static std::optional<TSecretIdOrValue> DeserializeFromOptional(
        const NKikimrSchemeOp::TSecretableVariable& proto, const TString& secretInfo, const TString& defaultOwnerId = Default<TString>()) {
        if (proto.HasSecretId()) {
            return DeserializeFromProto(proto, defaultOwnerId);
        } else if (proto.HasValue()) {
            return DeserializeFromString(proto.GetValue().GetData());
        }
        if (secretInfo) {
            return DeserializeFromString(secretInfo, defaultOwnerId);
        } else {
            return {};
        }
    }

    NKikimrSchemeOp::TSecretableVariable SerializeToProto() const {
        NKikimrSchemeOp::TSecretableVariable result;
        std::visit(TOverloaded(
            [](std::monostate){ },
            [&result](const TSecretId& id){
                result.MutableSecretId()->SetId(id.GetSecretId());
                result.MutableSecretId()->SetOwnerId(id.GetOwnerUserId());
            },
            [&result](const TSecretName& name){
                result.MutableSecretId()->SetId(name.GetSecretId());
            },
            [&result](const TString& value){
                result.MutableValue()->SetData(value);
            }
        ),
        State);
        return result;
    }

    static std::optional<TSecretIdOrValue> DeserializeFromProto(
        const NKikimrSchemeOp::TSecretableVariable& proto, const TString& defaultOwnerId = Default<TString>()) {
        if (proto.HasSecretId()) {
            TString ownerId;
            TString secretId;
            if (!proto.GetSecretId().HasOwnerId() || !proto.GetSecretId().GetOwnerId()) {
                ownerId = defaultOwnerId;
            } else {
                ownerId = proto.GetSecretId().GetOwnerId();
            }
            secretId = proto.GetSecretId().GetId();
            if (!ownerId || !secretId) {
                return {};
            }
            return TSecretIdOrValue::BuildAsId(TSecretId(ownerId, secretId));
        } else if (proto.HasValue()) {
            return TSecretIdOrValue::BuildAsValue(proto.GetValue().GetData());
        } else {
            return {};
        }
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
        return std::visit(TOverloaded(
            [](std::monostate) -> TString{
                return "";
            },
            [](const TSecretId& id) -> TString{
                return TStringBuilder() << TSecretId::PrefixWithUser << id.GetOwnerUserId() << ":" << id.GetSecretId();
            },
            [](const TSecretName& name) -> TString{
                return TStringBuilder() << TSecretName::PrefixNoUser << name.GetSecretId();
            },
            [](const TString& value) -> TString{
                return value;
            }
        ),
        State);
    }

    TString DebugString() const;
};
}   // namespace NKikimr::NMetadata::NSecret
