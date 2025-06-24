#include "checker_access.h"
#include "checker_secret.h"
#include "manager.h"

#include <ydb/core/base/path.h>
#include <ydb/services/metadata/manager/ydb_value_operator.h>

#include <util/string/vector.h>

namespace NKikimr::NMetadata::NSecret {

class TCheckSecretNameUnique: public NModifications::TModificationStage {
private:
    THashMap<TString, TString> SecretNameToOwner;

    static Ydb::Table::ExecuteDataQueryRequest BuildRequest(std::vector<TSecretId> secrets) {
        std::vector<TString> secretNameLiterals;
        for (const auto& id : secrets) {
            secretNameLiterals.push_back(TStringBuilder() << '"' << id.GetSecretId() << '"');
        }

        Ydb::Table::ExecuteDataQueryRequest request;
        TStringBuilder sb;
        sb << "SELECT " + TSecret::TDecoder::SecretId + ", " + TSecret::TDecoder::OwnerUserId + ", " + TSecret::TDecoder::Value << Endl;
        sb << "FROM `" + TSecret::GetBehaviour()->GetStorageTablePath() + "`" << Endl;
        sb << "VIEW index_by_secret_id" << Endl;
        sb << "WHERE " + TSecret::TDecoder::SecretId + " IN (" + JoinStrings(secretNameLiterals.begin(), secretNameLiterals.end(), ", ") + ")"
           << Endl;
        AFL_DEBUG(NKikimrServices::METADATA_SECRET)("event", "build_precondition")("sql", sb);
        request.mutable_query()->set_yql_text(sb);
        return request;
    }

public:
    TConclusionStatus HandleResult(const Ydb::Table::ExecuteQueryResult& result) const override {
        AFL_VERIFY(result.result_sets_size() == 1)("size", result.result_sets_size());
        const auto& resultSet = result.result_sets(0);
        TSecret::TDecoder decoder(resultSet);

        for (const auto& row : resultSet.rows()) {
            TSecret secret;
            AFL_VERIFY(secret.DeserializeFromRecord(decoder, row));
            auto findOwner = SecretNameToOwner.FindPtr(secret.GetSecretId());
            AFL_VERIFY(findOwner);
            if (*findOwner != secret.GetOwnerUserId()) {
                return TConclusionStatus::Fail("Secret already exists: " + secret.GetSecretId());
            }
        }

        return TConclusionStatus::Success();
    }

    TCheckSecretNameUnique(std::vector<TSecretId> secrets)
        : TModificationStage(BuildRequest(secrets)) {
        for (const auto& id : secrets) {
            AFL_VERIFY(SecretNameToOwner.emplace(id.GetSecretId(), id.GetOwnerUserId()).second);
        }
    }
};

void TAccessManager::DoPrepareObjectsBeforeModification(std::vector<TAccess>&& patchedObjects, NModifications::IAlterPreparationController<TAccess>::TPtr controller,
    const TInternalModificationContext& context, const NMetadata::NModifications::TAlterOperationContext& /*alterContext*/) const {
    if (context.GetActivityType() == IOperationsManager::EActivityType::Alter) {
        controller->OnPreparationProblem("access object cannot be modified");
        return;
    }
    if (!!context.GetExternalData().GetUserToken()) {
        for (auto&& i : patchedObjects) {
            if (i.GetOwnerUserId() != context.GetExternalData().GetUserToken()->GetUserSID()) {
                controller->OnPreparationProblem("no permissions for modify secret access");
                return;
            }
        }
    }
    TActivationContext::Register(new TAccessPreparationActor(std::move(patchedObjects), controller, context));
}

NModifications::TOperationParsingResult TAccessManager::DoBuildPatchFromSettings(const NYql::TObjectSettingsImpl& settings,
    TInternalModificationContext& context) const {
    NInternal::TTableRecord result;
    TStringBuf sb(settings.GetObjectId().data(), settings.GetObjectId().size());
    TStringBuf l;
    TStringBuf r;
    if (!sb.TrySplit(':', l, r)) {
        return TConclusionStatus::Fail("incorrect objectId format (secretId:accessSID)");
    }
    result.SetColumn(TAccess::TDecoder::SecretId, NInternal::TYDBValue::Utf8(l));
    result.SetColumn(TAccess::TDecoder::AccessSID, NInternal::TYDBValue::Utf8(r));
    if (!context.GetExternalData().GetUserToken()) {
        auto fValue = settings.GetFeaturesExtractor().Extract(TAccess::TDecoder::OwnerUserId);
        if (fValue) {
            result.SetColumn(TAccess::TDecoder::OwnerUserId, NInternal::TYDBValue::Utf8(*fValue));
        } else {
            return TConclusionStatus::Fail("OwnerUserId not defined");
        }
    } else {
        result.SetColumn(TAccess::TDecoder::OwnerUserId, NInternal::TYDBValue::Utf8(context.GetExternalData().GetUserToken()->GetUserSID()));
    }
    return result;
}

NModifications::TOperationParsingResult TSecretManager::DoBuildPatchFromSettings(const NYql::TObjectSettingsImpl& settings,
    TInternalModificationContext& context) const {
    static const TString ExtraPathSymbolsAllowed = "!\"#$%&'()*+,-.:;<=>?@[\\]^_`{|}~";
    NInternal::TTableRecord result;
    if (!context.GetExternalData().GetUserToken()) {
        auto fValue = settings.GetFeaturesExtractor().Extract(TSecret::TDecoder::OwnerUserId);
        if (fValue) {
            result.SetColumn(TSecret::TDecoder::OwnerUserId, NInternal::TYDBValue::Utf8(*fValue));
        } else {
            return TConclusionStatus::Fail("OwnerUserId not defined");
        }
    } else {
        result.SetColumn(TSecret::TDecoder::OwnerUserId, NInternal::TYDBValue::Utf8(context.GetExternalData().GetUserToken()->GetUserSID()));
    }
    const TString secretName{settings.GetObjectId()};
    for (auto&& c : secretName) {
        if (c >= '0' && c <= '9') {
            continue;
        }
        if (c >= 'a' && c <= 'z') {
            continue;
        }
        if (c >= 'A' && c <= 'Z') {
            continue;
        }
        if (ExtraPathSymbolsAllowed.Contains(c)) {
            continue;
        }
        return TConclusionStatus::Fail("incorrect character for secret id: '" + TString(c) + "'");
    }

    const bool requireDbPrefixInSecretName = HasAppData() ? AppData()->FeatureFlags.GetRequireDbPrefixInSecretName() : false;
    const TStringBuf databaseName{ExtractBase(context.GetExternalData().GetDatabase())};
    if (requireDbPrefixInSecretName && !secretName.StartsWith(databaseName)) {
        return TConclusionStatus::Fail(TStringBuilder{} << "Secret name " << secretName << " must start with database name " << databaseName);
    }

    {
        result.SetColumn(TSecret::TDecoder::SecretId, NInternal::TYDBValue::Utf8(secretName));
    }
    {
        auto fValue = settings.GetFeaturesExtractor().Extract(TSecret::TDecoder::Value);
        if (fValue) {
            result.SetColumn(TSecret::TDecoder::Value, NInternal::TYDBValue::Utf8(*fValue));
        }
    }
    return result;
}

void TSecretManager::DoPrepareObjectsBeforeModification(std::vector<TSecret>&& patchedObjects, NModifications::IAlterPreparationController<TSecret>::TPtr controller,
    const TInternalModificationContext& context, const NMetadata::NModifications::TAlterOperationContext& /*alterContext*/) const {
    if (!!context.GetExternalData().GetUserToken()) {
        for (auto&& i : patchedObjects) {
            if (i.GetOwnerUserId() != context.GetExternalData().GetUserToken()->GetUserSID()) {
                controller->OnPreparationProblem("no permissions for modify secrets");
                return;
            }
        }
    }
    TActivationContext::Register(new TSecretPreparationActor(std::move(patchedObjects), controller, context));
}

std::vector<NModifications::TModificationStage::TPtr> TSecretManager::GetPreconditions(
    const std::vector<TSecret>& objects, const IOperationsManager::TInternalModificationContext& context) const {
    if (context.GetActivityType() == NModifications::IOperationsManager::EActivityType::Create ||
        context.GetActivityType() == NModifications::IOperationsManager::EActivityType::Upsert) {
        std::vector<TSecretId> secretIds;
        for (const auto& secret : objects) {
            secretIds.emplace_back(TSecretId(secret.GetOwnerUserId(), secret.GetSecretId()));
        }
        return { std::make_shared<TCheckSecretNameUnique>(std::move(secretIds)) };
    }
    return {};
}
}
