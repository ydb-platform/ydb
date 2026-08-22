#include "initializer.h"
#include "object.h"

#include <ydb/library/table_creator/table_creator.h>

namespace NKikimr::NKqp {

namespace {

using namespace NMetadata::NInitializer;

class TStreamingQueriesTablesCreator final : public NTableCreator::TMultiTableCreator {
    using TBase = NTableCreator::TMultiTableCreator;

public:
    TStreamingQueriesTablesCreator(const TString& modificationId, IModifierExternalController::TPtr externalController)
        : TBase({GetStreamingQueriesCreator()})
        , ModificationId(modificationId)
        , ExternalController(externalController)
    {}

protected:
    static IActor* GetStreamingQueriesCreator() {
        NACLib::TDiffACL acl;
        acl.ClearAccess();
        acl.SetInterruptInheritance(AppData()->FeatureFlags.GetEnableSecureScriptExecutions());

        return CreateTableCreator(
            SplitPath(TStreamingQueryConfig::GetTablesPath()),
            {
                Col(TStreamingQueryConfig::TColumns::DatabaseId, NScheme::NTypeIds::Utf8),
                Col(TStreamingQueryConfig::TColumns::QueryPath, NScheme::NTypeIds::Utf8),
                Col(TStreamingQueryConfig::TColumns::State, NScheme::NTypeIds::Json),
            },
            { TStreamingQueryConfig::TColumns::DatabaseId, TStreamingQueryConfig::TColumns::QueryPath },
            NKikimrServices::KQP_PROXY,
            {},
            {},
            /* isSystemUser */ true,
            Nothing(),
            acl
        );
    }

    void OnTablesCreated(bool success, NYql::TIssues issues) final {
        if (success) {
            ExternalController->OnModificationFinished(ModificationId);
        } else {
            ExternalController->OnModificationFailed(Ydb::StatusIds::INTERNAL_ERROR, issues.ToString(), ModificationId);
        }
    }

private:
    const TString ModificationId;
    const IModifierExternalController::TPtr ExternalController;
};

class TStreamingQueriesTablesInitializer final : public ITableModifier {
    static constexpr char MODIFICATION_ID[] = "create-generic";

public:
    TStreamingQueriesTablesInitializer()
        : ITableModifier(MODIFICATION_ID, /* supportDbCache */ false)
    {}

protected:
    bool DoExecute(IModifierExternalController::TPtr externalController, const NMetadata::NRequest::TConfig& config) const final {
        Y_UNUSED(config);

        TActivationContext::Register(new TStreamingQueriesTablesCreator(MODIFICATION_ID, externalController));
        return true;
    }
};

}  // anonymous namespace

void TStreamingQueryInitializer::DoPrepare(IInitializerInput::TPtr controller) const {
    controller->OnPreparationFinished({std::make_shared<TStreamingQueriesTablesInitializer>()});
}

}  // namespace NKikimr::NKqp
