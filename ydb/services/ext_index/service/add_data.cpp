#include "add_data.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::EXT_INDEX

namespace NKikimr::NCSIndex {

void TDataUpserter::OnDescriptionSuccess(NMetadata::NProvider::TTableInfo&& result, const TString& /*requestId*/) {
    Y_ABORT_UNLESS(SelfContainer);
    const std::vector<TString> pkFields = result.GetPKFieldNames();
    AtomicCounter.Inc();
    for (auto&& i : Indexes) {
        if (i.IsDelete()) {
            YDB_LOG_WARN("Extractor is removing");
        } else if (!i.IsActive()) {
            YDB_LOG_WARN("Extractor not active yet");
        } else {
            YDB_LOG_DEBUG("Add data");
            AtomicCounter.Inc();
            TActivationContext::ActorSystem()->Register(new TIndexUpsertActor(Data, i, pkFields, DatabaseName, i.GetIndexTablePath(), SelfContainer));
        }
    }
    OnIndexUpserted();
}

void TDataUpserter::Start(std::shared_ptr<TDataUpserter> selfContainer) {
    if (Indexes.empty()) {
        ExternalController->OnAllIndexesUpserted();
    } else {
        for (auto&& i : Indexes) {
            if (Indexes.front().GetTablePath() != i.GetTablePath()) {
                ExternalController->OnAllIndexesUpsertionFailed("inconsistency tables list in indexes");
                return;
            }
        }
        SelfContainer = selfContainer;
        TActivationContext::ActorSystem()->Register(new NMetadata::NProvider::TSchemeDescriptionActor(SelfContainer, "", Indexes.front().GetTablePath()));
    }
}

}
