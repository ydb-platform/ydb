#include "add_data.h"

namespace NKikimr::NCSIndex {

void TDataUpserter::OnDescriptionSuccess(NMetadata::NProvider::TTableInfo&& result, const TString& /*requestId*/) {
    Y_ABORT_UNLESS(SelfContainer);
    const std::vector<TString> pkFields = result.GetPKFieldNames();
    AtomicCounter.Inc();
    for (auto&& i : Indexes) {
        if (i.IsDelete()) {
            ALS_WARN(NKikimrServices::EXT_INDEX) << "extractor is removing";
        } else if (!i.IsActive()) {
            ALS_WARN(NKikimrServices::EXT_INDEX) << "extractor not active yet";
        } else {
            ALS_DEBUG(NKikimrServices::EXT_INDEX) << "add data";
            AtomicCounter.Inc();
            TActivationContext::ActorSystem()->Register(new TIndexUpsertActor(Data, i, pkFields, i.GetIndexTablePath(), SelfContainer));
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
