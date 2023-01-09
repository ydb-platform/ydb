#include "initializer_actor.h"

namespace NKikimr::NMetadata::NProvider {

void TInitializerSnapshotReader::Handle(TDSAccessorSimple::TEvController::TEvResult::TPtr& ev) {
    RegistrationData->StartInitialization();
    RegistrationData->SetInitializationSnapshot(ev->Get()->GetResult());
}

void TInitializerSnapshotReader::Handle(TEvStartMetadataService::TPtr& /*ev*/) {
    Register(new TDSAccessorSimple(ReqConfig, InternalController, RegistrationData->GetInitializationFetcher()));
}

void TInitializerSnapshotReader::Handle(TDSAccessorSimple::TEvController::TEvError::TPtr& ev) {
    ALS_ERROR(NKikimrServices::METADATA_PROVIDER) << "cannot receive initializer snapshot: " << ev->Get()->GetErrorMessage() << Endl;
    Schedule(TDuration::Seconds(1), new TEvStartMetadataService());
}

void TInitializerSnapshotReader::Handle(TDSAccessorSimple::TEvController::TEvTableAbsent::TPtr& /*ev*/) {
    RegistrationData->NoInitializationSnapshot();
}

void TInitializerSnapshotReader::Bootstrap() {
    InternalController = std::make_shared<TInitializerSnapshotReaderController>(SelfId());
    Become(&TInitializerSnapshotReader::StateMain);
    Sender<TEvStartMetadataService>().SendTo(SelfId());
}

}
