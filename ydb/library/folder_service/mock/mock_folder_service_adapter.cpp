#include <ydb/library/folder_service/mock/mock_folder_service_adapter.h>
#include <ydb/library/folder_service/events.h>
#include <library/cpp/actors/core/hfunc.h>

namespace NKikimr::NFolderService {

class TFolderServiceAdapterMock
    : public NActors::TActor<TFolderServiceAdapterMock> {
    using TThis = TFolderServiceAdapterMock;
    using TBase = NActors::TActor<TFolderServiceAdapterMock>;

    using TEvGetCloudByFolderRequest = NKikimr::NFolderService::TEvFolderService::TEvGetCloudByFolderRequest;
    using TEvGetCloudByFolderResponse = NKikimr::NFolderService::TEvFolderService::TEvGetCloudByFolderResponse;

public:
    TFolderServiceAdapterMock()
        : TBase(&TThis::StateWork) {
    }

    void Handle(TEvGetCloudByFolderRequest::TPtr& ev) {
        auto folderId = ev->Get()->FolderId;
        auto result = std::make_unique<TEvGetCloudByFolderResponse>();
        TString cloudId = "mock_cloud";
        auto p = folderId.find('@');
        if (p != folderId.npos) {
            cloudId = folderId.substr(p + 1);
        }
        result->FolderId = folderId;
        result->CloudId = cloudId;

        result->Status = NGrpc::TGrpcStatus();
        Send(ev->Sender, result.release());
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvGetCloudByFolderRequest, Handle)
            cFunc(NActors::TEvents::TEvPoisonPill::EventType, PassAway)
        }
    }
};

NActors::IActor* CreateMockFolderServiceAdapterActor(const NKikimrProto::NFolderService::TFolderServiceConfig&) {
    return new TFolderServiceAdapterMock();
}
} // namespace NKikimr::NFolderService
