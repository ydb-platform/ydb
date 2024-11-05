#include <ydb/library/folder_service/mock/mock_folder_service_adapter.h>
#include <ydb/library/folder_service/events.h>
#include <ydb/library/actors/core/hfunc.h>

static const TString MOCK_CLOUD_ID = "mock_cloud";

namespace NKikimr::NFolderService {

class TFolderServiceAdapterMock
    : public NActors::TActor<TFolderServiceAdapterMock> {
    using TThis = TFolderServiceAdapterMock;
    using TBase = NActors::TActor<TFolderServiceAdapterMock>;

    using TEvGetCloudByFolderRequest = NKikimr::NFolderService::TEvFolderService::TEvGetCloudByFolderRequest;
    using TEvGetCloudByFolderResponse = NKikimr::NFolderService::TEvFolderService::TEvGetCloudByFolderResponse;

public:
    TFolderServiceAdapterMock(TMaybe<TString> mockedCloudId)
        : TBase(&TThis::StateWork)
        , MockedCloudId(mockedCloudId.Empty()
            ? MOCK_CLOUD_ID
            : (mockedCloudId.Get()->empty() ? MOCK_CLOUD_ID : mockedCloudId.GetRef()))
    {
    }

    void Handle(TEvGetCloudByFolderRequest::TPtr& ev) {
        auto folderId = ev->Get()->FolderId;
        auto result = std::make_unique<TEvGetCloudByFolderResponse>();
        TString cloudId = MockedCloudId;
        auto p = folderId.find('@');
        if (p != folderId.npos) {
            cloudId = folderId.substr(p + 1);
        }
        result->FolderId = folderId;
        result->CloudId = cloudId;

        result->Status = NYdbGrpc::TGrpcStatus();
        Send(ev->Sender, result.release());
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvGetCloudByFolderRequest, Handle)
            cFunc(NActors::TEvents::TEvPoisonPill::EventType, PassAway)
        }
    }

private:
    TString MockedCloudId;
};

NActors::IActor* CreateMockFolderServiceAdapterActor(
        const NKikimrProto::NFolderService::TFolderServiceConfig&,
        const TMaybe<TString> mockedCloudId) {
    return new TFolderServiceAdapterMock(mockedCloudId);
}
} // namespace NKikimr::NFolderService
