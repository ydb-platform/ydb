#pragma once

#include <ydb/library/testlib/service_mocks/common.h>

#include <ydb/public/api/client/yc_private/resourcemanager/folder_service.grpc.pb.h>

#include <util/system/mutex.h>

class TFolderServiceMock : public yandex::cloud::priv::resourcemanager::v1::FolderService::Service {
public:
    THashMap<TString, yandex::cloud::priv::resourcemanager::v1::ResolvedFolder> Folders;
    THashSet<TString> NoAnswerFolders;
    TMutex MetadataMutex;
    TString CapturedUserAgent;

    virtual grpc::Status Resolve(
            grpc::ServerContext* context,
            const yandex::cloud::priv::resourcemanager::v1::ResolveFoldersRequest* request,
            yandex::cloud::priv::resourcemanager::v1::ResolveFoldersResponse* response) override {
        with_lock (MetadataMutex) {
            CapturedUserAgent = NTestUtils::CaptureUserAgent(context);
        }

        TString key = request->folder_ids(0);
        if (NoAnswerFolders.contains(key)) {
            return grpc::Status::OK;
        }
        auto it = Folders.find(key);
        if (it != Folders.end()) {
            response->add_resolved_folders()->CopyFrom(it->second);
            return grpc::Status::OK;
        } else {
            return grpc::Status(grpc::StatusCode::NOT_FOUND, "Not Found");
        }
    }
};
