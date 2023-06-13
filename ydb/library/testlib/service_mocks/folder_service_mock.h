#pragma once

#include <ydb/public/api/client/yc_private/resourcemanager/folder_service.grpc.pb.h>

class TFolderServiceMock : public yandex::cloud::priv::resourcemanager::v1::FolderService::Service {
public:
    THashMap<TString, yandex::cloud::priv::resourcemanager::v1::ResolvedFolder> Folders;

    virtual grpc::Status Resolve(
            grpc::ServerContext*,
            const yandex::cloud::priv::resourcemanager::v1::ResolveFoldersRequest* request,
            yandex::cloud::priv::resourcemanager::v1::ResolveFoldersResponse* response) override {
        TString key = request->folder_ids(0);
        auto it = Folders.find(key);
        if (it != Folders.end()) {
            response->add_resolved_folders()->CopyFrom(it->second);
            return grpc::Status::OK;
        } else {
            return grpc::Status(grpc::StatusCode::NOT_FOUND, "Not Found");
        }
    }
};

