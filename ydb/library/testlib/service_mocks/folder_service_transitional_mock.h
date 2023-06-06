#pragma once

#include <ydb/public/api/client/yc_private/resourcemanager/transitional/folder_service.grpc.pb.h>

class TFolderServiceTransitionalMock: public yandex::cloud::priv::resourcemanager::v1::transitional::FolderService::Service {
public:
    THashMap<TString, yandex::cloud::priv::resourcemanager::v1::Folder> Folders;

    virtual grpc::Status List(grpc::ServerContext*,
                              const yandex::cloud::priv::resourcemanager::v1::transitional::ListFoldersRequest* request,
                              yandex::cloud::priv::resourcemanager::v1::transitional::ListFoldersResponse* response) override {
        TString key = request->id();
        auto it = Folders.find(key);
        if (it != Folders.end()) {
            response->add_result()->CopyFrom(it->second);
            return grpc::Status::OK;
        } else {
            return grpc::Status(grpc::StatusCode::NOT_FOUND, "Not Found");
        }
    }
};
