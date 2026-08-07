#pragma once

#include <ydb/library/testlib/service_mocks/common.h>

#include <ydb/public/api/client/yc_private/resourcemanager/transitional/folder_service.grpc.pb.h>

#include <util/system/mutex.h>

class TFolderServiceTransitionalMock: public yandex::cloud::priv::resourcemanager::v1::transitional::FolderService::Service {
public:
    THashMap<TString, yandex::cloud::priv::resourcemanager::v1::Folder> Folders;
    TMutex MetadataMutex;
    TString CapturedUserAgent;

    virtual grpc::Status List(grpc::ServerContext* context,
                              const yandex::cloud::priv::resourcemanager::v1::transitional::ListFoldersRequest* request,
                              yandex::cloud::priv::resourcemanager::v1::transitional::ListFoldersResponse* response) override {
        with_lock (MetadataMutex) {
            CapturedUserAgent = NTestUtils::CaptureUserAgent(context);
        }

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
