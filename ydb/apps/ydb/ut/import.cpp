#include "mock_env.h"

#include <ydb/public/api/grpc/ydb_import_v1.grpc.pb.h>

class TImportImpl : public TMockGrpcServiceBase<Ydb::Import::V1::ImportService::Service> {
public:
    grpc::Status ImportFromS3(grpc::ServerContext* context, const Ydb::Import::ImportFromS3Request* request, Ydb::Import::ImportFromS3Response* response) override {
        Y_UNUSED(context);
        Y_UNUSED(request);
        return FillImportResponse(response);
    }

    grpc::Status ListObjectsInS3Export(grpc::ServerContext* context, const Ydb::Import::ListObjectsInS3ExportRequest* request, Ydb::Import::ListObjectsInS3ExportResponse* response) override {
        Y_UNUSED(context);
        Y_UNUSED(request);
        return FillListResponse(response);
    }

    grpc::Status FillImportResponse(Ydb::Import::ImportFromS3Response* response) {
        Ydb::Import::ImportFromS3Result res;
        auto* operation = response->mutable_operation();
        operation->set_ready(true);
        operation->set_status(Ydb::StatusIds::SUCCESS);
        operation->set_id("ydb://import/6?id=1&kind=s3");
        operation->mutable_result()->PackFrom(res);
        return grpc::Status();
    }

    grpc::Status FillListResponse(Ydb::Import::ListObjectsInS3ExportResponse* response) {
        Ydb::Import::ListObjectsInS3ExportResult res;
        auto* operation = response->mutable_operation();
        operation->set_ready(true);
        operation->set_status(Ydb::StatusIds::SUCCESS);
        operation->mutable_result()->PackFrom(res);
        return grpc::Status();
    }
};

class TImportFixture : public TCliTestFixture {
    void AddServices() override {
        TCliTestFixture::AddServices();
        AddService<TImportImpl>();
    }
};

Y_UNIT_TEST_SUITE(ImportTest) {
}
