#include "mock_env.h"

#include <ydb/public/api/grpc/ydb_import_v1.grpc.pb.h>

class TImportImpl : public TMockGrpcServiceBase<Ydb::Import::V1::ImportService::Service> {
};

class TImportFixture : public TCliTestFixture {
    void AddServices() override {
        TCliTestFixture::AddServices();
        AddService<TImportImpl>();
    }
};

Y_UNIT_TEST_SUITE(ImportTest) {
}
