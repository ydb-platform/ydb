#include "mock_table.h"

#include <grpcpp/support/sync_stream.h>

namespace {

class TReadTableService : public TMockTableService {
public:
    grpc::Status StreamReadTable(
        grpc::ServerContext*,
        const Ydb::Table::ReadTableRequest* request,
        grpc::ServerWriter<Ydb::Table::ReadTableResponse>* writer
    ) override {
        ++ReadCalls;
        CHECK_EXP(request->path() == TEST_DATABASE + "/orders", "Unexpected table path: " << request->path());
        CHECK_EXP(request->ordered() == ExpectedOrdered,
            "Expected ReadTableRequest.ordered=" << ExpectedOrdered << ", got " << request->ordered());

        Ydb::Table::ReadTableResponse response;
        response.set_status(Ydb::StatusIds::SUCCESS);
        auto* resultSet = response.mutable_result()->mutable_result_set();
        auto* column = resultSet->add_columns();
        column->set_name("id");
        column->mutable_type()->set_type_id(Ydb::Type::UINT64);
        resultSet->add_rows()->add_items()->set_uint64_value(42);
        CHECK_EXP(writer->Write(response), "Failed to stream the result to the CLI");
        return grpc::Status::OK;
    }

    void CheckExpectations() override {
        CHECK_EXP(ReadCalls == 1, "Expected one ReadTable call, got " << ReadCalls);
        TChecker::CheckExpectations();
    }

    void ClearExpectations() override {
        ReadCalls = 0;
        ExpectedOrdered = false;
    }

    bool ExpectedOrdered = false;

private:
    size_t ReadCalls = 0;
};

class TReadTableFixture : public TCliTestFixture {
    void AddServices() override {
        TCliTestFixture::AddServices();
        AddService<TReadTableService>();
    }
};

} // namespace

Y_UNIT_TEST_SUITE(ReadTableTest) {
    Y_UNIT_TEST_F(PassesOrderedFlag, TReadTableFixture) {
        TList<TString> args = {"-e", GetEndpoint(), "-d", GetDatabase(), "table", "readtable", "orders", "--count-only"};
        UNIT_ASSERT_VALUES_EQUAL(RunCli(args), "1\n");

        Service<TReadTableService>().ExpectedOrdered = true;
        args.emplace_back("--ordered");
        UNIT_ASSERT_VALUES_EQUAL(RunCli(args), "1\n");
    }
}
