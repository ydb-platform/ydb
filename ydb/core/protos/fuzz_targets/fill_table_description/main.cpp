#include <ydb/core/protos/config.pb.h>
#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/core/base/appdata.h>
#include <ydb/library/actors/testlib/test_runtime.h>

namespace {

class TFuzzActorRuntime final : public NActors::TTestActorRuntimeBase {
public:
    TFuzzActorRuntime()
        : NActors::TTestActorRuntimeBase(1, false)
    {
        NKikimr::TAppData::RandomProvider = RandomProvider;
        NKikimr::TAppData::TimeProvider = TimeProvider;
        Initialize();
    }

protected:
    void InitNodeImpl(TNodeDataBase* node, size_t nodeIndex) override {
        node->AppData0.reset(new NKikimr::TAppData(
            0,
            0,
            0,
            0,
            {},
            nullptr,
            nullptr,
            nullptr,
            nullptr));
        TTestActorRuntimeBase::InitNodeImpl(node, nodeIndex);
    }
};

TFuzzActorRuntime* GetFuzzRuntime() {
    static auto runtime = std::make_unique<TFuzzActorRuntime>();
    return runtime.get();
}

} // anonymous namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    if (size == 0) {
        return 0;
    }

    Ydb::Table::CreateTableRequest request;
    if (!request.ParseFromArray(data, size)) {
        return 0;
    }

    auto* runtime = GetFuzzRuntime();

    try {
        runtime->RunCall([&] {
            NKikimrSchemeOp::TModifyScheme modifyScheme;
            NKikimr::TTableProfiles profiles;
            Ydb::StatusIds::StatusCode status;
            TString error;

            NKikimr::FillTableDescription(
                modifyScheme, request, profiles, status, error
            );
            return true;
        });
    } catch (...) {}

    return 0;
}
