#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/vector.h>
#include <util/system/yassert.h>

#include <algorithm>

namespace {

using namespace NKikimr;
using namespace NKikimr::NColumnShard;
using namespace NKikimr::NTxUT;

constexpr ui64 TableId = 42;
constexpr size_t MaxSteps = 10;

struct TPreparedWrite {
    ui64 LockId = 0;
    ui64 TxId = 0;
    std::vector<ui64> WriteIds;
};

std::pair<ui64, ui64> PickRange(FuzzedDataProvider& provider) {
    ui64 from = provider.ConsumeIntegralInRange<ui64>(0, 128);
    ui64 to = provider.ConsumeIntegralInRange<ui64>(from + 1, from + 64);
    return {from, to};
}

NEvWrite::EModificationType PickModification(FuzzedDataProvider& provider) {
    switch (provider.ConsumeIntegralInRange<ui8>(0, 2)) {
        case 0:
            return NEvWrite::EModificationType::Upsert;
        case 1:
            return NEvWrite::EModificationType::Insert;
        default:
            return NEvWrite::EModificationType::Update;
    }
}

void CommitPrepared(TTestBasicRuntime& runtime, TActorId& sender, TVector<TPreparedWrite>& prepared, FuzzedDataProvider& provider) {
    if (prepared.empty()) {
        return;
    }

    const size_t idx = provider.ConsumeIntegralInRange<size_t>(0, prepared.size() - 1);
    TPreparedWrite item = std::move(prepared[idx]);
    prepared.erase(prepared.begin() + idx);

    const TPlanStep planStep = ProposeCommit(runtime, sender, item.TxId, item.WriteIds, item.LockId);
    PlanCommit(runtime, sender, planStep, item.TxId);
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider provider(data, size);

    auto csControllerGuard = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();

    TTestBasicRuntime runtime;
    TTester::Setup(runtime);

    TestTableDescription table;
    Y_UNUSED(PrepareTablet(runtime, TableId, table.Schema, table.Pk.size()));
    TActorId sender = runtime.AllocateEdgeActor();

    TVector<TPreparedWrite> prepared;
    ui64 nextWriteId = 1000;
    ui64 nextLockId = 2000;
    ui64 nextTxId = 3000;

    const size_t steps = provider.ConsumeIntegralInRange<size_t>(1, MaxSteps);
    for (size_t step = 0; step < steps && provider.remaining_bytes(); ++step) {
        switch (provider.ConsumeIntegralInRange<ui8>(0, 3)) {
            case 0: {
                const auto range = PickRange(provider);
                const TString dataBlob = MakeTestBlob(range, table.Schema);
                Y_ABORT_UNLESS(WriteData(runtime, sender, ++nextWriteId, TableId, dataBlob, table.Schema));
                break;
            }
            case 1: {
                const auto range = PickRange(provider);
                const ui64 lockId = ++nextLockId;
                std::vector<ui64> writeIds;
                const TString dataBlob = MakeTestBlob(range, table.Schema);
                if (WriteData(runtime, sender, ++nextWriteId, TableId, dataBlob, table.Schema, true, &writeIds, PickModification(provider), lockId)) {
                    prepared.push_back({lockId, ++nextTxId, std::move(writeIds)});
                }
                break;
            }
            case 2:
                CommitPrepared(runtime, sender, prepared, provider);
                break;
            case 3:
                Wakeup(runtime, sender, TTestTxConfig::TxTablet0);
                break;
        }

        if (prepared.size() > 4) {
            CommitPrepared(runtime, sender, prepared, provider);
        }
    }

    while (!prepared.empty()) {
        CommitPrepared(runtime, sender, prepared, provider);
    }

    return 0;
}
