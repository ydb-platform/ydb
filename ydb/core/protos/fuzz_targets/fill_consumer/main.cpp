#include <ydb/core/protos/fuzz_targets/common.h>
#include <ydb/core/protos/fuzz_targets/fuzz_actor_utils.h>
#include <ydb/core/ydb_convert/topic_description.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    NKikimrFuzz::TFuzzInput input;
    if (!input.ParseFromArray(data, size)) {
        return 0;
    }

    if (!input.has_pq_consumer()) {
        return 0;
    }

    Ydb::Topic::Consumer result;
    Ydb::StatusIds::StatusCode status;
    TString error;

    RunWithMockedActorSystem([&]() {
        try {
            NKikimr::FillConsumer(
                result,
                input.pq_consumer(),
                status,
                error,
                true // checkServiceType
            );
        } catch (...) {
        }
    });

    return 0;
}
