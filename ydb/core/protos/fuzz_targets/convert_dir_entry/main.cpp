#include <ydb/core/protos/fuzz_inputs.pb.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <contrib/libs/protobuf-mutator/src/libfuzzer/libfuzzer_macro.h>

using namespace NKikimr;

DEFINE_PROTO_FUZZER(const NKikimrFuzz::TFuzzInput& input) {
    if (!input.has_dir_entry()) {
        return;
    }
    Ydb::Scheme::Entry out;
    try {
        ConvertDirectoryEntry(input.dir_entry(), &out, true);
    } catch (...) {
    }
    (void)out;
}
