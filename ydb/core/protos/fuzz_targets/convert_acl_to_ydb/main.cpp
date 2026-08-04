#include <ydb/core/protos/fuzz_inputs.pb.h>
#include <contrib/libs/protobuf-mutator/src/libfuzzer/libfuzzer_macro.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/public/api/protos/ydb_scheme.pb.h>

using namespace NKikimr;

DEFINE_PROTO_FUZZER(const NKikimrFuzz::TFuzzInput& input) {
    google::protobuf::RepeatedPtrField<Ydb::Scheme::Permissions> permissions;
    try {
        ConvertAclToYdb(input.owner(), input.acl_text(), input.is_container(), &permissions);
        (void)permissions;
    } catch (...) {
    }
}
