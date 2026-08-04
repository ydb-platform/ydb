#include <ydb/core/protos/fuzz_inputs.pb.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <contrib/libs/protobuf-mutator/src/libfuzzer/libfuzzer_macro.h>

using namespace NKikimr;

DEFINE_PROTO_FUZZER(const NKikimrFuzz::TFuzzInput& input) {
    if (!input.has_type_info()) {
        return;
    }
    try {
        NScheme::TTypeInfo typeInfo = NScheme::TypeInfoFromProto(
            static_cast<NScheme::TTypeId>(input.type_id_hint()),
            input.type_info()
        );
        (void)typeInfo;
    } catch (...) {
    }
}
