#include <ydb/core/protos/table_stats.pb.h>
#include <library/cpp/protobuf/json/json2proto.h>
#include <library/cpp/protobuf/json/proto2json.h>

#include <cstddef>
#include <cstdint>

#include <util/generic/string.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    TString json(reinterpret_cast<const char*>(data), size);
    NKikimrTableStats::TTableStats stats;

    try {
        if (stats.ParseFromArray(data, size)) {
            NProtobufJson::Proto2Json(stats, json);
        }

        NKikimrTableStats::TTableStats parsed;
        NProtobufJson::MergeJson2Proto(
            json,
            parsed,
            NProtobufJson::TJson2ProtoConfig().SetAllowUnknownFields(true));

        TString roundTripJson;
        NProtobufJson::Proto2Json(parsed, roundTripJson);
    } catch (...) {
    }

    return 0;
}
