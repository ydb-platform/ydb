#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/library/mkql_proto/protos/minikql.pb.h>
#include <ydb/core/scheme/scheme_type_id.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size == 0) {
        return 0;
    }

    const size_t split = size / 2;

    NKikimrSchemeOp::TTableDescription input;
    if (!input.ParseFromArray(data, split)) {
        return 0;
    }

    NKikimrMiniKQL::TType splitKeyType;
    if (!splitKeyType.ParseFromArray(data + split, size - split)) {
        splitKeyType.set_kind(NKikimrMiniKQL::Data);
        splitKeyType.mutable_data()->set_scheme(NKikimr::NScheme::NTypeIds::Utf8);
    }

    Ydb::Table::DescribeTableResult output;
    try {
        NKikimr::FillTableBoundary(output, input, splitKeyType);
    } catch (...) {
    }

    return 0;
}
