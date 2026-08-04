#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/library/mkql_proto/protos/minikql.pb.h>
#include <ydb/core/scheme/scheme_type_id.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    Ydb::Table::DescribeTableResult out;
    NKikimrSchemeOp::TTableDescription in;
    NKikimrMiniKQL::TType splitKeyType;

    const size_t split = size / 2;
    (void)in.ParseFromArray(data, split);

    if (!splitKeyType.ParseFromArray(data + split, size - split)) {
        splitKeyType.set_kind(NKikimrMiniKQL::Data);
        splitKeyType.mutable_data()->set_scheme(NKikimr::NScheme::NTypeIds::Utf8);
    }

    if (in.ColumnsSize() == 0) {
        auto* column = in.AddColumns();
        column->SetName("key");
        column->SetType("Utf8");
        in.AddKeyColumnNames("key");
    }

    try {
        NKikimr::FillColumnDescription(out, splitKeyType, in);
    } catch (...) {
    }

    return 0;
}
