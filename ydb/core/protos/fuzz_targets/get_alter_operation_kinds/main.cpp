#include <ydb/core/ydb_convert/table_description.h>

#include <cstddef>
#include <cstdint>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    Ydb::Table::AlterTableRequest req;

    if (!req.ParseFromArray(data, size)) {
        const uint8_t mask = size ? data[0] : 0;
        if (mask & 1U) {
            req.add_add_columns();
        }
        if (mask & 2U) {
            req.add_drop_columns("test");
        }
        if (mask & 4U) {
            req.add_add_indexes();
        }
        if (mask & 8U) {
            req.add_drop_indexes("test");
        }
        if (mask & 16U) {
            req.add_add_changefeeds();
        }
        if (mask & 32U) {
            req.add_drop_changefeeds("test");
        }
        if (mask & 64U) {
            (*req.mutable_alter_attributes())["k"] = "v";
        }
        if (mask & 128U) {
            req.mutable_compact();
        }
    }

    try {
        auto ops = NKikimr::GetAlterOperationKinds(&req);
        (void)ops;
    } catch (...) {
    }

    return 0;
}
