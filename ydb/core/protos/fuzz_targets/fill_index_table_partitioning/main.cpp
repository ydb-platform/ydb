#include <ydb/core/ydb_convert/table_settings.h>

#include <cstring>
#include <vector>

using namespace NKikimr;

namespace {

TString LoadField(const uint8_t* data, size_t size, size_t offset, const TString& fallback) {
    if (offset >= size) {
        return fallback;
    }

    const size_t remaining = size - offset;
    const size_t len = remaining < 32 ? remaining : 32;
    TString value(reinterpret_cast<const char*>(data + offset), len);
    return value.empty() ? fallback : value;
}

ui64 LoadPartitionSizeMb(const uint8_t* data, size_t size) {
    ui64 value = 2048;
    if (size >= sizeof(value)) {
        std::memcpy(&value, data, sizeof(value));
    } else if (size > 0) {
        value = data[0];
    }
    return value == 0 ? 2048 : value;
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    std::vector<NKikimrSchemeOp::TTableDescription> indexImplTableDescriptions;
    Ydb::Table::TableIndex index;

    index.set_name(LoadField(data, size, 0, "idx1"));
    index.add_index_columns(LoadField(data, size, 8, "key"));
    auto* settings = index.mutable_global_index()->mutable_settings();
    auto* partitioning = settings->mutable_partitioning_settings();

    partitioning->set_partition_size_mb(LoadPartitionSizeMb(data, size));

    Ydb::StatusIds::StatusCode code = Ydb::StatusIds::SUCCESS;
    TString error;

    try {
        FillIndexTablePartitioning(indexImplTableDescriptions, index, code, error);
    } catch (...) {
    }
    (void)indexImplTableDescriptions;
    (void)code;
    (void)error;

    return 0;
}
