// Fuzzer for NKikimr::NFormats::TArrowCSV — CSV to Apache Arrow batch parsing.
// TArrowCSV is used for bulk data import into YDB column tables via the
// BulkUpsert gRPC endpoint. The CSV content is attacker-supplied and parsed
// by Apache Arrow's CSV reader with YDB-specific type handling.
// The constructor is protected, so we expose it via a thin local subclass.
#include <ydb/library/formats/arrow/csv/converter/csv_arrow.h>
#include <util/generic/string.h>
#include <set>

namespace {
// Expose the protected TArrowCSV constructor: no fixed columns, header mode.
class TFuzzArrowCSV : public NKikimr::NFormats::TArrowCSV {
public:
    TFuzzArrowCSV()
        : TArrowCSV(/*columns=*/{}, /*header=*/true, /*notNullColumns=*/{})
    {}
};
} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    if (size > 256 * 1024) return 0;
    TString csv(reinterpret_cast<const char*>(data), size);
    TFuzzArrowCSV parser;
    TString err;
    try {
        auto batch = parser.ReadSingleBatch(csv, err);
        (void)batch;
    } catch (...) {}
    return 0;
}
