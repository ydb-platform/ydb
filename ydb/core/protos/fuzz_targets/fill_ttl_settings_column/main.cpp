#include <ydb/core/ydb_convert/table_settings.h>

#include <cstring>

using namespace NKikimr;

namespace {

TString LoadColumnName(const uint8_t* data, size_t size) {
    if (size == 0) {
        return "ttl";
    }

    const size_t len = size < 64 ? size : 64;
    TString name(reinterpret_cast<const char*>(data), len);
    return name.empty() ? TString("ttl") : name;
}

ui32 LoadExpireAfterSeconds(const uint8_t* data, size_t size) {
    ui32 value = 3600;
    if (size >= sizeof(value)) {
        std::memcpy(&value, data, sizeof(value));
    } else if (size > 0) {
        value = data[0];
    }
    return value == 0 ? 3600 : value;
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    Ydb::Table::TtlSettings out;
    NKikimrSchemeOp::TColumnDataLifeCycle::TTtl in;

    in.SetColumnName(LoadColumnName(data, size));
    in.SetExpireAfterSeconds(LoadExpireAfterSeconds(data, size));

    Ydb::StatusIds::StatusCode code = Ydb::StatusIds::SUCCESS;
    TString error;

    try {
        FillTtlSettings(out, in, code, error);
    } catch (...) {
    }
    (void)out;
    (void)code;
    (void)error;

    return 0;
}
