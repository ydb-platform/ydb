#include "local_try_parse_local_db_path.h"

#include <cstddef>
#include <cstdint>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace {

TVector<TString> MakePath(const uint8_t* data, size_t size) {
    if (size == 0) {
        return {"db", ".sys_tablets", "1", "table"};
    }

    TVector<TString> parts;
    TString current;

    for (size_t i = 0; i < size; ++i) {
        const char ch = static_cast<char>(data[i]);
        if (ch == '/') {
            parts.push_back(std::move(current));
            current.clear();
        } else {
            current.push_back(ch);
        }
    }

    parts.push_back(std::move(current));

    return parts;
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    const auto path = MakePath(data, size);
    const auto result = NKikimr::NGRpcService::TryParseLocalDbPath(path);
    (void)result;
    return 0;
}
