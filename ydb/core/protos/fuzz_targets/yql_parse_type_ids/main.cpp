#include <yql/essentials/ast/yql_type_string.h>
#include <util/generic/string.h>
#include <util/memory/pool.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    if (size == 0) {
        return 0;
    }

    const TString typeString(reinterpret_cast<const char*>(data), size);

    TMemoryPool pool(4096);
    NYql::TIssues issues;
    try {
        NYql::ParseType(typeString, pool, issues);
    } catch (...) {
        // We catch standard exceptions, fuzzer is looking for unhandled aborts/segfaults.
    }

    return 0;
}
