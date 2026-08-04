#include <ydb/core/ydb_convert/ydb_convert.h>

using namespace NKikimr;

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size == 0) {
        return 0;
    }

    const size_t split = size / 2;

    Ydb::Type inputType;
    if (!inputType.ParseFromArray(data, split)) {
        return 0;
    }

    if (inputType.type_case() == Ydb::Type::TYPE_NOT_SET) {
        return 0;
    }

    Ydb::Value inputValue;
    if (!inputValue.ParseFromArray(data + split, size - split)) {
        return 0;
    }

    NKikimrMiniKQL::TValue out;
    try {
        ConvertYdbValueToMiniKQLValue(inputType, inputValue, out);
    } catch (...) {
    }
    (void)out;

    return 0;
}
