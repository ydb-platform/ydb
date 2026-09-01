#include <ydb/services/udf_store/wasm/abi/udf_cpp_abi.h>

#include <library/cpp/digest/md5/md5.h>

#include <util/generic/string.h>

#include <cstring>

using namespace NYdb::NUdfStore::NAbi;

extern "C" {
    __attribute__((visibility("default"))) void md5(
        TExpressionContext* context,
        TUnversionedValue* result,
        TUnversionedValue* arg0)
    {
        if (arg0->Type == EValueType::Null) {
            result->Type = EValueType::Null;
            return;
        }

        const TString hash = MD5::Calc(TStringBuf(arg0->Data.String, arg0->Length));

        result->Type = EValueType::String;
        result->Length = hash.size();
        result->Data.String = AllocateBytes(context, result->Length);
        if (result->Length > 0) {
            memcpy(result->Data.String, hash.data(), result->Length);
        }
    }
}
