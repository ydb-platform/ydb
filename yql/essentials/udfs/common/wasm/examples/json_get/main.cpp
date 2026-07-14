#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>

#include <yql/essentials/udfs/common/wasm/abi/udf_cpp_abi.h>

#include <cstring>

using namespace NYT::NQueryClient::NUdf;

namespace {

void SetStringResult(TExpressionContext* context, TUnversionedValue* result, const TString& value)
{
    result->Data.String = AllocateBytes(context, value.size());
    memcpy(result->Data.String, value.data(), value.size());
    result->Type = EValueType::String;
    result->Length = value.size();
}

void SetNullResult(TUnversionedValue* result)
{
    result->Type = EValueType::Null;
}

} // namespace

extern "C" void json_get(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* jsonString,
    TUnversionedValue* path)
{
    if (jsonString->Type == EValueType::Null || path->Type == EValueType::Null) {
        SetNullResult(result);
        return;
    }

    const TString json(TStringBuf(jsonString->Data.String, jsonString->Length));
    const TString pathStr(TStringBuf(path->Data.String, path->Length));

    NJson::TJsonValue root;
    if (!NJson::ReadJsonTree(json, &root, /*throwOnError*/ false)) {
        SetNullResult(result);
        return;
    }

    const NJson::TJsonValue* value = root.GetValueByPath(pathStr);
    if (!value || !value->IsDefined()) {
        SetNullResult(result);
        return;
    }

    SetStringResult(context, result, value->GetStringRobust());
}
