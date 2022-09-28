#include "partitioning.h"
#include <library/cpp/json/json_reader.h>
#include <util/string/builder.h>

namespace NSQLTranslation {

TString ParsePartitionedByBinding(const TString& name, const TString& value, TVector<TString>& columns) {
    using namespace NJson;
    TJsonValue json;
    bool throwOnError = false;
    if (!ReadJsonTree(value, &json, throwOnError)) {
        return TStringBuilder() << "Binding setting " << name << " is not a valid JSON";
    }

    const TJsonValue::TArray* arr = nullptr;
    if (!json.GetArrayPointer(&arr)) {
        return TStringBuilder() << "Binding setting " << name << ": expecting array";
    }

    if (arr->empty()) {
        return TStringBuilder() << "Binding setting " << name << ": expecting non-empty array";
    }

    for (auto& item : *arr) {
        TString str;
        if (!item.GetString(&str)) {
            return TStringBuilder() << "Binding setting " << name << ": expecting non-empty array of strings";
        }
        columns.push_back(std::move(str));
    }

    return {};
}

}  // namespace NSQLTranslation
