#pragma once

#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/string/subst.h>
#include <util/string/cast.h>

namespace NKikimr::NDDisk {

inline NJson::TJsonValue ComponentData(const TString& html, TStringBuf attribute, size_t index = 0, size_t start = 0) {
    const TString marker = TString(attribute) + "=\"";
    for (size_t i = 0; i <= index; ++i) {
        start = html.find(marker, start);
        UNIT_ASSERT_C(start != TString::npos, attribute);
        start += marker.size();
    }
    TString json = html.substr(start, html.find('"', start) - start);
    SubstGlobal(json, "&quot;", "\"");
    SubstGlobal(json, "&#39;", "'");
    SubstGlobal(json, "&lt;", "<");
    SubstGlobal(json, "&gt;", ">");
    SubstGlobal(json, "&amp;", "&");
    NJson::TJsonValue data;
    UNIT_ASSERT(NJson::ReadJsonTree(json, &data));
    return data;
}

inline NJson::TJsonValue TabletBar(const TString& html, TStringBuf kind) {
    const auto start = html.find("data-share-bar=\"" + TString(kind) + "\"");
    UNIT_ASSERT(start != TString::npos);
    return ComponentData(html, "data-allocation", 0, start);
}

inline NJson::TJsonValue TabletSegment(const TString& html, TStringBuf kind, ui64 id) {
    const auto bar = TabletBar(html, kind);
    for (const auto& segment : bar["segments"].GetArraySafe()) {
        if (segment["key"].GetString() == ToString(id)) {
            return segment;
        }
    }
    return NJson::TJsonValue(NJson::JSON_NULL);
}

} // namespace NKikimr::NDDisk
