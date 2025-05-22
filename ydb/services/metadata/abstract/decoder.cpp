#include "decoder.h"
#include <library/cpp/protobuf/json/proto2json.h>
#include <contrib/libs/protobuf/src/google/protobuf/text_format.h>
#include <ydb/library/actors/core/log.h>
#include <library/cpp/json/json_reader.h>

namespace NKikimr::NMetadata::NInternal {

i32 TDecoderBase::GetFieldIndex(const Ydb::ResultSet& rawData, const TString& columnId) const {
    i32 idx = 0;
    for (auto&& i : rawData.columns()) {
        if (i.name() == columnId) {
            return idx;
        }
        ++idx;
    }
    return -1;
}

bool TDecoderBase::Read(const i32 columnIdx, TString& result, const Ydb::Value& r) const {
    if (columnIdx >= (i32)r.items().size() || columnIdx < 0) {
        return false;
    }
    if (r.items()[columnIdx].has_bytes_value()) {
        result = r.items()[columnIdx].bytes_value();
    } else if (r.items()[columnIdx].has_text_value()) {
        result = r.items()[columnIdx].text_value();
    } else {
        // its normally for empty strings
        result = "";
    }
    return true;
}

bool TDecoderBase::Read(const i32 columnIdx, ui64& result, const Ydb::Value& r) const {
    if (columnIdx >= (i32)r.items().size() || columnIdx < 0) {
        return false;
    }
    if (r.items()[columnIdx].has_uint64_value()) {
        result = r.items()[columnIdx].uint64_value();
        return true;
    }
    return false;
}

bool TDecoderBase::Read(const i32 columnIdx, i64& result, const Ydb::Value& r) const {
    if (columnIdx >= (i32)r.items().size() || columnIdx < 0) {
        return false;
    }
    if (r.items()[columnIdx].has_int64_value()) {
        result = r.items()[columnIdx].int64_value();
        return true;
    }
    return false;
}

bool TDecoderBase::Read(const i32 columnIdx, ui32& result, const Ydb::Value& r) const {
    if (columnIdx >= (i32)r.items().size() || columnIdx < 0) {
        return false;
    }
    if (r.items()[columnIdx].has_uint32_value()) {
        result = r.items()[columnIdx].uint32_value();
        return true;
    }
    return false;
}

bool TDecoderBase::Read(const i32 columnIdx, TDuration& result, const Ydb::Value& r) const {
    if (columnIdx >= (i32)r.items().size() || columnIdx < 0) {
        return false;
    }
    const TString& s = r.items()[columnIdx].bytes_value();
    if (!TDuration::TryParse(s, result)) {
        ALS_WARN(0) << "cannot parse duration for tiering: " << s;
        return false;
    }
    return true;
}

bool TDecoderBase::Read(const i32 columnIdx, bool& result, const Ydb::Value& r) const {
    if (columnIdx >= (i32)r.items().size() || columnIdx < 0) {
        return false;
    }
    auto& pValue = r.items()[columnIdx];
    if (pValue.has_bool_value()) {
        result = pValue.bool_value();
    } else {
        ALS_WARN(0) << "incorrect type for instant seconds parsing";
        return false;
    }
    return true;
}

bool TDecoderBase::Read(const i32 columnIdx, TInstant& result, const Ydb::Value& r) const {
    if (columnIdx >= (i32)r.items().size() || columnIdx < 0) {
        return false;
    }
    auto& pValue = r.items()[columnIdx];
    if (pValue.has_uint32_value()) {
        result = TInstant::Seconds(pValue.uint32_value());
    } else if (pValue.has_int64_value()) {
        result = TInstant::MicroSeconds(pValue.int64_value());
    } else if (pValue.has_uint64_value()) {
        result = TInstant::MicroSeconds(pValue.uint64_value());
    } else if (pValue.has_int32_value()) {
        result = TInstant::Seconds(pValue.int32_value());
    } else {
        ALS_WARN(0) << "incorrect type for instant seconds parsing";
        return false;
    }
    return true;
}

bool TDecoderBase::ReadJson(const i32 columnIdx, NJson::TJsonValue& result, const Ydb::Value& r) const {
    if (columnIdx >= (i32)r.items().size() || columnIdx < 0) {
        return false;
    }
    const TString* jsonString = nullptr;
    if (r.items()[columnIdx].has_bytes_value()) {
        jsonString = &r.items()[columnIdx].bytes_value();
    } else if (r.items()[columnIdx].has_text_value()) {
        jsonString = &r.items()[columnIdx].text_value();
    } else {
        ALS_ERROR(0) << "no data for json string";
        return false;
    }
    if (!NJson::ReadJsonFastTree(*jsonString, &result)) {
        ALS_ERROR(0) << "cannot parse json string: " << *jsonString;
        return false;
    }
    return true;
}

bool TDecoderBase::ReadDebugProto(const i32 columnIdx, ::google::protobuf::Message& result, const Ydb::Value& r) const {
    if (columnIdx >= (i32)r.items().size() || columnIdx < 0) {
        return false;
    }
    const TString* jsonString = nullptr;
    if (r.items()[columnIdx].has_bytes_value()) {
        jsonString = &r.items()[columnIdx].bytes_value();
    } else if (r.items()[columnIdx].has_text_value()) {
        jsonString = &r.items()[columnIdx].text_value();
    } else {
        ALS_ERROR(0) << "no data for debug proto string";
        return false;
    }
    if (!::google::protobuf::TextFormat::ParseFromString(*jsonString, &result)) {
        ALS_ERROR(0) << "cannot parse proto string: " << *jsonString;
        return false;
    }
    return true;
}

}
