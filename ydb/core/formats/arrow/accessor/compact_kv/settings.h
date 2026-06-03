#pragma once

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/formats/arrow/protos/accessor.pb.h>

#include <library/cpp/json/writer/json_value.h>

namespace NKikimr::NArrow::NAccessor::NCompactKV {

class TSettings {
private:
    // When true, string values that look like embedded JSON are recursively expanded.
    YDB_ACCESSOR(bool, ParseNested, false);

public:
    TSettings() = default;

    explicit TSettings(const bool parseNested)
        : ParseNested(parseNested) {
    }

    NJson::TJsonValue DebugJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("parse_nested", ParseNested);
        return result;
    }

    template <class TProto>
    void SerializeToProtoImpl(TProto& result) const {
        result.SetParseNested(ParseNested);
    }

    template <class TProto>
    bool DeserializeFromProtoImpl(const TProto& proto) {
        ParseNested = proto.GetParseNested();
        return true;
    }

    NKikimrArrowAccessorProto::TConstructor::TCompactKV SerializeToProto() const {
        NKikimrArrowAccessorProto::TConstructor::TCompactKV result;
        SerializeToProtoImpl(result);
        return result;
    }

    bool DeserializeFromProto(const NKikimrArrowAccessorProto::TConstructor::TCompactKV& proto) {
        return DeserializeFromProtoImpl(proto);
    }

    NKikimrArrowAccessorProto::TRequestedConstructor::TCompactKV SerializeToRequestedProto() const {
        NKikimrArrowAccessorProto::TRequestedConstructor::TCompactKV result;
        SerializeToProtoImpl(result);
        return result;
    }

    bool DeserializeFromRequestedProto(const NKikimrArrowAccessorProto::TRequestedConstructor::TCompactKV& proto) {
        return DeserializeFromProtoImpl(proto);
    }
};

}   // namespace NKikimr::NArrow::NAccessor::NCompactKV
