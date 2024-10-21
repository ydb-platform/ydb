#pragma once
#include <ydb/public/api/protos/ydb_value.pb.h>
#include <ydb/services/metadata/manager/table_record.h>

#include <library/cpp/json/writer/json_value.h>
#include <util/datetime/base.h>

namespace NKikimr::NMetadata::NInternal {

class TDecoderBase {
protected:
    i32 GetFieldIndex(const Ydb::ResultSet& rawData, const TString& columnId) const;

public:
    bool Read(const i32 columnIdx, TString& result, const Ydb::Value& r) const;
    bool Read(const i32 columnIdx, ui64& result, const Ydb::Value& r) const;
    bool Read(const i32 columnIdx, i64& result, const Ydb::Value& r) const;
    bool Read(const i32 columnIdx, ui32& result, const Ydb::Value& r) const;
    bool ReadDebugProto(const i32 columnIdx, ::google::protobuf::Message& result, const Ydb::Value& r) const;
    bool ReadJson(const i32 columnIdx, NJson::TJsonValue& result, const Ydb::Value& r) const;

    template <class T>
    bool ReadFromJson(const i32 columnIdx, T& result, const Ydb::Value& r) const {
        NJson::TJsonValue resultJson;
        if (!ReadJson(columnIdx, resultJson, r)) {
            return false;
        }
        return result.DeserializeFromJson(resultJson);
    }

    bool Read(const i32 columnIdx, TDuration& result, const Ydb::Value& r) const;
    bool Read(const i32 columnIdx, TInstant& result, const Ydb::Value& r) const;
    bool Read(const i32 columnIdx, bool& result, const Ydb::Value& r) const;

    template <class TObject>
    static bool DeserializeFromRecord(TObject& object, const TTableRecord& tr) {
        auto rs = tr.BuildRecordSet();
        Y_ABORT_UNLESS(rs.rows().size() == 1);
        typename TObject::TDecoder decoder(rs);
        return object.DeserializeFromRecord(decoder, rs.rows()[0]);
    }
};
}
