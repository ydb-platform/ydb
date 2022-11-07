#pragma once
#include <ydb/public/api/protos/ydb_value.pb.h>
#include <util/datetime/base.h>

namespace NKikimr::NInternal {

class TDecoderBase {
protected:
    i32 GetFieldIndex(const Ydb::ResultSet& rawData, const TString& columnId, const bool verify = true) const;
public:
    bool Read(const ui32 columnIdx, TString& result, const Ydb::Value& r) const;
    bool ReadDebugProto(const ui32 columnIdx, ::google::protobuf::Message& result, const Ydb::Value& r) const;
    bool Read(const ui32 columnIdx, TDuration& result, const Ydb::Value& r) const;
    bool Read(const ui32 columnIdx, TInstant& result, const Ydb::Value& r) const;
    bool Read(const ui32 columnIdx, bool& result, const Ydb::Value& r) const;
};

}
