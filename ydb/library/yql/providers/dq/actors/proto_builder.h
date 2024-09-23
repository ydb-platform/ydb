#pragma once

#include <ydb/public/api/protos/ydb_value.pb.h>

#include <ydb/library/yql/dq/common/dq_serialized_batch.h>

#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/mkql_node.h>

namespace NYql::NDqProto {
    class TData;
}

namespace NYql::NDqs {

class TProtoBuilder {
public:
    TProtoBuilder(const TString& type, const TVector<TString>& columns);
    ~TProtoBuilder();

    bool CanBuildResultSet() const;
    Ydb::ResultSet BuildResultSet(TVector<NYql::NDq::TDqSerializedBatch>&& data);
    TString BuildYson(TVector<NYql::NDq::TDqSerializedBatch>&& data, 
        ui64 maxBytesLimit = std::numeric_limits<ui64>::max(),
        ui64 maxRowsLimit = std::numeric_limits<ui64>::max(),
        bool* truncated = nullptr);
    bool WriteYsonData(NYql::NDq::TDqSerializedBatch&& data, const std::function<bool(const TString& rawYson)>& func);
    bool WriteData(NYql::NDq::TDqSerializedBatch&& data, const std::function<bool(const NYql::NUdf::TUnboxedValuePod& value)>& func);
    bool WriteData(TVector<NYql::NDq::TDqSerializedBatch>&& data, const std::function<bool(const NYql::NUdf::TUnboxedValuePod& value)>& func);
    TString GetSerializedType() const;
    TString AllocDebugInfo();

private:
    NKikimr::NMiniKQL::TScopedAlloc Alloc;
    NKikimr::NMiniKQL::TTypeEnvironment TypeEnv;
    NKikimr::NMiniKQL::TType* ResultType;
    const TVector<ui32> ColumnOrder;
};

} // NYql::NDqs

