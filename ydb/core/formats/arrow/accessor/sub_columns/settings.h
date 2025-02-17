#pragma once
#include <ydb/core/formats/arrow/arrow_helpers.h>

#include <ydb/library/formats/arrow/accessor/abstract/accessor.h>
#include <ydb/library/formats/arrow/protos/accessor.pb.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_base.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

class TSettings {
private:
    YDB_ACCESSOR(ui32, SparsedDetectorKff, 20);
    YDB_ACCESSOR(ui32, ColumnsLimit, 1024);
    YDB_ACCESSOR(ui32, ChunkMemoryLimit, 50 * 1024 * 1024);
    YDB_READONLY(double, OthersAllowedFraction, 0.05);

public:
    TSettings() = default;
    TSettings(const ui32 sparsedDetectorKff, const ui32 columnsLimit, const ui32 chunkMemoryLimit, const double othersAllowedFraction)
        : SparsedDetectorKff(sparsedDetectorKff)
        , ColumnsLimit(columnsLimit)
        , ChunkMemoryLimit(chunkMemoryLimit)
        , OthersAllowedFraction(othersAllowedFraction) {
        AFL_VERIFY(OthersAllowedFraction >= 0 && OthersAllowedFraction <= 1)("others_fraction", OthersAllowedFraction);
    }

    TSettings& SetOthersAllowedFraction(const double value) {
        AFL_VERIFY(value >= 0 && value <= 1)("others_fraction_value", value);
        OthersAllowedFraction = value;
        return *this;
    }

    NJson::TJsonValue DebugJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("sparsed_detector_kff", SparsedDetectorKff);
        result.InsertValue("columns_limit", ColumnsLimit);
        result.InsertValue("memory_limit", ChunkMemoryLimit);
        result.InsertValue("others_allowed_fraction", OthersAllowedFraction);
        return result;
    }

    bool IsSparsed(const ui32 keyUsageCount, const ui32 recordsCount) const {
        AFL_VERIFY(recordsCount);
        return keyUsageCount * SparsedDetectorKff < recordsCount;
    }

    template <class TProto>
    void SerializeToProtoImpl(TProto& result) const {
        result.SetSparsedDetectorKff(SparsedDetectorKff);
        result.SetColumnsLimit(ColumnsLimit);
        result.SetChunkMemoryLimit(ChunkMemoryLimit);
        result.SetOthersAllowedFraction(OthersAllowedFraction);
    }

    template <class TProto>
    bool DeserializeFromProtoImpl(const TProto& proto) {
        SparsedDetectorKff = proto.GetSparsedDetectorKff();
        ColumnsLimit = proto.GetColumnsLimit();
        ChunkMemoryLimit = proto.GetChunkMemoryLimit();
        OthersAllowedFraction = proto.GetOthersAllowedFraction();
        return true;
    }

    NKikimrArrowAccessorProto::TConstructor::TSubColumns::TSettings SerializeToProto() const {
        NKikimrArrowAccessorProto::TConstructor::TSubColumns::TSettings result;
        SerializeToProtoImpl(result);
        return result;
    }

    bool DeserializeFromProto(const NKikimrArrowAccessorProto::TConstructor::TSubColumns::TSettings& proto) {
        return DeserializeFromProtoImpl(proto);
    }

    NKikimrArrowAccessorProto::TRequestedConstructor::TSubColumns::TSettings SerializeToRequestedProto() const {
        NKikimrArrowAccessorProto::TRequestedConstructor::TSubColumns::TSettings result;
        SerializeToProtoImpl(result);
        return result;
    }

    bool DeserializeFromRequestedProto(const NKikimrArrowAccessorProto::TRequestedConstructor::TSubColumns::TSettings& proto) {
        return DeserializeFromProtoImpl(proto);
    }
};

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
