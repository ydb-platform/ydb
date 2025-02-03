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

public:
    TSettings() = default;
    TSettings(const ui32 sparsedDetectorKff, const ui32 columnsLimit)
        : SparsedDetectorKff(sparsedDetectorKff)
        , ColumnsLimit(columnsLimit) {
    }

    bool IsSparsed(const ui32 keyUsageCount, const ui32 recordsCount) const {
        AFL_VERIFY(recordsCount);
        return keyUsageCount * SparsedDetectorKff < recordsCount;
    }

    NKikimrArrowAccessorProto::TConstructor::TSubColumns::TSettings SerializeToProto() const {
        NKikimrArrowAccessorProto::TConstructor::TSubColumns::TSettings result;
        result.SetSparsedDetectorKff(SparsedDetectorKff);
        result.SetColumnsLimit(ColumnsLimit);
        return result;
    }

    bool DeserializeFromProto(const NKikimrArrowAccessorProto::TConstructor::TSubColumns::TSettings& proto) {
        SparsedDetectorKff = proto.GetSparsedDetectorKff();
        ColumnsLimit = proto.GetColumnsLimit();
        return true;
    }

    NKikimrArrowAccessorProto::TRequestedConstructor::TSubColumns::TSettings SerializeToRequestedProto() const {
        NKikimrArrowAccessorProto::TRequestedConstructor::TSubColumns::TSettings result;
        result.SetSparsedDetectorKff(SparsedDetectorKff);
        result.SetColumnsLimit(ColumnsLimit);
        return result;
    }

    bool DeserializeFromRequestedProto(const NKikimrArrowAccessorProto::TRequestedConstructor::TSubColumns::TSettings& proto) {
        SparsedDetectorKff = proto.GetSparsedDetectorKff();
        ColumnsLimit = proto.GetColumnsLimit();
        return true;
    }
};

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
