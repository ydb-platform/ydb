#pragma once
#include "column/info.h"

#include <ydb/core/formats/arrow/dictionary/object.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/core/formats/arrow/transformer/abstract.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storage.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <ydb/core/tx/columnshard/splitter/abstract/chunks.h>
#include <ydb/core/formats/arrow/common/validation.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract/index_info.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>

namespace NKikimr::NOlap {

class TSaverContext {
private:
    YDB_READONLY_DEF(std::shared_ptr<IStoragesManager>, StoragesManager);
public:
    TSaverContext(const std::shared_ptr<IStoragesManager>& storagesManager)
        : StoragesManager(storagesManager) {
        AFL_VERIFY(StoragesManager);
    }
};

struct TIndexInfo;

class TColumnFeatures: public TSimpleColumnInfo {
private:
    using TBase = TSimpleColumnInfo;
    YDB_READONLY_DEF(std::shared_ptr<IBlobsStorageOperator>, Operator);
public:
    TColumnFeatures(const ui32 columnId, const std::shared_ptr<arrow::Field>& arrowField, const NArrow::NSerialization::TSerializerContainer& serializer,
        const std::shared_ptr<IBlobsStorageOperator>& bOperator, const bool needMinMax, const bool isSorted,
        const std::shared_ptr<arrow::Scalar>& defaultValue)
        : TBase(columnId, arrowField, serializer, needMinMax, isSorted, defaultValue)
        , Operator(bOperator)
    {
        AFL_VERIFY(Operator);

    }

    TConclusionStatus DeserializeFromProto(const NKikimrSchemeOp::TOlapColumnDescription& columnInfo, const std::shared_ptr<IStoragesManager>& storagesManager) {
        auto parsed = TBase::DeserializeFromProto(columnInfo);
        if (!parsed) {
            return parsed;
        }
        Operator = storagesManager->GetOperatorVerified(columnInfo.GetStorageId() ? columnInfo.GetStorageId() : IStoragesManager::DefaultStorageId);
        return TConclusionStatus::Success();
    }
};

} // namespace NKikimr::NOlap
