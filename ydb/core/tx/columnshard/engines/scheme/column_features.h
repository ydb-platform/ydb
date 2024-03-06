#pragma once
#include "abstract/loader.h"
#include "abstract/saver.h"

#include <ydb/core/formats/arrow/dictionary/object.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/core/formats/arrow/transformer/abstract.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storage.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>
#include <ydb/core/formats/arrow/common/validation.h>

namespace NKikimr::NOlap {

class TSaverContext {
private:
    YDB_ACCESSOR_DEF(NArrow::NSerialization::TSerializerContainer, ExternalSerializer);
    YDB_READONLY_DEF(std::shared_ptr<IStoragesManager>, StoragesManager);
public:
    TSaverContext(const std::shared_ptr<IStoragesManager>& storagesManager)
        : StoragesManager(storagesManager) {

    }
};

struct TIndexInfo;

class TColumnFeatures {
private:
    ui32 ColumnId;
    YDB_READONLY_DEF(std::shared_ptr<IBlobsStorageOperator>, Operator);
    YDB_READONLY_DEF(NArrow::NSerialization::TSerializerContainer, Serializer);
    std::optional<NArrow::NDictionary::TEncodingSettings> DictionaryEncoding;
    std::shared_ptr<TColumnLoader> Loader;
    NArrow::NTransformation::ITransformer::TPtr GetLoadTransformer() const;

    void InitLoader(const TIndexInfo& info);
    TColumnFeatures(const ui32 columnId, const std::shared_ptr<IBlobsStorageOperator>& blobsOperator);
public:

    TString DebugString() const {
        TStringBuilder sb;
        sb << "serializer=" << (Serializer ? Serializer->DebugString() : "NO") << ";";
        sb << "encoding=" << (DictionaryEncoding ? DictionaryEncoding->DebugString() : "NO") << ";";
        sb << "loader=" << (Loader ? Loader->DebugString() : "NO") << ";";
        return sb;
    }

    NArrow::NTransformation::ITransformer::TPtr GetSaveTransformer() const;
    static std::optional<TColumnFeatures> BuildFromProto(const NKikimrSchemeOp::TOlapColumnDescription& columnInfo, const TIndexInfo& indexInfo, const std::shared_ptr<IStoragesManager>& operators);
    static TColumnFeatures BuildFromIndexInfo(const ui32 columnId, const TIndexInfo& indexInfo, const std::shared_ptr<IBlobsStorageOperator>& blobsOperator);

    const std::shared_ptr<TColumnLoader>& GetLoader() const {
        AFL_VERIFY(Loader);
        return Loader;
    }
};

} // namespace NKikimr::NOlap
