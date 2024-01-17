#pragma once

#include <ydb/library/yql/minikql/mkql_node.h>

#include <ydb/library/actors/util/rope.h>

#include <arrow/datum.h>

#include <functional>
#include <memory>

namespace NKikimr::NMiniKQL {

class IBlockSerializer {
public:
    virtual ~IBlockSerializer() = default;

    virtual size_t ArrayMetadataCount() const = 0;

    using TMetadataSink = std::function<void(ui64 meta)>;
    virtual void StoreMetadata(const arrow::ArrayData& data, const TMetadataSink& metaSink) const = 0;
    virtual void StoreArray(const arrow::ArrayData& data, TRope& dst) const = 0;
};

class IBlockDeserializer {
public:
    virtual ~IBlockDeserializer() = default;

    using TMetadataSource = std::function<ui64()>;
    virtual void LoadMetadata(const TMetadataSource& metaSource) = 0;
    virtual std::shared_ptr<arrow::ArrayData> LoadArray(TRope& src, ui64 blockLen, ui64 offset) = 0;
};


std::unique_ptr<IBlockSerializer> MakeBlockSerializer(const NYql::NUdf::ITypeInfoHelper& typeInfoHelper, const NYql::NUdf::TType* type);
std::unique_ptr<IBlockDeserializer> MakeBlockDeserializer(const NYql::NUdf::ITypeInfoHelper& typeInfoHelper, const NYql::NUdf::TType* type);

}
