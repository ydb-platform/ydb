#pragma once
#include <ydb/core/tx/columnshard/engines/portions/meta.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract/index_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/indexes/abstract/meta.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/bits_storage/abstract.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/portions/meta.h>
#include <ydb/core/tx/columnshard/splitter/abstract/chunks.h>

namespace NKikimr::NOlap::NIndexes {

class TSkipIndex: public TIndexByColumns {
private:
    using TBase = TIndexByColumns;

public:
    using EOperation = NArrow::NSSA::EIndexCheckOperation;

private:
    virtual bool DoIsAppropriateFor(const TString& subColumnName, const EOperation op) const = 0;
    virtual bool DoCheckValue(
        const TString& data, const std::optional<ui64> cat, const std::shared_ptr<arrow::Scalar>& value, const EOperation op) const = 0;

public:
    bool CheckValue(const TString& data, const std::optional<ui64> cat, const std::shared_ptr<arrow::Scalar>& value, const EOperation op) const {
        return DoCheckValue(data, cat, value, op);
    }

    virtual bool IsSkipIndex() const override final {
        return true;
    }

    bool IsAppropriateFor(const NRequest::TOriginalDataAddress& addr, const EOperation op) const {
        if (GetColumnId() != addr.GetColumnId()) {
            return false;
        }
        return DoIsAppropriateFor(addr.GetSubColumnName(), op);
    }
    using TBase::TBase;
};

class TSkipBitmapIndex: public TSkipIndex {
private:
    std::shared_ptr<IBitsStorageConstructor> BitsStorageConstructor;
    using TBase = TSkipIndex;
    virtual bool DoCheckValueImpl(
        const IBitsStorage& data, const std::optional<ui64> cat, const std::shared_ptr<arrow::Scalar>& value, const EOperation op) const = 0;

    virtual bool DoCheckValue(const TString& data, const std::optional<ui64> cat, const std::shared_ptr<arrow::Scalar>& value,
        const EOperation op) const override final {
        auto storageConclusion = BitsStorageConstructor->Build(data);
        return DoCheckValueImpl(*storageConclusion.GetResult(), cat, value, op);
    }

protected:
    template <class TProto>
    TConclusionStatus DeserializeFromProtoImpl(const TProto& proto) {
        if (!proto.HasBitsStorage()) {
            BitsStorageConstructor = IBitsStorageConstructor::GetDefault();
            return TConclusionStatus::Success();
        } else {
            BitsStorageConstructor =
                std::shared_ptr<IBitsStorageConstructor>(IBitsStorageConstructor::TFactory::Construct(proto.GetBitsStorage().GetClassName()));
            return BitsStorageConstructor->DeserializeFromProto(proto.GetBitsStorage());
        }
    }

    template <class TProto>
    void SerializeToProtoImpl(TProto& proto) const {
        *proto.MutableBitsStorage() = BitsStorageConstructor->SerializeToProto();
    }

public:
    const std::shared_ptr<IBitsStorageConstructor>& GetBitsStorageConstructor() const {
        AFL_VERIFY(BitsStorageConstructor);
        return BitsStorageConstructor;
    }

    TSkipBitmapIndex() = default;
    TSkipBitmapIndex(const ui32 indexId, const TString& indexName, const ui32 columnId, const TString& storageId,
        const TReadDataExtractorContainer& extractor, const std::shared_ptr<IBitsStorageConstructor>& bitsStorageConstructor)
        : TBase(indexId, indexName, columnId, storageId, extractor)
        , BitsStorageConstructor(bitsStorageConstructor) {
        AFL_VERIFY(!!BitsStorageConstructor);
    }
};

}   // namespace NKikimr::NOlap::NIndexes
