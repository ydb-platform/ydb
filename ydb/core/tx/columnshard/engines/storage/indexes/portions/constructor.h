#pragma once
#include "extractor/abstract.h"

#include <ydb/core/tx/columnshard/engines/scheme/abstract/index_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/indexes/abstract/constructor.h>
#include <ydb/core/tx/columnshard/engines/scheme/indexes/abstract/meta.h>
#include <ydb/core/tx/columnshard/splitter/abstract/chunks.h>

namespace NKikimr::NOlap::NIndexes {

class TColumnIndexConstructor: public IIndexMetaConstructor {
protected:
    TString ColumnName;
    TReadDataExtractorContainer DataExtractor;

    virtual TConclusionStatus DoDeserializeFromJson(const NJson::TJsonValue& jsonInfo) override;

    template <class TProto>
    TConclusionStatus DeserializeFromProtoImpl(const TProto& proto) {
        if (!DataExtractor.DeserializeFromProto(proto.GetDataExtractor())) {
            return TConclusionStatus::Fail("cannot parse data extractor: " + proto.GetDataExtractor().DebugString());
        }
        if (proto.GetColumnNames().size() != 1) {
            return TConclusionStatus::Fail("incorrect columns count in index: " + proto.DebugString());
        }
        ColumnName = proto.GetColumnNames()[0];
        return TConclusionStatus::Success();
    }

    template <class TProto>
    void SerializeToProtoImpl(TProto& proto) const {
        proto.AddColumnNames(ColumnName);
        *proto.MutableDataExtractor() = DataExtractor.SerializeToProto();
    }

public:
    const TString& GetColumnName() const {
        AFL_VERIFY(ColumnName);
        return ColumnName;
    }

protected:
    TConclusion<TString> ResolveColumnNameForAlterIndex(
        const NSchemeShard::TOlapSchema& currentSchema,
        const IIndexMeta& existingMeta) const;

    template <class TDerived>
    std::shared_ptr<IIndexMeta> DoCreateOrPatchSingleColumnIndexMeta(
        const ui32 indexId, const TString& indexName,
        const NSchemeShard::TOlapSchema& currentSchema, NSchemeShard::IErrorCollector& errors,
        const IIndexMeta& existingMeta) const {
        if (!ColumnName.empty()) {
            return DoCreateIndexMeta(indexId, indexName, currentSchema, errors);
        }

        auto resolvedColumnName = ResolveColumnNameForAlterIndex(currentSchema, existingMeta);
        if (resolvedColumnName.IsFail()) {
            errors.AddError(resolvedColumnName.GetErrorMessage());
            return nullptr;
        }

        TDerived patched = static_cast<const TDerived&>(*this);
        patched.ColumnName = *resolvedColumnName;
        const TColumnIndexConstructor& patchedBase = patched;
        return patchedBase.DoCreateIndexMeta(indexId, indexName, currentSchema, errors);
    }

public:
    const TReadDataExtractorContainer& GetDataExtractor() const {
        AFL_VERIFY(!!DataExtractor);
        return DataExtractor;
    }

    TColumnIndexConstructor() = default;
};

}   // namespace NKikimr::NOlap::NIndexes
