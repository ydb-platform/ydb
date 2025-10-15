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

    const TReadDataExtractorContainer& GetDataExtractor() const {
        AFL_VERIFY(!!DataExtractor);
        return DataExtractor;
    }

    TColumnIndexConstructor() = default;
};

}   // namespace NKikimr::NOlap::NIndexes
