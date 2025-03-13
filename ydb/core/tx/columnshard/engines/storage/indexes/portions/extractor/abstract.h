#pragma once
#include <ydb/core/formats/arrow/accessor/abstract/accessor.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/columnshard/engines/scheme/indexes/abstract/common.h>

#include <ydb/services/bg_tasks/abstract/interface.h>

#include <library/cpp/json/writer/json_value.h>

namespace NKikimr::NOlap::NIndexes {

class IReadDataExtractor {
public:
    using TRecordVisitor = const std::function<void(const std::string_view value, const ui64 hashBase)>;
    using TChunkVisitor = const std::function<void(const std::shared_ptr<arrow::Array>&, const ui64 hashBase)>;
    using TProto = NKikimrSchemeOp::TIndexDataExtractor;
    using TFactory = NObjectFactory::TObjectFactory<IReadDataExtractor, TString>;

private:
    virtual void DoVisitAll(const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& dataArray, const TChunkVisitor& chunkVisitor,
        const TRecordVisitor& recordVisitor) const = 0;
    virtual TConclusionStatus DoDeserializeFromJson(const NJson::TJsonValue& jsonInfo) = 0;

    virtual void DoSerializeToProto(TProto& proto) const = 0;
    virtual bool DoDeserializeFromProto(const TProto& proto) = 0;
    virtual bool DoCheckForIndex(const NRequest::TOriginalDataAddress& dataSource, ui64& baseHash) const = 0;
    virtual THashMap<ui64, ui32> DoGetIndexHitsCount(const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& dataArray) const = 0;

public:
    virtual TString GetClassName() const = 0;
    virtual ~IReadDataExtractor() = default;

    THashMap<ui64, ui32> GetIndexHitsCount(const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& dataArray) const {
        return DoGetIndexHitsCount(dataArray);
    }

    bool CheckForIndex(const NRequest::TOriginalDataAddress& dataSource, ui64& baseHash) const {
        baseHash = 0;
        return DoCheckForIndex(dataSource, baseHash);
    }

    virtual void SerializeToProto(TProto& proto) const {
        return DoSerializeToProto(proto);
    }

    virtual bool DeserializeFromProto(const TProto& proto) {
        return DoDeserializeFromProto(proto);
    }

    void VisitAll(const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& dataArray, const TChunkVisitor& chunkVisitor,
        const TRecordVisitor& recordVisitor) const {
        AFL_VERIFY(dataArray->IsDataOwner())("type", dataArray->GetType());
        DoVisitAll(dataArray, chunkVisitor, recordVisitor);
    }

    TConclusionStatus DeserializeFromJson(const NJson::TJsonValue& jsonInfo) {
        return DoDeserializeFromJson(jsonInfo);
    }
};

class TReadDataExtractorContainer: public NBackgroundTasks::TInterfaceProtoContainer<IReadDataExtractor> {
private:
    using TBase = NBackgroundTasks::TInterfaceProtoContainer<IReadDataExtractor>;

public:
    using TBase::TBase;
    TReadDataExtractorContainer() = default;

    bool DeserializeFromProto(const IReadDataExtractor::TProto& data);
    TConclusionStatus DeserializeFromJson(const NJson::TJsonValue& jsonValue);
};

}   // namespace NKikimr::NOlap::NIndexes
