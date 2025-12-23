#pragma once
#include "abstract.h"

#include <ydb/core/tx/columnshard/engines/reader/common_reader/common/accessors_ordering.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/constructor/read_metadata.h>

#include <ydb/library/accessor/positive_integer.h>

namespace NKikimr::NOlap {
class TPortionInfo;
}

namespace NKikimr::NOlap::NReader::NSimple {

class TSourceConstructor: public NCommon::TDataSourceConstructor {
private:
    YDB_READONLY_DEF(std::shared_ptr<TPortionInfo>, Portion);
    ui32 RecordsCount = 0;
    bool IsStartedByCursorFlag = false;

    virtual ui64 DoGetEntityRecordsCount() const override {
        return RecordsCount;
    }

public:
    void SetIsStartedByCursor() {
        IsStartedByCursorFlag = true;
    }
    bool GetIsStartedByCursor() const {
        return IsStartedByCursorFlag;
    }

    TSourceConstructor(const std::shared_ptr<TPortionInfo>&& portion, const NReader::ERequestSorting sorting)
        : NCommon::TDataSourceConstructor(
              TReplaceKeyAdapter((sorting == NReader::ERequestSorting::DESC) ? portion->IndexKeyEnd() : portion->IndexKeyStart(),
                  sorting == NReader::ERequestSorting::DESC),
              TReplaceKeyAdapter((sorting == NReader::ERequestSorting::DESC) ? portion->IndexKeyStart() : portion->IndexKeyEnd(),
                  sorting == NReader::ERequestSorting::DESC))
        , Portion(std::move(portion))
        , RecordsCount(portion->GetRecordsCount())
    {
    }

    std::shared_ptr<TPortionDataSource> Construct(const std::shared_ptr<NCommon::TSpecialReadContext>& context, std::shared_ptr<TPortionDataAccessor>&& accessor) const;

    virtual bool QueryAgnosticLess(const TDataSourceConstructor& rhs) const override {
        return Portion->GetPortionId() < VerifyDynamicCast<const TSourceConstructor*>(&rhs)->GetPortion()->GetPortionId();
    }
};

class TPortionsSources: public NCommon::TSourcesConstructorWithAccessors<TSourceConstructor> {
private:
    using TBase = NCommon::TSourcesConstructorWithAccessors<TSourceConstructor>;

    virtual void DoFillReadStats(TReadStats& stats) const override {
        ui64 compactedPortionsBytes = 0;
        ui64 insertedPortionsBytes = 0;
        ui64 committedPortionsBytes = 0;
        for (auto&& i : TBase::GetConstructors()) {
            if (i.GetPortion()->GetPortionType() == EPortionType::Compacted) {
                compactedPortionsBytes += i.GetPortion()->GetTotalBlobBytes();
            } else if (i.GetPortion()->GetProduced() == NPortion::EProduced::INSERTED) {
                insertedPortionsBytes += i.GetPortion()->GetTotalBlobBytes();
            } else {
                committedPortionsBytes += i.GetPortion()->GetTotalBlobBytes();
            }
        }
        stats.IndexPortions = TBase::GetConstructorsCount();
        stats.InsertedPortionsBytes = insertedPortionsBytes;
        stats.CompactedPortionsBytes = compactedPortionsBytes;
        stats.CommittedPortionsBytes = committedPortionsBytes;
    }

    virtual void DoInitCursor(const std::shared_ptr<IScanCursor>& cursor) override;

    virtual std::vector<TInsertWriteId> GetUncommittedWriteIds() const override;

    virtual std::shared_ptr<NCommon::IDataSource> DoExtractNextImpl(const std::shared_ptr<NCommon::TSpecialReadContext>& context) override {
        auto constructor = TBase::PopObjectWithAccessor();
        return constructor.MutableObject().Construct(context, constructor.DetachAccessor());
    }

public:
    TPortionsSources(std::deque<TSourceConstructor>&& sources, const ERequestSorting sorting)
        : TBase(sorting) {
        InitializeConstructors(std::move(sources));
    }

    static std::unique_ptr<TPortionsSources> BuildEmpty() {
        std::deque<TSourceConstructor> sources;
        return std::make_unique<TPortionsSources>(std::move(sources), ERequestSorting::NONE);
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple
