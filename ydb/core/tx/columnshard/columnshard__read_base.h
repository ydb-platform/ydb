#pragma once
#include "engines/reader/description.h"
#include <ydb/core/tx/columnshard/columnshard_impl.h>

namespace NKikimr::NColumnShard {

/// Read portion of data in OLAP transaction
class TTxReadBase : public TTransactionBase<TColumnShard> {
protected:
    explicit TTxReadBase(TColumnShard* self)
        : TBase(self)
    {}

    std::shared_ptr<NOlap::TReadMetadata> PrepareReadMetadata(
                                    const TActorContext& ctx,
                                    const NOlap::TReadDescription& readDescription,
                                    const std::unique_ptr<NOlap::TInsertTable>& insertTable,
                                    const std::unique_ptr<NOlap::IColumnEngine>& index,
                                    const TBatchCache& batchCache,
                                    TString& error, const bool isReverse) const;

protected:
    bool ParseProgram(
        const TActorContext& ctx,
        NKikimrSchemeOp::EOlapProgramType programType,
        TString serializedProgram,
        NOlap::TReadDescription& read,
        const NOlap::IColumnResolver& columnResolver
    );

protected:
    TString ErrorDescription;
};

}
