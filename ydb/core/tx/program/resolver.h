#pragma once
#include <ydb/core/formats/arrow/program/abstract.h>
#include <ydb/core/tx/schemeshard/olap/schema/schema.h>

namespace NKikimr::NArrow::NSSA {

class TSchemaResolverColumnsOnly: public IColumnResolver {
private:
    std::shared_ptr<NSchemeShard::TOlapSchema> Schema;

public:
    TSchemaResolverColumnsOnly(const std::shared_ptr<NSchemeShard::TOlapSchema>& schema)
        : Schema(schema) {
        AFL_VERIFY(Schema);
    }

    virtual TString GetColumnName(ui32 id, bool required = true) const override;
    virtual std::optional<ui32> GetColumnIdOptional(const TString& name) const override;
    virtual TColumnInfo GetDefaultColumn() const override;
};

}   // namespace NKikimr::NArrow::NSSA
