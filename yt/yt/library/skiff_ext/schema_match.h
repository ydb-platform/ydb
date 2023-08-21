#pragma once

#include "public.h"

#include <yt/yt/core/misc/property.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/public.h>

#include <library/cpp/skiff/skiff_schema.h>

namespace NYT::NSkiffExt {

////////////////////////////////////////////////////////////////////////////////

extern const TString SparseColumnsName;
extern const TString OtherColumnsName;
extern const TString KeySwitchColumnName;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ERowRangeIndexMode,
    (Incremental)
    (IncrementalWithError)
);

class TFieldDescription
{
public:
    DEFINE_BYREF_RO_PROPERTY(TString, Name);
    DEFINE_BYREF_RO_PROPERTY(std::shared_ptr<NSkiff::TSkiffSchema>, Schema);

public:
    TFieldDescription(TString name, std::shared_ptr<NSkiff::TSkiffSchema> schema);

    bool IsRequired() const;
    bool IsNullable() const;
    std::optional<NSkiff::EWireType> Simplify() const;
    NSkiff::EWireType ValidatedSimplify() const;
};

////////////////////////////////////////////////////////////////////////////////

struct TSkiffTableDescription
{
    // Dense fields of the row.
    std::vector<TFieldDescription> DenseFieldDescriptionList;

    // Sparse fields of the row.
    std::vector<TFieldDescription> SparseFieldDescriptionList;

    // Indexes of $key_switch/$row_index/$range_index field inside dense part of the row.
    std::optional<size_t> KeySwitchFieldIndex;

    std::optional<size_t> RowIndexFieldIndex;
    std::optional<size_t> RangeIndexFieldIndex;

    // $row_index/$range_index field can be written in several modes.
    ERowRangeIndexMode RowIndexMode = ERowRangeIndexMode::Incremental;
    ERowRangeIndexMode RangeIndexMode = ERowRangeIndexMode::Incremental;

    // Whether or not row contains $other_columns field.
    bool HasOtherColumns = false;
};

////////////////////////////////////////////////////////////////////////////////

struct TSkiffTableColumnIds
{
    std::vector<ui16> DenseFieldColumnIds;
    std::vector<ui16> SparseFieldColumnIds;
};

////////////////////////////////////////////////////////////////////////////////

std::vector<TSkiffTableDescription> CreateTableDescriptionList(
    const std::vector<std::shared_ptr<NSkiff::TSkiffSchema>>& skiffSchema,
    const TString& rangeIndexColumnName,
    const TString& rowIndexColumnName);

std::vector<std::shared_ptr<NSkiff::TSkiffSchema>> ParseSkiffSchemas(
    const NYTree::IMapNodePtr& skiffSchemaRegistry,
    const NYTree::IListNodePtr& tableSkiffSchemas);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSkiffExt
