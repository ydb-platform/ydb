#pragma once

#include "public.h"
#include "comparator.h"

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TColumnSortSchema
{
    TString Name;
    ESortOrder SortOrder;

    bool operator==(const TColumnSortSchema& other) const = default;

    void Persist(const TStreamPersistenceContext& context);
};

void Serialize(const TColumnSortSchema& schema, NYson::IYsonConsumer* consumer);
void Deserialize(TColumnSortSchema& schema, NYTree::INodePtr node);
void Deserialize(TColumnSortSchema& schema, NYson::TYsonPullParserCursor* cursor);

////////////////////////////////////////////////////////////////////////////////

void ValidateSortColumns(const std::vector<TColumnSortSchema>& columns);

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NProto::TSortColumnsExt* protoSortColumns,
    const TSortColumns& sortColumns);

void FromProto(
    TSortColumns* sortColumns,
    const NProto::TSortColumnsExt& protoSortColumns);

void FormatValue(TStringBuilderBase* builder, const TSortColumns& key, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

TKeyColumns GetColumnNames(const TSortColumns& sortColumns);

std::vector<ESortOrder> GetSortOrders(const TSortColumns& sortColumns);

TComparator GetComparator(const TSortColumns& sortColumns);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
