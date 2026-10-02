#pragma once

#include <yql/essentials/utils/yql_panic.h>
#include <yql/essentials/public/udf/udf_data_type.h>
#include <yql/essentials/minikql/mkql_node.h>
// #include <ydb/library/yql/dq/proto/dq_tasks.pb.h>

namespace NYql::NDq {

struct TColumnInfo {
    TString Name;
    ui32 Index;
    NKikimr::NMiniKQL::TType* OriginalType; // may be optional
    NKikimr::NMiniKQL::TType* DataType;     // optionality removed
    TMaybe<bool> IsScalar; // defined only on block types

    TColumnInfo(TString name, ui32 index, NKikimr::NMiniKQL::TType* type, TMaybe<bool> isScalar)
        : Name(name)
        , Index(index)
        , OriginalType(type)
        , IsScalar(isScalar)
    {
        DataType = (type->GetKind() == NKikimr::NMiniKQL::TType::EKind::Optional) ? static_cast<NKikimr::NMiniKQL::TOptionalType&>(*type).GetItemType() : type;
    }

    bool IsBlockOrScalar() const {
        return IsScalar.Defined();
    }

    NUdf::TDataTypeId GetTypeId() const {
        YQL_ENSURE(DataType->GetKind() == NKikimr::NMiniKQL::TType::EKind::Data);
        return static_cast<NKikimr::NMiniKQL::TDataType&>(*DataType).GetSchemeType();
    }
};

struct TSortColumnInfo : public TColumnInfo {
    bool Ascending;

    TSortColumnInfo(const TColumnInfo&& colInfo)
        : TColumnInfo(std::move(colInfo))
        , Ascending(false)
    {}
};

TMaybe<TColumnInfo> FindColumnInfo(const NKikimr::NMiniKQL::TType* type, TStringBuf column);
TColumnInfo GetColumnInfo(const NKikimr::NMiniKQL::TType* type, TStringBuf column);

template<typename TList>
void GetColumnsInfo(const NKikimr::NMiniKQL::TType* type, const TList& columns,
    TVector<TColumnInfo>& result)
{
    result.clear();
    result.reserve(columns.size());
    for (auto& column : columns) {
        result.emplace_back(GetColumnInfo(type, column));
    }
}

template<typename TList>
void GetSortColumnsInfo(const NKikimr::NMiniKQL::TType* type, const TList& protoSortCols,
    TVector<TSortColumnInfo>& sortCols)
{
    sortCols.clear();
    sortCols.reserve(protoSortCols.size());
    for (const auto& protoSortCol : protoSortCols) {
        TSortColumnInfo colInfo = static_cast<TSortColumnInfo>(GetColumnInfo(type, protoSortCol.GetColumn()));
        colInfo.Ascending = protoSortCol.GetAscending();
        sortCols.emplace_back(std::move(colInfo));
    }
}

}
