#pragma once

#include <ydb/library/yql/minikql/mkql_node.h>

#include <util/generic/maybe.h>
#include <util/generic/strbuf.h>

namespace NYql {

NKikimr::NMiniKQL::TType* GetRecordType(NKikimr::NMiniKQL::TType* type);

class TExprNode;

struct TRecordsRange {
    TMaybe<ui64> Offset;
    TMaybe<ui64> Limit;

    explicit operator bool() const {
        return Offset.Defined() || Limit.Defined();
    }

    void Fill(const TExprNode& settingsNode);
};

NUdf::TDataTypeId GetSysColumnTypeId(TStringBuf column);

}
