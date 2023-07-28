#include "mkql_helpers.h"

#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <ydb/library/yql/minikql/mkql_node_cast.h>

#include <util/generic/strbuf.h>
#include <util/generic/utility.h>
#include <util/generic/ylimits.h>
#include <util/generic/hash.h>

namespace NYql {

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;

namespace {

bool UpdateRecordsRange(TRecordsRange& range, TStringBuf settingName, ui64 data)
{
    if (settingName == TStringBuf("take")) {
        range.Limit = Min(data, range.Limit.GetOrElse(Max<ui64>()));
    } else if (settingName == TStringBuf("skip")) {
        if (range.Limit.Defined()) {
            if (data >= range.Limit.GetRef()) {
                range.Limit = 0;
                range.Offset = 0;
                return false;
            }

            range.Offset = data;
            range.Limit = range.Limit.GetRef() - data;
        } else {
            ui64 prevOffset = range.Offset.GetOrElse(0);
            if (data > Max<ui64>() - prevOffset) {
                range.Limit = 0;
                range.Offset = 0;
                return false;
            }

            range.Offset = data + prevOffset;
        }
    }
    return true;
}

const THashMap<TStringBuf, NUdf::TDataTypeId> SYS_COLUMN_TYPE_IDS = {
    {"path", NUdf::TDataType<char*>::Id},
    {"record", NUdf::TDataType<ui64>::Id},
    {"index", NUdf::TDataType<ui32>::Id},
    {"num", NUdf::TDataType<ui64>::Id},
    {"keyswitch", NUdf::TDataType<bool>::Id},
};

}

void TRecordsRange::Fill(const TExprNode& settingsNode) {
    Offset.Clear();
    Limit.Clear();

    for (auto& setting: settingsNode.Children()) {
        if (setting->ChildrenSize() == 0) {
            continue;
        }

        auto settingName = setting->Child(0)->Content();
        if (settingName != TStringBuf("take") && settingName != TStringBuf("skip")) {
            continue;
        }
        YQL_ENSURE(setting->Child(1)->IsCallable("Uint64"));
        if (!UpdateRecordsRange(*this, settingName, NYql::FromString<ui64>(*setting->Child(1)->Child(0), NUdf::EDataSlot::Uint64))) {
            break;
        }
    }
}

TType* GetRecordType(TType* type) {
    if (type->GetKind() == TType::EKind::List) {
        return AS_TYPE(TListType, type)->GetItemType();
    } else if (type->GetKind() == TType::EKind::Optional) {
        return AS_TYPE(TOptionalType, type)->GetItemType();
    } else if (type->GetKind() == TType::EKind::Stream) {
        return AS_TYPE(TStreamType, type)->GetItemType();
    }

    return type;
}

NUdf::TDataTypeId GetSysColumnTypeId(TStringBuf column) {
    auto p = SYS_COLUMN_TYPE_IDS.FindPtr(column);
    YQL_ENSURE(p, "Unsupported system column:" << column);
    return *p;
}

} // NYql
