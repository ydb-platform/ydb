#include "schemeshard_build_index_helpers.h"

#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/scheme/scheme_type_order.h>

#include <util/generic/yexception.h>
#include <util/string/builder.h>

namespace NKikimr::NSchemeShard {

bool PerformCrossShardUniqIndexValidation(const std::vector<NScheme::TTypeInfoOrder>& indexColumnTypeInfos, const std::vector<TString>& indexColumns, const std::vector<const TSerializedTableRange*>& ranges, TString& errorDesc) {
    // Compare index shards edge keys
    const TSerializedCellVec* prevLastKey = nullptr;
    for (const TSerializedTableRange* range : ranges) {
        if (range->From) {
            if (prevLastKey) {
                Y_ENSURE(range->From.GetCells().size() == indexColumnTypeInfos.size());
                if (TypedCellVectorsEqualWithNullSemantics(range->From.GetCells().data(), prevLastKey->GetCells().data(), indexColumnTypeInfos.data(), indexColumnTypeInfos.size()) == ETriBool::True) {
                    TStringBuilder err;
                    err << "Duplicate key found: (";
                    for (size_t i = 0; i < indexColumns.size(); ++i) {
                        if (i > 0) {
                            err << ", ";
                        }
                        err << indexColumns[i] << "=";
                        DbgPrintValue(err, range->From.GetCells()[i], indexColumnTypeInfos[i].ToTypeInfo());
                    }
                    err << ")";
                    errorDesc = std::move(err);
                    return false;
                }
            }
        }
        if (range->To) {
            prevLastKey = &range->To;
            Y_ENSURE(prevLastKey->GetCells().size() == indexColumnTypeInfos.size());
        }
    }
    return true;
}

} // namespace NKikimr::NSchemeShard
