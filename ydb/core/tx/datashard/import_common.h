#pragma once

#include "defs.h"
#include "datashard_user_table.h"

#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/tablet_flat/util_basics.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#if defined IMPORT_LOG_T || \
    defined IMPORT_LOG_D || \
    defined IMPORT_LOG_I || \
    defined IMPORT_LOG_N || \
    defined IMPORT_LOG_W || \
    defined IMPORT_LOG_E || \
    defined IMPORT_LOG_C
#error log macro redefinition
#endif

#define IMPORT_LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::DATASHARD_RESTORE, "[Import] [" << LogPrefix() << "] " << stream)
#define IMPORT_LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::DATASHARD_RESTORE, "[Import] [" << LogPrefix() << "] " << stream)
#define IMPORT_LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::DATASHARD_RESTORE, "[Import] [" << LogPrefix() << "] " << stream)
#define IMPORT_LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::DATASHARD_RESTORE, "[Import] [" << LogPrefix() << "] " << stream)
#define IMPORT_LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::DATASHARD_RESTORE, "[Import] [" << LogPrefix() << "] " << stream)
#define IMPORT_LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::DATASHARD_RESTORE, "[Import] [" << LogPrefix() << "] " << stream)
#define IMPORT_LOG_C(stream) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::DATASHARD_RESTORE, "[Import] [" << LogPrefix() << "] " << stream)

namespace NKikimr {
namespace NDataShard {

struct TImportJobProduct: public IDestructable {
    bool Success;
    TString Error;
    ui64 BytesWritten;
    ui64 RowsWritten;

    explicit TImportJobProduct(bool success, TString error, ui64 bytes, ui64 rows)
        : Success(success)
        , Error(std::move(error))
        , BytesWritten(bytes)
        , RowsWritten(rows)
    {
    }

}; // TImportJobProduct

class TTableInfo {
    using TColumnIdIndex = TMap<ui32, TUserTable::TUserColumn>;
    using TColumnNameIndex = THashMap<TString, TColumnIdIndex::const_iterator>;

    static TColumnNameIndex MakeColumnNameIndex(TUserTable::TCPtr info) {
        TColumnNameIndex result;

        auto it = info->Columns.begin();
        while (it != info->Columns.end()) {
            Y_ABORT_UNLESS(result.emplace(it->second.Name, it).second);
            it++;
        }

        return result;
    }

public:
    explicit TTableInfo(ui64 id, TUserTable::TCPtr info)
        : Id(id)
        , Info(info)
        , ColumnNameIndex(MakeColumnNameIndex(info))
    {
    }

    ui64 GetId() const {
        return Id;
    }

    bool HasColumn(const TString& name) const {
        return ColumnNameIndex.contains(name);
    }

    std::pair<NScheme::TTypeInfo, TString> GetColumnType(const TString& name) const {
        auto it = ColumnNameIndex.find(name);
        Y_ABORT_UNLESS(it != ColumnNameIndex.end());
        auto& column = it->second->second;
        return {column.Type, column.TypeMod};
    }

    const TVector<ui32>& GetKeyColumnIds() const {
        return Info->KeyColumnIds;
    }

    TVector<ui32> GetValueColumnIds(const TVector<TString>& order) const {
        TVector<ui32> result;
        for (const auto& name : order) {
            auto it = ColumnNameIndex.find(name);
            Y_ABORT_UNLESS(it != ColumnNameIndex.end());

            if (it->second->second.IsKey) {
                continue;
            }

            result.push_back(it->second->first);
        }

        return result;
    }

    ui32 KeyOrder(const TString& name) const {
        auto it = ColumnNameIndex.find(name);
        Y_ABORT_UNLESS(it != ColumnNameIndex.end());

        if (!it->second->second.IsKey) {
            return Max<ui32>();
        }

        return FindIndex(Info->KeyColumnIds, it->second->first);
    }

    bool IsMyKey(const TVector<TCell>& key) const {
        return ComparePointAndRange(key, Info->GetTableRange(), Info->KeyColumnTypes, Info->KeyColumnTypes) == 0;
    }

private:
    const ui64 Id;
    TUserTable::TCPtr Info;
    const TColumnNameIndex ColumnNameIndex;

}; // TTableInfo

} // NDataShard
} // NKikimr
