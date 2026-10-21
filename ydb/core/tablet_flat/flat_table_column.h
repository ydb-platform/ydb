#pragma once

#include "flat_row_column.h"

#include <ydb/core/scheme/scheme_tablecell.h>

namespace NKikimr {
namespace NTable {

    struct TColumn {

        enum EDefaults {
            LeaderFamily = 0,
        };

        TColumn() = default;

        TColumn(const TString& name, TTag tag, NScheme::TTypeInfo type,
            const TString& typeMod, bool notNull = false, bool isSensitive = false, bool setNotNullInProgress = false)
            : Id(tag)
            , PType(type)
            , PTypeMod(typeMod)
            , Name(name)
            , NotNull(notNull)
            , SetNotNullInProgress(setNotNullInProgress)
            , IsSensitive(isSensitive)
        {

        }

        // Null may point into Storage, so a copy must re-point it into its own Storage
        TColumn(const TColumn& other)
        {
            *this = other;
        }

        TColumn& operator=(const TColumn& other)
        {
            if (this != &other) {
                Id = other.Id;
                PType = other.PType;
                PTypeMod = other.PTypeMod;
                Name = other.Name;
                Family = other.Family;
                KeyOrder = other.KeyOrder;
                Storage = other.Storage;
                Null = Storage.empty() ? other.Null : TCell(Storage.data(), Storage.size());
                NotNull = other.NotNull;
                SetNotNullInProgress = other.SetNotNullInProgress;
                IsSensitive = other.IsSensitive;
            }
            return *this;
        }

        void SetDefault(const TCell &null)
        {
            if (!null || TCell::CanInline(null.Size())) {
                Storage = { };
                Null = null;
            } else {
                Storage.assign(null.Data(), null.Size());
                Null = TCell(Storage.data(), Storage.size());
            }
        }

        std::optional<ui32> GetCorrectKeyOrder() const {
            if (KeyOrder == Max<TPos>()) {
                return std::nullopt;
            } else {
                return KeyOrder;
            }
        }

        NTable::TTag Id = Max<TTag>();
        NScheme::TTypeInfo PType;
        TString PTypeMod;
        TString Name;
        ui32 Family = LeaderFamily;
        NTable::TPos KeyOrder = Max<TPos>();
        TCell Null;
        TString Storage;
        bool NotNull = false;
        bool SetNotNullInProgress = false;
        bool IsSensitive = false;
    };
}
}
