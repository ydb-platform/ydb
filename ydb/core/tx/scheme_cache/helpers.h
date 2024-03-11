#pragma once

#include <ydb/core/base/appdata.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <util/string/builder.h>

#include <functional>

namespace NKikimr::NSchemeCache {

struct TSchemeCacheHelpers {
    using TNavigate = TSchemeCacheNavigate;
    using TEvNavigate = TEvTxProxySchemeCache::TEvNavigateKeySet;
    using TResolve = TSchemeCacheRequest;
    using TEvResolve = TEvTxProxySchemeCache::TEvResolveKeySet;
    using TCheckFailFunc = std::function<void(const TString&)>;

    inline static TNavigate::TEntry MakeNavigateEntry(const TTableId& tableId, TNavigate::EOp op) {
        TNavigate::TEntry entry;
        entry.RequestType = TNavigate::TEntry::ERequestType::ByTableId;
        entry.TableId = tableId;
        entry.Operation = op;
        entry.ShowPrivatePath = true;
        return entry;
    }

    template <typename T>
    static bool CheckNotEmpty(const TStringBuf marker, const TAutoPtr<T>& result, TCheckFailFunc onFailure) {
        if (result) {
            return true;
        }

        onFailure(TStringBuilder() << "Empty result at '" << marker << "'");
        return false;
    }

    template <typename T>
    static bool CheckEntriesCount(const TStringBuf marker, const TAutoPtr<T>& result, ui32 expected, TCheckFailFunc onFailure) {
        if (result->ResultSet.size() == expected) {
            return true;
        }

        onFailure(TStringBuilder() << "Unexpected entries count at '" << marker << "'"
            << ": expected# " << expected
            << ", got# " << result->ResultSet.size()
            << ", result# " << result->ToString(*AppData()->TypeRegistry));
        return false;
    }

    inline static const TTableId& GetTableId(const TNavigate::TEntry& entry) {
        return entry.TableId;
    }

    inline static const TTableId& GetTableId(const TResolve::TEntry& entry) {
        return entry.KeyDescription->TableId;
    }

    template <typename T>
    static bool CheckTableId(const TStringBuf marker, const T& entry, const TTableId& expected, TCheckFailFunc onFailure) {
        if (GetTableId(entry).HasSamePath(expected)) {
            return true;
        }

        onFailure(TStringBuilder() << "Unexpected table id at '" << marker << "'"
            << ": expected# " << expected
            << ", got# " << GetTableId(entry)
            << ", entry# " << entry.ToString());
        return false;
    }

    inline static bool IsSucceeded(TNavigate::EStatus status) {
        return status == TNavigate::EStatus::Ok;
    }

    inline static bool IsSucceeded(TResolve::EStatus status) {
        return status == TResolve::EStatus::OkData;
    }

    template <typename T>
    static bool CheckEntrySucceeded(const TStringBuf marker, const T& entry, TCheckFailFunc onFailure) {
        if (IsSucceeded(entry.Status)) {
            return true;
        }

        onFailure(TStringBuilder() << "Failed entry at '" << marker << "'"
            << ": entry# " << entry.ToString());
        return false;
    }

    template <typename T>
    static bool CheckEntryKind(const TStringBuf marker, const T& entry, TNavigate::EKind expected, TCheckFailFunc onFailure) {
        if (entry.Kind == expected) {
            return true;
        }

        onFailure(TStringBuilder() << "Unexpected entry kind at '" << marker << "'"
            << ", expected# " << static_cast<ui32>(expected)
            << ", got# " << static_cast<ui32>(entry.Kind)
            << ", entry# " << entry.ToString());
        return false;
    }

}; // TSchemeCacheHelpers

}
