#pragma once
#include "defs.h"
#include "keyvalue_intermediate.h"
#include "keyvalue_item_type.h"
#include <ydb/core/base/logoblob.h>

namespace NKikimr {
namespace NKeyValue {

struct TIndexRecord {
    struct TChainItem {
        TLogoBlobID LogoBlobId;
        TRope InlineData;
        ui64 Offset;

        TChainItem(const TLogoBlobID &id, ui64 offset);
        TChainItem(TRope&& inlineData, ui64 offset);
        bool IsInline() const;
        ui64 GetSize() const;

        // ordering operator for LowerBound
        friend bool operator<(ui64 left, const TChainItem& right);

        // equlity operator for testing
        bool operator==(const TChainItem& right) const;
    };

    TVector<TChainItem> Chain;
    ui64 CreationUnixTime;

    TIndexRecord();
    TIndexRecord(const TIndexRecord& /*other*/) = delete;
    ui64 GetFullValueSize() const;
    ui32 GetReadItems(ui64 offset, ui64 size, TIntermediate::TRead& read) const;
    TString Serialize() const;
    bool Deserialize1(const TString &rawData, TString &outErrorInfo);
    bool Deserialize2(const TString &rawData, TString &outErrorInfo);
    static EItemType ReadItemType(const TString &rawData);

    // equlity operator for testing
    bool operator==(const TIndexRecord& right) const;
};

} // NKeyValue
} // NKikimr
