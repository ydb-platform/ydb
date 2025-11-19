#pragma once
#include <ydb/core/formats/arrow/rows/view.h>

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap {
class TPortionInfo;
namespace NReader {
class TReadMetadataBase;
}
}

namespace NKikimr::NOlap::NReader::NCommon {

class TReplaceKeyAdapter {
private:
    bool Reverse = false;
    NArrow::TSimpleRow Value;

public:
    static TReplaceKeyAdapter BuildStart(const TPortionInfo& portion, const TReadMetadataBase& readMetadata);
    static TReplaceKeyAdapter BuildFinish(const TPortionInfo& portion, const TReadMetadataBase& readMetadata);

    const NArrow::TSimpleRow& GetValue() const {
        return Value;
    }

    NArrow::TSimpleRow CopyValue() const {
        return Value;
    }

    explicit TReplaceKeyAdapter(NArrow::TSimpleRow&& rk, const bool reverse)
        : Reverse(reverse)
        , Value(std::move(rk)) {
    }

    std::partial_ordering Compare(const TReplaceKeyAdapter& item) const;

    bool operator<(const TReplaceKeyAdapter& item) const {
        return Compare(item) == std::partial_ordering::less;
    }

    TString DebugString() const {
        return TStringBuilder() << "point:{" << Value.DebugString() << "};reverse:" << Reverse << ";";
    }
};

class TCompareKeyForScanSequence {
private:
    TReplaceKeyAdapter Key;
    YDB_READONLY(ui32, SourceId, 0);

public:
    const TReplaceKeyAdapter GetKey() const {
        return Key;
    }

    explicit TCompareKeyForScanSequence(const TReplaceKeyAdapter& key, const ui32 sourceId)
        : Key(key)
        , SourceId(sourceId) {
    }

    static TCompareKeyForScanSequence BorderStart(const TReplaceKeyAdapter& key) {
        return TCompareKeyForScanSequence(key, 0);
    }

    bool operator<(const TCompareKeyForScanSequence& item) const {
        const std::partial_ordering compareResult = Key.Compare(item.Key);
        if (compareResult == std::partial_ordering::equivalent) {
            return SourceId < item.SourceId;
        } else {
            return compareResult == std::partial_ordering::less;
        }
    };
};

}   // namespace NKikimr::NOlap::NReader::NCommon
