#pragma once
#include <ydb/core/tx/columnshard/common/blob.h>

#include <util/system/types.h>

namespace NKikimr::NOlap::NIndexes {

class TChunkOriginalData {
private:
    std::optional<TBlobRange> Range;
    std::optional<TString> Data;

public:
    explicit TChunkOriginalData(const TBlobRange& range)
        : Range(range) {
    }
    explicit TChunkOriginalData(const TString& data)
        : Data(data) {
    }

    ui32 GetSize() const {
        if (Range) {
            return Range->GetSize();
        } else {
            return Data->size();
        }
    }

    bool HasData() const {
        return !!Data;
    }
    const TBlobRange& GetBlobRangeVerified() const;
    const TString& GetDataVerified() const;
};

}   // namespace NKikimr::NOlap::NIndexes
