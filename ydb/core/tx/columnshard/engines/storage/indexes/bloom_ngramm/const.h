#pragma once
#include <ydb/core/tx/columnshard/engines/scheme/indexes/abstract/constructor.h>
namespace NKikimr::NOlap::NIndexes::NBloomNGramm {

class TConstants {
public:
    static constexpr ui32 MinNGrammSize = 3;
    static constexpr ui32 MaxNGrammSize = 8;
    static constexpr ui32 MinHashesCount = 1;
    static constexpr ui32 MaxHashesCount = 8;
    static constexpr ui32 MinFilterSizeBytes = 128;
    static constexpr ui32 MaxFilterSizeBytes = 1 << 20;
    static constexpr ui32 MinRecordsCount = 128;
    static constexpr ui32 MaxRecordsCount = 1000000;

    static bool CheckRecordsCount(const ui32 value) {
        return MinRecordsCount <= value && value <= MaxRecordsCount;
    }

    static bool CheckNGrammSize(const ui32 value) {
        return MinNGrammSize <= value && value <= MaxNGrammSize;
    }

    static bool CheckHashesCount(const ui32 value) {
        return MinHashesCount <= value && value <= MaxHashesCount;
    }

    static bool CheckFilterSizeBytes(const ui32 value) {
        return MinFilterSizeBytes <= value && value <= MaxFilterSizeBytes;
    }

    static TString GetHashesCountIntervalString();
    static TString GetFilterSizeBytesIntervalString();
    static TString GetNGrammSizeIntervalString();
    static TString GetRecordsCountIntervalString();
};

}   // namespace NKikimr::NOlap::NIndexes::NBloomNGramm
