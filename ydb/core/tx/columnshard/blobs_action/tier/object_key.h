#pragma once

#include <ydb/core/base/logoblob.h>

#include <util/generic/string.h>

namespace NKikimr::NOlap::NBlobOperations::NTier {

class TObjectKey {
public:
    static constexpr ui32 FlatLayoutChannel = TLogoBlobID::MaxChannel;
    static constexpr ui32 TreeLayoutChannel = TLogoBlobID::MaxChannel - 1;

    static ui32 GetChannelForWriting();

    static TString Make(const TLogoBlobID& blobId);

    static bool Parse(const TString& key, TLogoBlobID& blobId, TString& error);
};

}   // namespace NKikimr::NOlap::NBlobOperations::NTier
