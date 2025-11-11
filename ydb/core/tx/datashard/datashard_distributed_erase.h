#pragma once

#include "defs.h"

#include <ydb/core/scheme/scheme_tabledefs.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/stream/str.h>

namespace NKikimr {
namespace NDataShard {

using TKeyMap = TVector<std::pair<ui32, ui32>>; // index to main
using TIndexes = THashMap<TTableId, TKeyMap>;

IActor* CreateDistributedEraser(const TActorId& replyTo, const TTableId& mainTableId, const TIndexes& indexes);

template <typename TBitMapType>
static TString SerializeBitMap(const TBitMapType& bitmap) {
    TString serialized;
    TStringOutput output(serialized);
    bitmap.Save(&output);
    return serialized;
}

template <typename TBitMapType>
static TBitMapType DeserializeBitMap(const TString& serialized) {
    TStringInput input(serialized);
    TBitMapType bitmap;
    bitmap.Load(&input);
    return bitmap;
}

} // NDataShard
} // NKikimr
