#include "address.h"

#include <ydb/core/tx/columnshard/engines/scheme/tiering/common.h>

#include <ydb/library/actors/core/log.h>

#include <util/digest/numeric.h>
#include <util/generic/string_hash.h>
#include <util/string/join.h>

namespace NKikimr::NOlap::NActualizer {

TRWAddress::TRWAddress(std::set<TString>&& readStorages, std::set<TString>&& writeStorages): ReadStorages(std::move(readStorages))
, WriteStorages(std::move(writeStorages)) {
    AFL_VERIFY(!ReadStorages.contains(""));
    AFL_VERIFY(!WriteStorages.contains(""));
    if (WriteStorages.contains(NTiering::NCommon::DeleteTierName)) {
        AFL_VERIFY(WriteStorages.size() == 1);
    }
    Hash = 0;
    for (auto&& i : ReadStorages) {
        Hash = CombineHashes(Hash, CityHash64(i.data(), i.size()));
    }
    for (auto&& i : WriteStorages) {
        Hash = CombineHashes(Hash, CityHash64(i.data(), i.size()));
    }
}

TString TRWAddress::DebugString() const {
    return "R:" + JoinSeq(",", ReadStorages) + ";W:" + JoinSeq(",", WriteStorages);
}

}