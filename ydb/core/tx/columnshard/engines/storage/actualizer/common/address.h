#pragma once
#include <set>
#include <util/generic/string.h>

namespace NKikimr::NOlap::NActualizer {

class TRWAddress {
private:
    std::set<TString> ReadStorages;
    std::set<TString> WriteStorages;
    ui64 Hash = 0;
public:
    bool WriteIs(const TString& storageId) const {
        return WriteStorages.size() == 1 && WriteStorages.contains(storageId);
    }

    bool ReadIs(const TString& storageId) const {
        return ReadStorages.size() == 1 && ReadStorages.contains(storageId);
    }

    TString DebugString() const;

    TRWAddress(std::set<TString>&& readStorages, std::set<TString>&& writeStorages);

    bool operator==(const TRWAddress& item) const {
        return ReadStorages == item.ReadStorages && WriteStorages == item.WriteStorages;
    }

    operator size_t() const { 
        return Hash;
    }
};

}