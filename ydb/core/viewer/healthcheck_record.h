#pragma once
#include <util/digest/numeric.h>
#include <util/generic/string.h>

namespace NKikimr::NViewer {

struct TMetricRecord {
    TString Database;
    TString Message;
    TString Status;
    TString Type;

    bool operator!=(const TMetricRecord& x) const noexcept {
        return !(x == *this);
    }

    bool operator==(const TMetricRecord& x) const noexcept {
        return this->Database == x.Database && this->Message == x.Message && this->Status == x.Status && this->Type == x.Type;
    }

    ui64 Hash() const noexcept {
        ui64 hash = std::hash<TString>()(Database);
        hash = CombineHashes<ui64>(hash, std::hash<TString>()(Message));
        hash = CombineHashes<ui64>(hash, std::hash<TString>()(Status));
        hash = CombineHashes<ui64>(hash, std::hash<TString>()(Type));
        return hash;
    }

    struct THash {
        ui64 operator()(const TMetricRecord& record) const noexcept {
            return record.Hash();
        }
    };
};

}

template<>
struct THash<NKikimr::NViewer::TMetricRecord> {
    inline ui64 operator()(const NKikimr::NViewer::TMetricRecord& x) const noexcept {
        return x.Hash();
    }
};
