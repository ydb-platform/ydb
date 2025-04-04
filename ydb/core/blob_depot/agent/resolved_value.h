#pragma once

#include "defs.h"

namespace NKikimr::NBlobDepot {

    struct TResolvedValue {
        struct TLink {
            std::optional<std::tuple<TLogoBlobID, ui32>> Blob;
            std::optional<TS3Locator> S3Locator;
            ui32 SubrangeBegin;
            ui32 SubrangeEnd;

            TLink(const NKikimrBlobDepot::TResolvedValueChain& link);

            void Output(IOutputStream& s) const;
            TString ToString() const;

            friend std::strong_ordering operator <=>(const TLink&, const TLink&) = default;
        };

        bool Defined = false;
        bool ReliablyWritten = false;
        ui32 Version = 0;
        std::vector<TLink> Chain;

        TResolvedValue() = default;
        TResolvedValue(const TResolvedValue&) = default;
        TResolvedValue(TResolvedValue&&) = default;
        TResolvedValue(const NKikimrBlobDepot::TEvResolveResult::TResolvedKey& item);

        TResolvedValue& operator =(const TResolvedValue&) = default;
        TResolvedValue& operator =(TResolvedValue&&) = default;

        bool Supersedes(const TResolvedValue& old) const;
        void Output(IOutputStream& s) const;
        TString ToString() const;

        bool IsEmpty() const { // check if no data attached
            return Chain.empty();
        }
    };

} // NKikimr::NBlobDepot
