#pragma once

#include <ydb/core/base/appdata_fwd.h>

#include <library/cpp/random_provider/random_provider.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/random/random.h>

namespace NKikimr {
namespace NUtil {

    inline float NormalizeFreeSpaceShare(float x) {
        // Using a second-order approximation: log1p(x) ≈ x - x^2 / 2, but with a bit steeper curve, hence 3.0f.
        return x - (x * x) / 3.0f;
    }

    inline void CreateNormalizedSharesVector(const THashMap<ui32, float>& shares, const TVector<ui8>& channels, TVector<float>& normalizedShares) {
        normalizedShares.reserve(channels.size());

        float totalWeight = 0.0f;

        // Calculate the total weight
        for (size_t i = 0; i < channels.size(); i++) {
            const ui8 channel = channels[i];
            auto it = shares.find(channel);
            float spaceShare = 1.f;

            if (it != shares.end()) {
                float share = it->second; 
                
                if (share != 0.f) {
                     // zero means absence of correct data, see BlobStorage API
                    spaceShare = share;
                }
            }

            spaceShare = NormalizeFreeSpaceShare(spaceShare);

            totalWeight += spaceShare;
            normalizedShares.push_back(totalWeight);
        }
    }

    inline ui8 SelectChannel(const TVector<float>& normalizedShares, const TVector<ui8>& channels) {
        Y_ABORT_UNLESS(normalizedShares.size() == channels.size(), "Normalized shares and channels sizes mismatch");

        if (channels.size() == 1) {
            return channels[0];
        }

        float totalWeight = normalizedShares.back();

        // Generate a random value between 0 and totalWeight
        float randomWeight = totalWeight * TAppData::RandomProvider->GenRandReal1();

        auto it = std::lower_bound(normalizedShares.begin(), normalizedShares.end(), randomWeight);

        if (it == normalizedShares.end()) {
            return channels.back();
        }

        size_t index = std::distance(normalizedShares.begin(), it);

        return channels[index];
    }

    inline ui8 SelectChannel(const THashMap<ui32, float>& shares, const TVector<ui8>& channels) {
        if (channels.size() == 1) {
            return channels[0];
        }

        TVector<float> normalizedShares;
        normalizedShares.reserve(channels.size());

        CreateNormalizedSharesVector(shares, channels, normalizedShares);

        return SelectChannel(normalizedShares, channels);
    }

    struct TChannelsShares {
        THashMap<ui32, float> Shares; /** Normalized free space shares. */
        mutable TVector<float> PrefixSums; /** Prefix sum array for binary search. */
        mutable bool IsValid;

        TChannelsShares() : IsValid(false) {}

        explicit TChannelsShares(const THashMap<ui32, float>& normalizedShares)
            : Shares(normalizedShares)
            , IsValid(false) {}

        void Update(ui32 channel, float share) {
            Shares[channel] = NormalizeFreeSpaceShare(share);
            IsValid = false;
        }

        ui8 Select(const TVector<ui8>& channels) const {
            if (!IsValid) {
                PrefixSums.clear();
                CreateNormalizedSharesVector(Shares, channels, PrefixSums);
                IsValid = true;
            }
            return SelectChannel(PrefixSums, channels);
        }
    };
}
}
