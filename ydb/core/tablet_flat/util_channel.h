#pragma once

#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/random/random.h>

namespace NKikimr {
namespace NTable {

namespace {
    inline float Normalize(float x) {
        // Using a second-order approximation: log1p(x) ≈ x - x^2 / 2, but with a bit steeper curve, hence 3.0f.
        return x - (x * x) / 3.0f;
    }
} // anonymous namespace

    inline ui8 SelectChannel(const THashMap<ui32, float>& shares, const TVector<ui8>& channels) {
        float totalWeight = 0.0f;

        TVector<float> normalizedShares;
        normalizedShares.reserve(channels.size());

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

            spaceShare = Normalize(spaceShare);

            totalWeight += spaceShare;
            normalizedShares.push_back(spaceShare);
        }

        // Generate a random value between 0 and totalWeight
        float randomWeight = totalWeight * RandomNumber<float>();

        ui8 bestChannel = channels[0];

        // Iterate and select the channel based on the segment random weight falls into
        for (size_t i = 0; i < channels.size(); i++) {
            const ui8 channel = channels[i];
            float share = normalizedShares[i];
            if (randomWeight <= share) {
                bestChannel = channel;
                break;
            }
            randomWeight -= share;
        }

        return bestChannel;
    }
}
}
