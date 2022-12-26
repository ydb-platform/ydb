#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>

class TDigest {
    struct TCentroid {
        double Mean;
        double Count;

        TCentroid()
            : Mean(0)
            , Count(0)
        {
        }
        TCentroid(double x, double weight)
            : Mean(x)
            , Count(weight)
        {
        }

        bool operator<(const TCentroid& centroid) const {
            return Mean < centroid.Mean;
        }

        void Update(double x, double weight) {
            Count += weight;
            Mean += weight * (x - Mean) / Count;
        }
    };

    TVector<TCentroid> Centroids;
    TVector<TCentroid> Unmerged;
    TVector<TCentroid> Merged;
    typedef TVector<TCentroid>::iterator iter_t;
    double N;
    double Delta;
    double K;

    void Add(const TDigest& otherDigest);
    void AddCentroid(const TCentroid& centroid);
    double GetThreshold(double q);

    void MergeCentroid(TVector<TCentroid>& merged, double& sum, const TCentroid& centroid);

protected:
    void Update(double x, double w = 1.0);

public:
    TDigest(double delta = 0.01, double k = 25);
    TDigest(double delta, double k, double firstValue);
    TDigest(TStringBuf serializedDigest);
    TDigest(const TDigest* digest1, const TDigest* digest2); // merge

    TString Serialize();

    TDigest operator+(const TDigest& other);
    TDigest& operator+=(const TDigest& other);

    void AddValue(double value);
    void Compress();
    void Clear();
    double GetPercentile(double percentile);
    double GetRank(double value);
    i64 GetCount() const;
};
