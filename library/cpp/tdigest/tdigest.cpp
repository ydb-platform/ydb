#include "tdigest.h"

#include <library/cpp/tdigest/tdigest.pb.h>

#include <cmath>

// TODO: rewrite to https://github.com/tdunning/t-digest/blob/master/src/main/java/com/tdunning/math/stats/MergingDigest.java

TDigest::TDigest(double delta, double k)
    : N(0)
    , Delta(delta)
    , K(k)
{
}

TDigest::TDigest(double delta, double k, double firstValue)
    : TDigest(delta, k)
{
    AddValue(firstValue);
}

TDigest::TDigest(TStringBuf serializedDigest)
    : N(0)
{
    NTDigest::TDigest digest;
    Y_ABORT_UNLESS(digest.ParseFromArray(serializedDigest.data(), serializedDigest.size()));
    Delta = digest.GetDelta();
    K = digest.GetK();
    for (int i = 0; i < digest.centroids_size(); ++i) {
        const NTDigest::TDigest::TCentroid& centroid = digest.centroids(i);
        Update(centroid.GetMean(), centroid.GetWeight());
    }
}

TDigest::TDigest(const TDigest* digest1, const TDigest* digest2)
    : N(0)
    , Delta(std::min(digest1->Delta, digest2->Delta))
    , K(std::max(digest1->K, digest2->K))
{
    Add(*digest1);
    Add(*digest2);
}

void TDigest::Add(const TDigest& otherDigest) {
    for (auto& it : otherDigest.Centroids)
        Update(it.Mean, it.Count);
    for (auto& it : otherDigest.Unmerged)
        Update(it.Mean, it.Count);
}

TDigest TDigest::operator+(const TDigest& other) {
    TDigest T(Delta, K);
    T.Add(*this);
    T.Add(other);
    return T;
}

TDigest& TDigest::operator+=(const TDigest& other) {
    Add(other);
    return *this;
}

void TDigest::AddCentroid(const TCentroid& centroid) {
    Unmerged.push_back(centroid);
    N += centroid.Count;
}

double TDigest::GetThreshold(double q) {
    return 4 * N * Delta * q * (1 - q);
}

void TDigest::MergeCentroid(TVector<TCentroid>& merged, double& sum, const TCentroid& centroid) {
    if (merged.empty()) {
        merged.push_back(centroid);
        sum += centroid.Count;
        return;
    }
    // Use quantile that has the tightest k
    double q1 = (sum - merged.back().Count * 0.5) / N;
    double q2 = (sum + centroid.Count * 0.5) / N;
    double k = GetThreshold(q1);
    double k2 = GetThreshold(q2);
    if (k > k2) {
        k = k2;
    }
    if (merged.back().Count + centroid.Count <= k) {
        merged.back().Update(centroid.Mean, centroid.Count);
    } else {
        merged.push_back(centroid);
    }
    sum += centroid.Count;
}

void TDigest::Update(double x, double w) {
    AddCentroid(TCentroid(x, w));
    if (Unmerged.size() >= K / Delta) {
        Compress();
    }
}

void TDigest::Compress() {
    if (Unmerged.empty())
        return;
    // Merge Centroids and Unmerged into Merged
    std::stable_sort(Unmerged.begin(), Unmerged.end());
    Merged.clear();
    double sum = 0;
    iter_t i = Centroids.begin();
    iter_t j = Unmerged.begin();
    while (i != Centroids.end() && j != Unmerged.end()) {
        if (i->Mean <= j->Mean) {
            MergeCentroid(Merged, sum, *i++);
        } else {
            MergeCentroid(Merged, sum, *j++);
        }
    }
    while (i != Centroids.end()) {
        MergeCentroid(Merged, sum, *i++);
    }
    while (j != Unmerged.end()) {
        MergeCentroid(Merged, sum, *j++);
    }
    swap(Centroids, Merged);
    Unmerged.clear();
}

void TDigest::Clear() {
    Centroids.clear();
    Unmerged.clear();
    N = 0;
}

void TDigest::AddValue(double value) {
    Update(value, 1);
}

double TDigest::GetPercentile(double percentile) {
    Compress();
    if (Centroids.empty())
        return 0.0;
    // This algorithm uses C=1/2 with 0.5 optimized away
    // See https://en.wikipedia.org/wiki/Percentile#First_Variant.2C
    double x = percentile * N;
    double sum = 0.0;
    double prev_x = 0;
    double prev_mean = Centroids.front().Mean;
    for (const auto& C : Centroids) {
        double current_x = sum + C.Count * 0.5;
        if (x <= current_x) {
            double k = (x - prev_x) / (current_x - prev_x);
            return prev_mean + k * (C.Mean - prev_mean);
        }
        sum += C.Count;
        prev_x = current_x;
        prev_mean = C.Mean;
    }
    return Centroids.back().Mean;
}

double TDigest::GetRank(double value) {
    Compress();
    if (Centroids.empty()) {
        return 0.0;
    }
    if (value < Centroids.front().Mean) {
        return 0.0;
    }
    if (value == Centroids.front().Mean) {
        return Centroids.front().Count * 0.5 / N;
    }
    double sum = 0.0;
    double prev_x = 0.0;
    double prev_mean = Centroids.front().Mean;
    for (const auto& C : Centroids) {
        double current_x = sum + C.Count * 0.5;
        if (value <= C.Mean) {
            double k = (value - prev_mean) / (C.Mean - prev_mean);
            return (prev_x + k * (current_x - prev_x)) / N;
        }
        sum += C.Count;
        prev_mean = C.Mean;
        prev_x = current_x;
    }
    return 1.0;
}

TString TDigest::Serialize() {
    Compress();
    NTDigest::TDigest digest;
    digest.SetDelta(Delta);
    digest.SetK(K);
    for (const auto& it : Centroids) {
        NTDigest::TDigest::TCentroid* centroid = digest.AddCentroids();
        centroid->SetMean(it.Mean);
        centroid->SetWeight(it.Count);
    }
    return digest.SerializeAsString();
}

i64 TDigest::GetCount() const {
    return std::llround(N);
}
