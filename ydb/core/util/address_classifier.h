#pragma once

#include <ydb/core/protos/netclassifier.pb.h>

#include <library/cpp/ipmath/range_set.h>

#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>

#include <vector>

namespace NKikimr::NAddressClassifier {

TString ExtractAddress(const TString& peer);

TString ParseAddress(const TString& address, TString& hostname, ui32& port);

class TAddressClassifier {
public:
    bool AddNetByCidrAndLabel(const TString& cidr, const size_t label);
    std::pair<bool, size_t> ClassifyAddress(const TString& address) const;

    static constexpr std::pair<bool, size_t> UnknownAddressClass = {false, 0};

    TAddressClassifier() = default;
    TAddressClassifier(TAddressClassifier&&) = default;
    TAddressClassifier(const TAddressClassifier&) = delete;

private:
    THashMap<int, TIpRangeSet> RegisteredV4Nets;
    THashMap<int, TIpRangeSet> RegisteredV6Nets;
};

class TLabeledAddressClassifier : public TAtomicRefCount<TLabeledAddressClassifier>, TNonCopyable {
public:
    using TConstPtr = TIntrusiveConstPtr<TLabeledAddressClassifier>;

public:
    TLabeledAddressClassifier(TAddressClassifier&& addressClassifier, std::vector<TString>&& labels);
    static TConstPtr MakeLabeledAddressClassifier(TAddressClassifier&& addressClassifier, std::vector<TString>&& labels);
    TMaybe<TString> ClassifyAddress(const TString& address) const;

    const std::vector<TString>& GetLabels() const;

private:
    TAddressClassifier Classifier;
    std::vector<TString> Labels;
};

template <typename TNetData>
TLabeledAddressClassifier::TConstPtr BuildLabeledAddressClassifierFromNetData(const TNetData& netData) {
    TAddressClassifier addressClassifier;
    THashMap<TString, size_t> knownLabels;
    std::vector<TString> labels;

    for (size_t i = 0; i < netData.SubnetsSize(); ++i) {
        const auto& subnet = netData.GetSubnets(i);

        auto it = knownLabels.find(subnet.GetLabel());

        size_t subnetIntLabel = 0;
        if (it == knownLabels.end()) {
            labels.push_back(subnet.GetLabel());
            subnetIntLabel = labels.size() - 1;
            knownLabels.insert(std::make_pair(labels.back(), subnetIntLabel));
        } else {
            subnetIntLabel = it->second;
        }

        if (!addressClassifier.AddNetByCidrAndLabel(subnet.GetMask(), subnetIntLabel)) {
            return nullptr;
        }
    }

    return TLabeledAddressClassifier::MakeLabeledAddressClassifier(std::move(addressClassifier), std::move(labels));
}

} // namespace NKikimr::NAddressClassifier
