#include "address_classifier.h"

namespace NKikimr::NAddressClassifier {

TString ExtractAddress(const TString& peer) {
    TStringBuf buf(peer);
    if (buf.SkipPrefix(TStringBuf("ipv"))) {
        buf.Skip(2); // skip 4/6 and ':'
        if (buf.StartsWith('[')) {
            buf.Skip(1);
            buf = buf.Before(']');
            return TString(buf);
        }
    }

    buf = buf.RBefore(':'); // remove port

    return TString(buf);
}

TString ParseAddress(const TString& address, TString& hostname, ui32& port) {
    size_t first_colon_pos = address.find(':');
    if (first_colon_pos != TString::npos) {
        size_t last_colon_pos = address.rfind(':');
        if (last_colon_pos == first_colon_pos) {
            // only one colon, simple case
            port = FromString<ui32>(address.substr(first_colon_pos + 1));
            hostname = address.substr(0, first_colon_pos);
        } else {
            // ipv6?
            size_t closing_bracket_pos = address.rfind(']');
            if (closing_bracket_pos == TString::npos || closing_bracket_pos > last_colon_pos) {
                // whole address is ipv6 host
                hostname = address;
            } else {
                port = FromString<ui32>(address.substr(last_colon_pos + 1));
                hostname = address.substr(0, last_colon_pos);
            }
            if (hostname.StartsWith('[') && hostname.EndsWith(']')) {
                hostname = hostname.substr(1, hostname.size() - 2);
            }
        }
    } else {
        hostname = address;
    }
    return hostname;
}


bool TAddressClassifier::AddNetByCidrAndLabel(const TString& cidr, const size_t label) {
    try {
        // The following method still throws despite its name
        const auto maybeRange = TIpAddressRange::TryFromCidrString(cidr);
        if (maybeRange.Empty()) {
            return false;
        }

        if (maybeRange->Type() == TIpAddressRange::TIpType::Ipv4) {
            RegisteredV4Nets[label].Add(*maybeRange);
        } else if (maybeRange->Type() == TIpAddressRange::TIpType::Ipv6) {
            RegisteredV6Nets[label].Add(*maybeRange);
        } else {
            Y_ABORT_UNLESS(false); // unknown net type?
        }
    } catch (yexception&) {
        return false;
    }

    return true;
}

std::pair<bool, size_t> TAddressClassifier::ClassifyAddress(const TString& address) const {
    bool ok;
    const auto inetAddress = TIpv6Address::FromString(address, ok);
    if (!ok) {
        return UnknownAddressClass;
    }

    auto findAddress = [&inetAddress](const auto& registeredNets) -> std::pair<bool, size_t> {
        for (const auto& [label, rangeSet] : registeredNets) {
            if (rangeSet.Contains(inetAddress)) {
                return {true, label};
            }
        }
        return UnknownAddressClass;
    };

    if (inetAddress.Type() == TIpv6Address::TIpType::Ipv4) {
        return findAddress(RegisteredV4Nets);
    } else if (inetAddress.Type() == TIpv6Address::TIpType::Ipv6) {
        return findAddress(RegisteredV6Nets);
    } else {
        Y_ABORT_UNLESS(false); // unknown net type?
    }

    return UnknownAddressClass;
}

TLabeledAddressClassifier::TLabeledAddressClassifier(TAddressClassifier&& addressClassifier, std::vector<TString>&& labels)
    : Classifier(std::move(addressClassifier))
    , Labels(std::move(labels))
    {
    }

TLabeledAddressClassifier::TConstPtr TLabeledAddressClassifier::MakeLabeledAddressClassifier(TAddressClassifier&& addressClassifier, std::vector<TString>&& labels) {
    return MakeIntrusiveConst<TLabeledAddressClassifier>(std::move(addressClassifier), std::move(labels));
}

TMaybe<TString> TLabeledAddressClassifier::ClassifyAddress(const TString& address) const {
    TMaybe<TString> result = Nothing();

    const auto netClass = Classifier.ClassifyAddress(address);
    if (netClass != TAddressClassifier::UnknownAddressClass) {
        Y_ABORT_UNLESS(netClass.second < Labels.size());
        result = Labels[netClass.second];
    }

    return result;
}

const std::vector<TString>& TLabeledAddressClassifier::GetLabels() const {
    return Labels;
}

} // namespace NKikimr::NAddressClassifier
