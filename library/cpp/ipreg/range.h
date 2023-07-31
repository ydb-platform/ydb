#pragma once

#include "address.h"

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/input.h>
#include <util/stream/output.h>

#include <stdexcept>

namespace NIPREG {

struct TRange {
    TAddress First;
    TAddress Last;
    TString Data;

    TRange() = default;
    TRange(TAddress first, TAddress last, const TString& data);
    TRange(const TNetwork& net, const TString& data);

    ui128 GetAddrsQty() const;
    void DumpTo(IOutputStream& output, bool withData = true, EAddressFormat format = EAddressFormat::SHORT_IP) const;

    static TRange BuildRange(const TString& line, bool isEmptyData = false, const TString& dataDelim = "\t");
    bool Contains(const TRange& range) const;
    bool Contains(const TAddress& ip) const;

    static TRange BuildRangeByFirst(const TRange& range, int prefix = 64);
    static TRange BuildRangeByLast(const TRange& range, int prefix = 64);

    bool IsIpv6Only() const;
    bool IsIpv4Only() const;

    bool IsRangeInSingleNet64() const;
};
using TGenericEntry = TRange;

void SetIpFullOutFormat();
void SetIpShortOutFormat();

TVector<TRange> SplitRangeNets(const TRange& range, bool addOrigSize = false, int maskLen = 64);

bool operator==(const TRange& lhs, const TRange& rhs);
inline bool operator!=(const TRange& lhs, const TRange& rhs) { return !(lhs == rhs); }
}  // ns NIPREG

IInputStream&  operator>>(IInputStream& input, NIPREG::TRange& range);
IOutputStream& operator<<(IOutputStream& output, const NIPREG::TRange& range);
