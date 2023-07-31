#include "range.h"

#include "util_helpers.h"

#include <library/cpp/int128/int128.h>
#include <util/generic/maybe.h>
#include <util/string/split.h>
#include <util/string/vector.h>

#include <stdexcept>

namespace NIPREG {

namespace {
    EAddressFormat CurrentFormat = EAddressFormat::SHORT_IPV6;

    void throwExceptionWithFormat(const TString& line) {
        throw yexception() << "wanted format: ${ip-begin}-${ip-end}[\t${data}]; $input := '" << line << "'";
    }

    void throwIfReverseOrder(TAddress first, TAddress last) {
        if (first > last) {
            const TString err_msg = "reverse order of addresses (first / last) => " + first.AsIPv6() + " / " + last.AsIPv6();
            throw std::runtime_error(err_msg.data());
        }
    }
} // anon-ns

TRange::TRange(TAddress first, TAddress last, const TString& data)
    : First(first)
    , Last(last)
    , Data(data)
{
    throwIfReverseOrder(First, Last);
}

TRange::TRange(const TNetwork& net, const TString& data)
    : TRange(net.begin, net.end, data)
{
}

ui128 TRange::GetAddrsQty() const {
    return TAddress::Distance(First, Last) + 1;
}

TRange TRange::BuildRange(const TString& line, bool isEmptyData, const TString& dataDelim) {
    const TVector<TString> parts = StringSplitter(line).SplitBySet(dataDelim.data()).SkipEmpty();
    if (parts.empty()) {
        throwExceptionWithFormat(line);
    }

    if (TString::npos != parts[0].find('/')) {
        const auto data = (2 == parts.size()) ? parts[1] : "";
        return TRange(TNetwork(parts[0]), data);
    }

    const TVector<TString> range_parts = StringSplitter(parts[0]).SplitBySet(" -\t").SkipEmpty();
    if (2 != range_parts.size() || range_parts[0].empty() || range_parts[1].empty()) {
        throwExceptionWithFormat(line);
    }

    if (!isEmptyData && (2 != parts.size() || parts[1].empty())) {
        throwExceptionWithFormat(line);
    }

    const auto& data = (2 == parts.size()) ? parts[1] : "";
    return TRange(TAddress::ParseAny(range_parts[0]), TAddress::ParseAny(range_parts[1]), data);
}

bool TRange::Contains(const TRange& range) const {
    return First <= range.First && range.Last <= Last;
}

bool TRange::Contains(const TAddress& ip) const {
    return First <= ip && ip <= Last;
}

void SetIpFullOutFormat() {
    CurrentFormat = EAddressFormat::IPV6;
}

void SetIpShortOutFormat() {
    CurrentFormat = EAddressFormat::SHORT_IPV6;
}

void TRange::DumpTo(IOutputStream& output, bool withData, EAddressFormat format) const {
    output << First.Format(format) << '-' << Last.Format(format);
    if (withData) {
        output << '\t' << Data;
    }
}

bool TRange::IsIpv6Only() const {
    return 6 == First.GetType() && 6 == Last.GetType();
}

bool TRange::IsIpv4Only() const {
    return 4 == First.GetType() && 4 == Last.GetType();
}

bool TRange::IsRangeInSingleNet64() const {
    return First.GetHigh64() == Last.GetHigh64();
}

TRange TRange::BuildRangeByFirst(const TRange& range, int prefix) {
    Y_UNUSED(prefix);
    return TRange(TAddress::MakeNet64Prefix(range.First),
                  TAddress::MakeNet64Broadcast(range.IsRangeInSingleNet64() ? range.Last : range.Last.GetPrevNet64())   ,
                  range.Data
           );
}

TRange TRange::BuildRangeByLast(const TRange& range, int prefix) {
    Y_UNUSED(prefix);
    const auto prevLast = TAddress::MakeNet64Broadcast(range.Last.GetPrevNet64());
    return TRange(range.First, prevLast, range.Data);
//    const auto prevLast = TAddress::MakeNet64Broadcast(range.Last);
//    return TRange(TAddress::MakeNet64Prefix(range.First), prevLast, range.Data);
}

TVector<TRange> SplitRangeNets(const TRange& origRange, bool addOrigSize, int maskLen) {
    Y_UNUSED(maskLen);

    static const auto firstCheckedIpv6Prefix = TAddress::ParseAny("2000::");

    const auto& CalcNetSize = [&](const TRange& range) {
        static const auto MAX_FOR_DIGITS_ANSWER = ui128{1 << 30};
        const auto netSize = range.GetAddrsQty();
        return (netSize < MAX_FOR_DIGITS_ANSWER) ? ToString(netSize) : "huge";
    };

    const auto& AddSizeField = [&](TRange& changedRange, const TRange& origAddrRange) {
        if (addOrigSize) {
            changedRange.Data = AddJsonAttrs({"orig_net_size"}, changedRange.Data, TMaybe<TString>(CalcNetSize(origAddrRange)));
        }
    };

    if (origRange.Last <= firstCheckedIpv6Prefix) {
        return {origRange};
    }

    if (origRange.IsRangeInSingleNet64()) {
        TRange theOne{
            TAddress::MakeNet64Prefix(origRange.First),
            TAddress::MakeNet64Broadcast(origRange.Last),
            origRange.Data
        };
        AddSizeField(theOne, origRange);
        return {theOne};
    }

    TRange range{origRange};
    TVector<TRange> result; {
        // 1st
        TRange byFirst{TAddress::MakeNet64Prefix(range.First),TAddress::MakeNet64Broadcast(range.First), range.Data};
        AddSizeField(byFirst, {range.First, byFirst.Last, ""});
        result.push_back(byFirst);

        // maybe 2nd
        range.First = byFirst.Last.Next();
        if (!range.IsRangeInSingleNet64()) {
            const TAddress lastPrefix = TAddress::MakeNet64Prefix(range.Last);

            TRange inTheMiddle{TAddress::MakeNet64Prefix(range.First), lastPrefix.Prev(), range.Data};
            AddSizeField(inTheMiddle, inTheMiddle);
            result.push_back(inTheMiddle);

            range.First = lastPrefix;
        }

        // the last
        TRange byLast{range.First, TAddress::MakeNet64Broadcast(range.Last), range.Data};
        AddSizeField(byLast, {byLast.First, range.Last, ""});
        result.push_back(byLast);
    }
    return result;
}

bool operator==(const TRange& lhs, const TRange& rhs) {
    return lhs.First == rhs.First && lhs.Last == rhs.Last;
}

}  // ns IPREG

IInputStream& operator>>(IInputStream& input, NIPREG::TRange& range) {
    TString line;
    if (!input.ReadLine(line)) {
        throw std::runtime_error("unable to load data from stream");
    }
    range = NIPREG::TRange::BuildRange(line);
    return input;
}

IOutputStream& operator<<(IOutputStream& output, const NIPREG::TRange& range) {
    range.DumpTo(output, true, NIPREG::CurrentFormat);
    output << "\n";
    return output;
}
