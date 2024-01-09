#include "ipmath.h"

namespace {
    constexpr auto IPV4_BITS = 32;
    constexpr auto IPV6_BITS = 128;

    const ui128 MAX_IPV4_ADDR = Max<ui32>();
    const ui128 MAX_IPV6_ADDR = Max<ui128>();

    TStringBuf TypeToString(TIpv6Address::TIpType type) {
        switch (type) {
            case TIpv6Address::Ipv4:
                return TStringBuf("IPv4");
            case TIpv6Address::Ipv6:
                return TStringBuf("IPv6");
            default:
                return TStringBuf("UNKNOWN");
        }
    }

    size_t MaxPrefixLenForType(TIpv6Address::TIpType type) {
        switch (type) {
            case TIpv6Address::Ipv4:
                return IPV4_BITS;
            case TIpv6Address::Ipv6:
                return IPV6_BITS;
            case TIpv6Address::LAST:
                ythrow yexception() << "invalid type";
        }
    }

    template <ui8 ADDR_LEN>
    ui128 LowerBoundForPrefix(ui128 value, ui8 prefixLen) {
        const int shift = ADDR_LEN - prefixLen;
        const ui128 shifted = (shift < 128) ? (ui128{1} << shift) : 0;
        ui128 mask = ~(shifted - 1);
        return value & mask;
    }

    template <ui8 ADDR_LEN>
    ui128 UpperBoundForPrefix(ui128 value, ui8 prefixLen) {
        const int shift = ADDR_LEN - prefixLen;
        const ui128 shifted = (shift < 128) ? (ui128{1} << shift) : 0;
        ui128 mask = shifted - 1;
        return value | mask;
    }

    auto LowerBoundForPrefix4 = LowerBoundForPrefix<IPV4_BITS>;
    auto LowerBoundForPrefix6 = LowerBoundForPrefix<IPV6_BITS>;
    auto UpperBoundForPrefix4 = UpperBoundForPrefix<IPV4_BITS>;
    auto UpperBoundForPrefix6 = UpperBoundForPrefix<IPV6_BITS>;

    TIpv6Address IpFromStringSafe(const TStringBuf s) {
        bool ok{};
        auto addr = TIpv6Address::FromString(s, ok);
        Y_ENSURE(ok, "Failed to parse an IP address from " << s);
        return addr;
    }

    /// it's different from TIpv6Address::IsValid for 0.0.0.0
    bool IsValid(TIpv6Address addr) {
        switch (addr.Type()) {
            case TIpv6Address::Ipv4:
            case TIpv6Address::Ipv6:
                return true;

            case TIpv6Address::LAST:
                return false;
        }
    }

    bool HasNext(TIpv6Address addr) {
        switch (addr.Type()) {
            case TIpv6Address::Ipv4:
                return ui128(addr) != MAX_IPV4_ADDR;
            case TIpv6Address::Ipv6:
                return ui128(addr) != MAX_IPV6_ADDR;
            case TIpv6Address::LAST:
                return false;
        }
    }

    TIpv6Address Next(TIpv6Address addr) {
        return {ui128(addr) + 1, addr.Type()};
    }
} // namespace

TIpv6Address LowerBoundForPrefix(TIpv6Address value, ui8 prefixLen) {
    auto type = value.Type();
    switch (type) {
        case TIpv6Address::Ipv4:
            return {LowerBoundForPrefix4(value, prefixLen), type};
        case TIpv6Address::Ipv6:
            return {LowerBoundForPrefix6(value, prefixLen), type};
        default:
            ythrow yexception() << "invalid type";
    }
}

TIpv6Address UpperBoundForPrefix(TIpv6Address value, ui8 prefixLen) {
    auto type = value.Type();
    switch (type) {
        case TIpv6Address::Ipv4:
            return {UpperBoundForPrefix4(value, prefixLen), type};
        case TIpv6Address::Ipv6:
            return {UpperBoundForPrefix6(value, prefixLen), type};
        default:
            ythrow yexception() << "invalid type";
    }
}

TIpAddressRange::TIpAddressRangeBuilder::operator TIpAddressRange() {
    return Build();
}

TIpAddressRange TIpAddressRange::TIpAddressRangeBuilder::Build() {
    return TIpAddressRange{Start_, End_};
}

TIpAddressRange::TIpAddressRangeBuilder::TIpAddressRangeBuilder(const TStringBuf from)
    : TIpAddressRangeBuilder{IpFromStringSafe(from)}
{
}

TIpAddressRange::TIpAddressRangeBuilder::TIpAddressRangeBuilder(TIpv6Address from) {
    Y_ENSURE_EX(IsValid(from), TInvalidIpRangeException() << "Address " << from.ToString() << " is invalid");
    Start_ = from;
    End_ = Start_;
}

TIpAddressRange::TIpAddressRangeBuilder& TIpAddressRange::TIpAddressRangeBuilder::To(const TStringBuf to) {
    End_ = IpFromStringSafe(to);
    return *this;
}

TIpAddressRange::TIpAddressRangeBuilder& TIpAddressRange::TIpAddressRangeBuilder::To(TIpv6Address to) {
    Y_ENSURE_EX(IsValid(to), TInvalidIpRangeException() << "Address " << to.ToString() << " is invalid");
    End_ = to;
    return *this;
}

TIpAddressRange::TIpAddressRangeBuilder& TIpAddressRange::TIpAddressRangeBuilder::WithPrefixImpl(ui8 len, bool checkLowerBound) {
    Y_ENSURE_EX(IsValid(Start_), TInvalidIpRangeException() << "Start value must be set before prefix");
    const auto type = Start_.Type();
    const auto maxLen = MaxPrefixLenForType(type);
    Y_ENSURE_EX(len <= maxLen, TInvalidIpRangeException() << "Maximum prefix length for this address type is "
        << maxLen << ", but requested " << (ui32)len);

    const auto lowerBound = LowerBoundForPrefix(Start_, len);
    if (checkLowerBound) {
        Y_ENSURE_EX(Start_ == lowerBound, TInvalidIpRangeException() << "Cannot create IP range from start address "
            << Start_ << " with prefix length " << (ui32)len);
    }

    Start_ = lowerBound;
    End_ = UpperBoundForPrefix(Start_, len);

    return *this;
}

TIpAddressRange::TIpAddressRangeBuilder& TIpAddressRange::TIpAddressRangeBuilder::WithPrefix(ui8 len) {
    return WithPrefixImpl(len, true);
}

TIpAddressRange::TIpAddressRangeBuilder& TIpAddressRange::TIpAddressRangeBuilder::WithMaskedPrefix(ui8 len) {
    return WithPrefixImpl(len, false);
}

void TIpAddressRange::Init(TIpv6Address from, TIpv6Address to) {
    Start_ = from;
    End_ = to;

    Y_ENSURE_EX(Start_ <= End_, TInvalidIpRangeException() << "Invalid IP address range: from " << Start_ << " to " << End_);
    Y_ENSURE_EX(Start_.Type() == End_.Type(), TInvalidIpRangeException()
        << "Address type mismtach: start address type is " << TypeToString(Start_.Type())
        << " end type is " << TypeToString(End_.Type()));
}

TIpAddressRange::TIpAddressRange(TIpv6Address start, TIpv6Address end) {
    Y_ENSURE_EX(IsValid(start), TInvalidIpRangeException() << "start address " << start.ToString() << " is invalid");
    Y_ENSURE_EX(IsValid(end), TInvalidIpRangeException() << "end address " << end.ToString() << " is invalid");
    Init(start, end);
}

TIpAddressRange::TIpAddressRange(const TStringBuf start, const TStringBuf end) {
    auto startAddr = IpFromStringSafe(start);
    auto endAddr = IpFromStringSafe(end);
    Init(startAddr, endAddr);
}

TIpAddressRange::~TIpAddressRange() {
}

TIpAddressRange::TIpType TIpAddressRange::Type() const {
    return Start_.Type();
}

ui128 TIpAddressRange::Size() const {
    return ui128(End_) - ui128(Start_) + 1;
}

bool TIpAddressRange::IsSingle() const {
    return Start_ == End_;
}

bool TIpAddressRange::Contains(const TIpAddressRange& other) const {
    return Start_ <= other.Start_ && End_ >= other.End_;
}

bool TIpAddressRange::Contains(const TIpv6Address& addr) const {
    return Start_ <= addr && End_ >= addr;
}

bool TIpAddressRange::Overlaps(const TIpAddressRange& other) const {
    return Start_ <= other.End_ && other.Start_ <= End_;
}

bool TIpAddressRange::IsConsecutive(const TIpAddressRange& other) const {
    return (HasNext(End_) && Next(End_) == other.Start_)
        || (HasNext(other.End_) && Next(other.End_) == Start_);
}

TIpAddressRange TIpAddressRange::Union(const TIpAddressRange& other) const {
    Y_ENSURE(IsConsecutive(other) || Overlaps(other), "Can merge only consecutive or overlapping ranges");
    Y_ENSURE(other.Start_.Type() == Start_.Type(), "Cannot merge ranges of addresses of different types");

    auto s = Start_;
    auto e = End_;

    s = {Min<ui128>(Start_, other.Start_), Start_.Type()};
    e = {Max<ui128>(End_, other.End_), End_.Type()};

    return {s, e};
}

TIpAddressRange TIpAddressRange::FromCidrString(const TStringBuf str) {
    if (auto result = TryFromCidrString(str)) {
        return *result;
    }

    ythrow TInvalidIpRangeException() << "Cannot parse " << str << " as a CIDR string";
}

TMaybe<TIpAddressRange> TIpAddressRange::TryFromCidrStringImpl(const TStringBuf str, bool compact) {
    auto idx = str.rfind('/');
    if (idx == TStringBuf::npos) {
        return Nothing();
    }

    TStringBuf sb{str};
    TStringBuf address, prefix;
    sb.SplitAt(idx, address, prefix);
    prefix.Skip(1);

    ui8 prefixLen{};
    if (!::TryFromString(prefix, prefixLen)) {
        return Nothing();
    }

    return compact ?
        TIpAddressRange::From(address).WithMaskedPrefix(prefixLen) :
        TIpAddressRange::From(address).WithPrefix(prefixLen);
}

TMaybe<TIpAddressRange> TIpAddressRange::TryFromCidrString(const TStringBuf str) {
    return TryFromCidrStringImpl(str, false);
}

TIpAddressRange TIpAddressRange::FromCompactString(const TStringBuf str) {
    if (auto result = TryFromCompactString(str)) {
        return *result;
    }

    ythrow TInvalidIpRangeException() << "Cannot parse " << str << " as a CIDR string";
}

TMaybe<TIpAddressRange> TIpAddressRange::TryFromCompactString(const TStringBuf str) {
    return TryFromCidrStringImpl(str, true);
}

TIpAddressRange TIpAddressRange::FromRangeString(const TStringBuf str) {
    if (auto result = TryFromRangeString(str)) {
        return *result;
    }

    ythrow TInvalidIpRangeException() << "Cannot parse " << str << " as a range string";
}

TMaybe<TIpAddressRange> TIpAddressRange::TryFromRangeString(const TStringBuf str) {
    auto idx = str.find('-');
    if (idx == TStringBuf::npos) {
        return Nothing();
    }

    TStringBuf sb{str};
    TStringBuf start, end;
    sb.SplitAt(idx, start, end);
    end.Skip(1);

    return TIpAddressRange::From(start).To(end);
}

TIpAddressRange TIpAddressRange::FromString(const TStringBuf str) {
    if (auto result = TryFromString(str)) {
        return *result;
    }

    ythrow TInvalidIpRangeException() << "Cannot parse an IP address from " << str;
}

TMaybe<TIpAddressRange> TIpAddressRange::TryFromString(const TStringBuf str) {
    if (auto idx = str.find('/'); idx != TStringBuf::npos) {
        return TryFromCidrString(str);
    } else if (idx = str.find('-'); idx != TStringBuf::npos) {
        return TryFromRangeString(str);
    } else {
        bool ok{};
        auto addr = TIpv6Address::FromString(str, ok);
        if (!ok) {
            return Nothing();
        }

        return TIpAddressRange::From(addr);
    }
}

TString TIpAddressRange::ToRangeString() const {
    bool ok{};
    return TStringBuilder() << Start_.ToString(ok) << "-" << End_.ToString(ok);
}

TIpAddressRange::TIterator TIpAddressRange::begin() const {
    return Begin();
}

TIpAddressRange::TIterator TIpAddressRange::Begin() const {
    return TIpAddressRange::TIterator{Start_};
}

TIpAddressRange::TIterator TIpAddressRange::end() const {
    return End();
}

TIpAddressRange::TIterator TIpAddressRange::End() const {
    return TIpAddressRange::TIterator{{ui128(End_) + 1, End_.Type()}};
}

TIpAddressRange::TIpAddressRangeBuilder TIpAddressRange::From(TIpv6Address from) {
    return TIpAddressRangeBuilder{from};
}

TIpAddressRange::TIpAddressRangeBuilder TIpAddressRange::From(const TStringBuf from) {
    return TIpAddressRangeBuilder{from};
}

bool operator==(const TIpAddressRange& lhs, const TIpAddressRange& rhs) {
    return lhs.Start_ == rhs.Start_ && lhs.End_ == rhs.End_;
}

bool operator!=(const TIpAddressRange& lhs, const TIpAddressRange& rhs) {
    return !(lhs == rhs);
}

TIpAddressRange::TIterator::TIterator(TIpv6Address val) noexcept
    : Current_{val}
{
}

bool TIpAddressRange::TIterator::operator==(const TIpAddressRange::TIterator& other) noexcept {
    return Current_ == other.Current_;
}

bool TIpAddressRange::TIterator::operator!=(const TIpAddressRange::TIterator& other) noexcept {
    return !(*this == other);
}

TIpAddressRange::TIterator& TIpAddressRange::TIterator::operator++() noexcept {
    ui128 numeric = Current_;
    Current_ = {numeric + 1, Current_.Type()};
    return *this;
}

const TIpv6Address& TIpAddressRange::TIterator::operator*() noexcept {
    return Current_;
}
