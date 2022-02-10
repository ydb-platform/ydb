#pragma once

#include <library/cpp/ipv6_address/ipv6_address.h>

#include <util/generic/maybe.h>
#include <util/ysaveload.h>

struct TInvalidIpRangeException: public virtual yexception {
};

class TIpAddressRange {
    friend bool operator==(const TIpAddressRange& lhs, const TIpAddressRange& rhs);
    friend bool operator!=(const TIpAddressRange& lhs, const TIpAddressRange& rhs);

    class TIpAddressRangeBuilder;
public:
    class TIterator;
    using TIpType = TIpv6Address::TIpType;

    TIpAddressRange() = default;
    TIpAddressRange(TIpv6Address start, TIpv6Address end);
    TIpAddressRange(const TString& start, const TString& end);
    ~TIpAddressRange();

    static TIpAddressRangeBuilder From(TIpv6Address from);
    static TIpAddressRangeBuilder From(const TString& from);

    /**
     * Parses a string tormatted in Classless Iter-Domain Routing (CIDR) notation.
     * @param str a CIDR-formatted string, e.g. "192.168.0.0/16"
     * @return a new TIpAddressRange
     * @throws TInvalidIpRangeException if the string cannot be parsed.
     */
    static TIpAddressRange FromCidrString(const TString& str);
    static TMaybe<TIpAddressRange> TryFromCidrString(const TString& str);

    /**
     * Parses a string formatted as two dash-separated addresses.
     * @param str a CIDR-formatted string, e.g. "192.168.0.0-192.168.0.2"
     * @return a new TIpAddressRange
     * @throws TInvalidIpRangeException if the string cannot be parsed.
     */
    static TIpAddressRange FromRangeString(const TString& str);
    static TMaybe<TIpAddressRange> TryFromRangeString(const TString& str);

    TString ToRangeString() const;

    /**
     * Tries to guess the format and parse it. Format must be one of: CIDR ("10.0.0.0/24"), range ("10.0.0.0-10.0.0.10") or a single address.
     * @return a new TIpAddressRange
     * @throws TInvlidIpRangeException if the string doesn't match any known format or if parsing failed.
     */
    static TIpAddressRange FromString(const TString& str);
    static TMaybe<TIpAddressRange> TryFromString(const TString& str);

    TIpType Type() const;

    // XXX: uint128 cannot hold size of the complete range of IPv6 addresses. Use IsComplete to determine whether this is the case.
    ui128 Size() const;

    /**
     * Determines whether this range contains only one address.
     * @return true if contains only one address, otherwise false.
     */
    bool IsSingle() const;
    bool IsComplete() const;

    bool Contains(const TIpAddressRange& other) const;
    bool Contains(const TIpv6Address& addr) const;

    bool Overlaps(const TIpAddressRange& other) const;

    /**
     * Determines whether two ranges follow one after another without any gaps.
     * @return true if either this range follows the given one or vice versa, otherwise false.
     */
    bool IsConsecutive(const TIpAddressRange& other) const;

    /**
     * Concatenates another range into this one.
     * Note, that ranges must be either consecutive or overlapping.
     * @throws yexception if ranges are neither consecutive nor overlapping.
     */
    TIpAddressRange Union(const TIpAddressRange& other) const;

    template <typename TFunction>
    void ForEach(TFunction func);

    // for-each compliance interface
    TIterator begin() const;
    TIterator end() const;

    // Arcadia style-guide friendly
    TIterator Begin() const;
    TIterator End() const;

    Y_SAVELOAD_DEFINE(Start_, End_);

private:
    void Init(TIpv6Address, TIpv6Address);

    TIpv6Address Start_;
    TIpv6Address End_;
};

bool operator==(const TIpAddressRange& lhs, const TIpAddressRange& rhs);
bool operator!=(const TIpAddressRange& lhs, const TIpAddressRange& rhs);

TIpv6Address LowerBoundForPrefix(TIpv6Address value, ui8 prefixLen);
TIpv6Address UpperBoundForPrefix(TIpv6Address value, ui8 prefixLen);


class TIpAddressRange::TIpAddressRangeBuilder {
    friend class TIpAddressRange;
    TIpAddressRangeBuilder() = default;
    TIpAddressRangeBuilder(TIpv6Address from);
    TIpAddressRangeBuilder(const TString& from);
    TIpAddressRangeBuilder(const TIpAddressRangeBuilder&) = default;
    TIpAddressRangeBuilder& operator=(const TIpAddressRangeBuilder&) = default;

    TIpAddressRangeBuilder(TIpAddressRangeBuilder&&) = default;
    TIpAddressRangeBuilder& operator=(TIpAddressRangeBuilder&&) = default;

public:
    operator TIpAddressRange();
    TIpAddressRange Build();

    TIpAddressRangeBuilder& To(const TString&);
    TIpAddressRangeBuilder& To(TIpv6Address);

    TIpAddressRangeBuilder& WithPrefix(ui8 len);

private:
    TIpv6Address Start_;
    TIpv6Address End_;
};


class TIpAddressRange::TIterator {
public:
    TIterator(TIpv6Address val) noexcept;

    bool operator==(const TIpAddressRange::TIterator& other) noexcept;
    bool operator!=(const TIpAddressRange::TIterator& other) noexcept;

    TIterator& operator++() noexcept;
    const TIpv6Address& operator*() noexcept;

private:
    TIpv6Address Current_;
};


template <typename TFunction>
void TIpAddressRange::ForEach(TFunction func) {
    static_assert(std::is_invocable<TFunction, TIpv6Address>::value, "function must take single address argument");
    for (auto addr : *this) {
        func(addr);
    }
}
