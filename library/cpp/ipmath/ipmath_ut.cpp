#include "ipmath.h"
#include "range_set.h"

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/gmock_in_unittest/gmock.h>

#include <library/cpp/ipv6_address/ipv6_address.h>

using namespace testing;

static constexpr auto MIN_IPV6_ADDR = "::";
static constexpr auto MAX_IPV6_ADDR = "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff";

std::ostream& operator<<(std::ostream& os, const TIpAddressRange& r) {
    auto s = r.ToRangeString();
    os.write(s.data(), s.size());
    return os;
}

std::ostream& operator<<(std::ostream& os, const TIpRangeSet& rangeSet) {
    os << "{\n";

    for (auto&& r : rangeSet) {
        os << r << '\n';
    }

    os << "}\n";
    return os;
}

class TIpRangeTests: public TTestBase {
public:
    UNIT_TEST_SUITE(TIpRangeTests);
    UNIT_TEST(IpRangeFromIpv4);
    UNIT_TEST(IpRangeFromIpv6);
    UNIT_TEST(FullIpRange);
    UNIT_TEST(IpRangeFromCidr);
    UNIT_TEST(IpRangeFromCompact);
    UNIT_TEST(IpRangeFromIpv4Builder);
    UNIT_TEST(IpRangeFromInvalidIpv4);
    UNIT_TEST(IpRangeFromInvalidIpv6);
    UNIT_TEST(RangeFromSingleAddress);
    UNIT_TEST(RangeFromRangeString);
    UNIT_TEST(ManualIteration);
    UNIT_TEST(RangeRelations);
    UNIT_TEST(RangeUnion);
    UNIT_TEST_SUITE_END();

    void RangeFromSingleAddress() {
        for (auto addrStr : {"192.168.0.1", "::2"}) {
            auto range = TIpAddressRange::From(addrStr).Build();
            ASSERT_THAT(range.Size(), Eq(1));
            ASSERT_TRUE(range.IsSingle());

            auto range2 = TIpAddressRange{addrStr, addrStr};
            ASSERT_THAT(range2, Eq(range));

            TVector<ui128> result;
            range.ForEach([&result] (TIpv6Address addr) {
                result.push_back(addr);
            });

            bool ok{};
            ASSERT_THAT(result, ElementsAre(TIpv6Address::FromString(addrStr, ok)));
        }

    }

    void IpRangeFromIpv4() {
        bool ok{};

        auto s = TIpv6Address::FromString("192.168.0.0", ok);
        ASSERT_TRUE(ok);
        auto e = TIpv6Address::FromString("192.168.0.255", ok);
        ASSERT_TRUE(ok);

        TIpAddressRange range{s, e};

        ASSERT_THAT(range.Size(), Eq(256));
        ASSERT_THAT(range.Type(), Eq(TIpAddressRange::TIpType::Ipv4));

        TIpAddressRange range2{"192.168.0.0", "192.168.0.255"};
        ASSERT_THAT(range2.Size(), Eq(256));
        ASSERT_THAT(range2, Eq(range));
    }

    void IpRangeFromIpv6() {
        bool ok{};

        auto s = TIpv6Address::FromString("ffce:abcd::", ok);
        ASSERT_TRUE(ok);
        auto e = TIpv6Address::FromString("ffce:abcd::00ff", ok);
        ASSERT_TRUE(ok);

        TIpAddressRange range{s, e};

        ASSERT_THAT(range.Size(), Eq(256));

        TIpAddressRange range2{"ffce:abcd::", "ffce:abcd::00ff"};
        ASSERT_THAT(range2.Size(), Eq(256));
        ASSERT_THAT(range.Type(), Eq(TIpAddressRange::TIpType::Ipv6));
        ASSERT_THAT(range2, Eq(range));
    }


    void FullIpRange() {
        auto check = [] (auto start, auto end, ui128 expectedSize) {
            auto range = TIpAddressRange::From(start).To(end).Build();
            ASSERT_THAT(range.Size(), Eq(expectedSize));
        };

        check("0.0.0.0", "255.255.255.255", ui128(Max<ui32>()) + 1);
        // XXX
        // check(MIN_IPV6_ADDR, MAX_IPV6_ADDR, ui128(Max<ui128>() + 1));
    }

    void IpRangeFromCidr() {
        auto range = TIpAddressRange::FromCidrString("10.0.0.0/30");

        ASSERT_THAT(range.Size(), Eq(4));
        TVector<TIpv6Address> result;
        Copy(range.begin(), range.end(), std::back_inserter(result));

        bool ok;
        TVector<TIpv6Address> expected {
            TIpv6Address::FromString("10.0.0.0", ok),
            TIpv6Address::FromString("10.0.0.1", ok),
            TIpv6Address::FromString("10.0.0.2", ok),
            TIpv6Address::FromString("10.0.0.3", ok),
        };

        ASSERT_THAT(result, ElementsAreArray(expected));

        // single host
        ASSERT_THAT(TIpAddressRange::FromCidrString("ffce:abcd::/128"), Eq(TIpAddressRange::From("ffce:abcd::").Build()));
        ASSERT_THAT(TIpAddressRange::FromCidrString("192.168.0.1/32"), Eq(TIpAddressRange::From("192.168.0.1").Build()));

        // full range
        ASSERT_THAT(TIpAddressRange::FromCidrString("::/0"), Eq(TIpAddressRange::From(MIN_IPV6_ADDR).To(MAX_IPV6_ADDR).Build()));
        ASSERT_THAT(TIpAddressRange::FromCidrString("0.0.0.0/0"), Eq(TIpAddressRange::From("0.0.0.0").To("255.255.255.255").Build()));

        // illformed
        ASSERT_THROW(TIpAddressRange::FromCidrString("::/"), TInvalidIpRangeException);
        ASSERT_THROW(TIpAddressRange::FromCidrString("::"), TInvalidIpRangeException);
        ASSERT_THROW(TIpAddressRange::FromCidrString("/::"), TInvalidIpRangeException);
        ASSERT_THROW(TIpAddressRange::FromCidrString("::/150"), TInvalidIpRangeException);
    }

    void IpRangeFromCompact() {
        // FromCidrString disallows node addresses
        EXPECT_THROW(TIpAddressRange::FromCidrString("10.10.36.12/12"), TInvalidIpRangeException);

        // FromCompactString allows to use node address instead of network address (suffix of zeroes is not required)
        ASSERT_THAT(TIpAddressRange::FromCompactString("10.10.36.12/12"), Eq(TIpAddressRange::FromCidrString("10.0.0.0/12")));
        ASSERT_THAT(TIpAddressRange::FromCompactString("abcd:ef01:2345::/24"), Eq(TIpAddressRange::FromCidrString("abcd:ef00::/24")));
    }

    void RangeFromRangeString() {
        {
            auto range = TIpAddressRange::FromRangeString("10.0.0.0-10.0.0.3");

            TVector<TIpv6Address> result;
            Copy(range.begin(), range.end(), std::back_inserter(result));

            bool ok;
            TVector<TIpv6Address> expected {
                TIpv6Address::FromString("10.0.0.0", ok),
                TIpv6Address::FromString("10.0.0.1", ok),
                TIpv6Address::FromString("10.0.0.2", ok),
                TIpv6Address::FromString("10.0.0.3", ok),
            };

            ASSERT_THAT(result, ElementsAreArray(expected));
        }
        {
            auto range = TIpAddressRange::FromRangeString("10.0.0.0-10.0.0.3");

            TVector<TIpv6Address> result;
            Copy(range.begin(), range.end(), std::back_inserter(result));

            bool ok;
            TVector<TIpv6Address> expected {
                TIpv6Address::FromString("10.0.0.0", ok),
                TIpv6Address::FromString("10.0.0.1", ok),
                TIpv6Address::FromString("10.0.0.2", ok),
                TIpv6Address::FromString("10.0.0.3", ok),
            };

            ASSERT_THAT(result, ElementsAreArray(expected));
        }
    }

    void IpRangeFromIpv4Builder() {
        auto range = TIpAddressRange::From("192.168.0.0")
            .To("192.168.0.255")
            .Build();

        ASSERT_THAT(range.Size(), Eq(256));
    }

    void IpRangeFromIpv4BuilderFromTIpv6Address() {
        const auto s = TIpv6Address::FromString("192.168.0.0");
        const auto e = TIpv6Address::FromString("192.168.0.255");
        auto range = TIpAddressRange::From(s).To(e).Build();

        ASSERT_THAT(range.Size(), Eq(256));
    }

    void IpRangeFromInvalidIpv4() {
        auto build = [] (auto from, auto to) {
            return TIpAddressRange::From(from).To(to).Build();
        };

        ASSERT_THROW(build("192.168.0.255", "192.168.0.0"), yexception);
        ASSERT_THROW(build("192.168.0.0", "192.168.0.300"), yexception);
        ASSERT_THROW(build("192.168.0.300", "192.168.0.330"), yexception);
        ASSERT_THROW(build("192.168.0.0", "::1"), yexception);
        ASSERT_THROW(build(TIpv6Address{}, TIpv6Address{}), yexception);
    }

    void IpRangeFromInvalidIpv6() {
        auto build = [] (auto from, auto to) {
            return TIpAddressRange::From(from).To(to).Build();
        };

        ASSERT_THROW(build("ffce:abcd::00ff", "ffce:abcd::"), yexception);
        ASSERT_THROW(build("ffce:abcd::", "ffce:abcd::fffff"), yexception);
        ASSERT_THROW(build("ffce:abcd::10000", "ffce:abcd::ffff"), yexception);
        ASSERT_THROW(build("ffce:abcd::", TIpv6Address{}), yexception);

        auto ctor = [] (auto s, auto e) {
            return TIpAddressRange{s, e};
        };

        ASSERT_THROW(ctor(TIpv6Address{}, TIpv6Address{}), yexception);
        ASSERT_THROW(ctor("", ""), yexception);
    }

    void ManualIteration() {
        {
            TIpAddressRange range{"::", "::"};
            auto it = range.Begin();
            ++it;
            bool ok;
            ASSERT_THAT(*it, Eq(TIpv6Address::FromString("::1", ok)));

            for (auto i = 0; i < 254; ++i, ++it) {
            }

            ASSERT_THAT(*it, Eq(TIpv6Address::FromString("::ff", ok)));
        }

        {
            TIpAddressRange range{"0.0.0.0", "0.0.0.0"};
            auto it = range.Begin();
            ++it;
            bool ok;
            ASSERT_THAT(*it, Eq(TIpv6Address::FromString("0.0.0.1", ok)));

            for (auto i = 0; i < 254; ++i, ++it) {
            }

            ASSERT_THAT(*it, Eq(TIpv6Address::FromString("0.0.0.255", ok)));
        }
    }

    void RangeRelations() {
        {
            auto range = TIpAddressRange::From(MIN_IPV6_ADDR)
                .To(MAX_IPV6_ADDR)
                .Build();

            ASSERT_TRUE(range.Overlaps(range));
            ASSERT_TRUE(range.Contains(range));
            // XXX
            //ASSERT_FALSE(range.IsConsecutive(range));
        }
        {
            auto range = TIpAddressRange::From("0.0.0.1").To("0.0.0.4").Build();
            auto range0 = TIpAddressRange::From("0.0.0.0").Build();
            auto range1 = TIpAddressRange::From("0.0.0.1").Build();
            auto range2 = TIpAddressRange::From("0.0.0.5").Build();
            auto range4 = TIpAddressRange::From("0.0.0.4").Build();

            ASSERT_FALSE(range.Overlaps(range0));
            ASSERT_TRUE(range.IsConsecutive(range0));
            ASSERT_FALSE(range.Contains(range0));

            ASSERT_TRUE(range.Overlaps(range1));
            ASSERT_FALSE(range.IsConsecutive(range1));
            ASSERT_TRUE(range.Contains(range1));

            ASSERT_TRUE(range.Overlaps(range4));
            ASSERT_FALSE(range.IsConsecutive(range4));
            ASSERT_TRUE(range.Contains(range4));
        }
        {
            auto range = TIpAddressRange::From("0.0.0.1").To("0.0.0.4").Build();
            auto range2 = TIpAddressRange::From("0.0.0.0").To("0.0.0.2").Build();

            ASSERT_TRUE(range.Overlaps(range2));
            ASSERT_FALSE(range.IsConsecutive(range2));
            ASSERT_FALSE(range.Contains(range2));

            bool ok;
            ASSERT_TRUE(range.Contains(TIpv6Address::FromString("0.0.0.1", ok)));
            ASSERT_TRUE(range.Contains(TIpv6Address::FromString("0.0.0.2", ok)));
            ASSERT_FALSE(range.Contains(TIpv6Address::FromString("0.0.0.5", ok)));
        }
    }

    void RangeUnion() {
        {
            auto range = TIpAddressRange::From(MIN_IPV6_ADDR)
                .To(MAX_IPV6_ADDR)
                .Build();

            ASSERT_THAT(range.Union(range), Eq(range));
            ASSERT_THAT(range.Union(TIpAddressRange::From("::")), range);
            ASSERT_THAT(range.Union(TIpAddressRange::From("::1")), range);

            ASSERT_THROW(range.Union(TIpAddressRange::From("0.0.0.0")), yexception);
        }

        {
            auto expected = TIpAddressRange::From("0.0.0.1").To("0.0.0.10").Build();

            auto range = TIpAddressRange{"0.0.0.1", "0.0.0.3"}.Union({"0.0.0.4", "0.0.0.10"});
            ASSERT_THAT(range, Eq(expected));

            auto range2 = TIpAddressRange{"0.0.0.1", "0.0.0.3"}.Union({"0.0.0.2", "0.0.0.10"});
            ASSERT_THAT(range2, Eq(expected));

            auto range3 = TIpAddressRange{"0.0.0.2", "0.0.0.3"}.Union({"0.0.0.1", "0.0.0.10"});
            ASSERT_THAT(range2, Eq(expected));

            auto range4 = TIpAddressRange{"0.0.0.1", "0.0.0.10"}.Union({"0.0.0.2", "0.0.0.3"});
            ASSERT_THAT(range2, Eq(expected));

            ASSERT_THROW(range.Union(TIpAddressRange::From("10.0.0.0")), yexception);
        }
    }
};

class TRangeSetTests: public TTestBase {
public:
    UNIT_TEST_SUITE(TRangeSetTests);
    UNIT_TEST(AddDisjoint);
    UNIT_TEST(AddOverlapping);
    UNIT_TEST(AddConsecutive);
    UNIT_TEST(DisallowsMixingTypes);
    UNIT_TEST(MembershipTest);
    UNIT_TEST_SUITE_END();

    void AddDisjoint() {
        TIpRangeSet set;
        TVector<TIpAddressRange> expected {
            TIpAddressRange::From("0.0.0.0").To("0.0.0.2").Build(),
            TIpAddressRange::From("0.0.0.4").To("255.255.255.255").Build(),
        };

        for (auto&& r : expected) {
            set.Add(r);
        }

        ASSERT_THAT(set, ElementsAreArray(expected));
    }

    void TestAdding(const TVector<TIpAddressRange>& toInsert, const TVector<TIpAddressRange>& expected) {
        TIpRangeSet set;
        {
            set.Add(toInsert);

            ASSERT_THAT(set, ElementsAreArray(expected));
        }

        {
            for (auto it = toInsert.rbegin(); it != toInsert.rend(); ++it) {
                set.Add(*it);
            }

            ASSERT_THAT(set, ElementsAreArray(expected));
        }
    }

    void AddOverlapping() {
        {
            TVector<TIpAddressRange> toInsert {
                TIpAddressRange::From("0.0.0.0").To("0.0.0.2").Build(),
                TIpAddressRange::From("0.0.0.2").To("0.0.0.4").Build(),
                TIpAddressRange::From("0.0.0.4").To("255.255.255.255").Build(),
            };

            TVector<TIpAddressRange> expected {
                TIpAddressRange::From("0.0.0.0").To("255.255.255.255").Build(),
            };

            TestAdding(toInsert, expected);
        }
        {
            TVector<TIpAddressRange> toInsert {
                TIpAddressRange::From("0.0.0.0").To("0.0.0.5").Build(),
                TIpAddressRange::From("0.0.0.8").To("0.0.0.10").Build(),
                TIpAddressRange::From("0.0.0.7").To("0.0.0.12").Build(),
            };

            TVector<TIpAddressRange> expected {
                TIpAddressRange::From("0.0.0.0").To("0.0.0.5").Build(),
                TIpAddressRange::From("0.0.0.7").To("0.0.0.12").Build(),
            };

            TestAdding(toInsert, expected);
        }
        {
            TVector<TIpAddressRange> toInsert {
                TIpAddressRange::From("0.0.0.0").To("0.0.0.5").Build(),
                TIpAddressRange::From("0.0.0.7").To("0.0.0.12").Build(),
                TIpAddressRange::From("0.0.0.8").To("0.0.0.10").Build(),
            };

            TVector<TIpAddressRange> expected {
                TIpAddressRange::From("0.0.0.0").To("0.0.0.5").Build(),
                TIpAddressRange::From("0.0.0.7").To("0.0.0.12").Build(),
            };

            TestAdding(toInsert, expected);
        }
        {
            TVector<TIpAddressRange> toInsert {
                TIpAddressRange::From("0.0.0.0").To("0.0.0.5").Build(),
                TIpAddressRange::From("0.0.0.3").To("0.0.0.10").Build(),
            };

            TVector<TIpAddressRange> expected {
                TIpAddressRange::From("0.0.0.0").To("0.0.0.10").Build(),
            };

            TestAdding(toInsert, expected);
        }
    }

    void DisallowsMixingTypes() {
        TVector<TIpAddressRange> toInsert {
            TIpAddressRange::From("0.0.0.0").To("0.0.0.5").Build(),
            TIpAddressRange::From("::1").Build(),
        };

        TIpRangeSet rangeSet;

        ASSERT_THROW([&] { rangeSet.Add(toInsert); }(), yexception);
        ASSERT_THROW([&] { rangeSet.Add(toInsert[1]); rangeSet.Add(toInsert[0]); }(), yexception);
    }

    void AddConsecutive() {
        {
            TVector<TIpAddressRange> toInsert {
                TIpAddressRange::From("0.0.0.0").To("0.0.0.5").Build(),
                TIpAddressRange::From("0.0.0.6").To("0.0.0.7").Build(),
                TIpAddressRange::From("0.0.0.8").To("0.0.0.10").Build(),
            };

            TVector<TIpAddressRange> expected {
                TIpAddressRange::From("0.0.0.0").To("0.0.0.10").Build(),
            };

            TestAdding(toInsert, expected);
        }
        {
            TVector<TIpAddressRange> toInsert {
                TIpAddressRange::From("0.0.0.0").To("255.255.255.255").Build(),
                TIpAddressRange::From("0.0.0.0").To("255.255.255.255").Build(),
            };

            TVector<TIpAddressRange> expected {
                TIpAddressRange::From("0.0.0.0").To("255.255.255.255").Build(),
            };

            TestAdding(toInsert, expected);
        }
    }

    void MembershipTest() {
        TVector<TIpAddressRange> toInsert {
            TIpAddressRange::From("0.0.0.0").To("0.0.0.5").Build(),
            TIpAddressRange::From("0.0.0.7").To("0.0.0.12").Build(),
            TIpAddressRange::From("0.0.0.8").To("0.0.0.10").Build(),
        };

        TIpRangeSet rangeSet;
        rangeSet.Add(toInsert);

        TVector<TIpAddressRange> in {
            TIpAddressRange::From("0.0.0.0").To("0.0.0.5").Build(),
            TIpAddressRange::From("0.0.0.7").To("0.0.0.12").Build(),
        };

        TVector<TIpAddressRange> notIn {
            TIpAddressRange::From("0.0.0.6").Build(),
            TIpAddressRange::From("0.0.0.13").To("0.0.0.255").Build(),
            // enumerating full range is slow and makes little sense
            TIpAddressRange::From("255.255.255.0").To("255.255.255.255").Build(),
        };

        for (auto&& range : in) {
            for (auto&& addr : range) {
                ASSERT_TRUE(rangeSet.Contains(addr));
                ASSERT_THAT(*rangeSet.Find(addr), Eq(range));
            }
        }

        for (auto&& range : notIn) {
            for (auto&& addr : range) {
                ASSERT_FALSE(rangeSet.Contains(addr));
                ASSERT_THAT(rangeSet.Find(addr), rangeSet.End());
            }
        }

        bool ok{};
        ASSERT_THAT(rangeSet.Find(TIpv6Address::FromString("::1", ok)), Eq(rangeSet.End()));
        ASSERT_FALSE(rangeSet.Contains(TIpv6Address::FromString("::1", ok)));
    }
};

UNIT_TEST_SUITE_REGISTRATION(TIpRangeTests);
UNIT_TEST_SUITE_REGISTRATION(TRangeSetTests);
