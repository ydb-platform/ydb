#include "net.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NSecurity {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TNetUtilTest) {

    Y_UNIT_TEST(TestIsIPv4) {
        // Valid IPv4 addresses
        UNIT_ASSERT(IsIPv4("192.168.1.1"));     // private class C address
        UNIT_ASSERT(IsIPv4("127.0.0.1"));       // loopback address
        UNIT_ASSERT(IsIPv4("255.255.255.255")); // broadcast address (max value)
        UNIT_ASSERT(IsIPv4("0.0.0.0"));         // unspecified address (min value)
        UNIT_ASSERT(IsIPv4("10.0.0.1"));        // private class A
        UNIT_ASSERT(IsIPv4("172.16.0.1"));      // private class B
        UNIT_ASSERT(IsIPv4("1.2.3.4"));         // simple address
        UNIT_ASSERT(IsIPv4("100.64.0.1"));      // shared address space (CGN)
        UNIT_ASSERT(IsIPv4("169.254.1.1"));     // link-local
        UNIT_ASSERT(IsIPv4("224.0.0.1"));       // multicast
        UNIT_ASSERT(IsIPv4("192.0.2.1"));       // documentation (TEST-NET-1)
        UNIT_ASSERT(IsIPv4("198.51.100.1"));    // documentation (TEST-NET-2)
        UNIT_ASSERT(IsIPv4("203.0.113.1"));     // documentation (TEST-NET-3)
        UNIT_ASSERT(IsIPv4("1.0.0.0"));         // minimal non-zero first octet
        UNIT_ASSERT(IsIPv4("254.254.254.254")); // near-max address

        // Invalid IPv4 addresses
        UNIT_ASSERT(!IsIPv4(""));                                        // empty string
        UNIT_ASSERT(!IsIPv4("::1"));                                     // IPv6 address, not IPv4
        UNIT_ASSERT(!IsIPv4("2001:0db8:85a3:0000:0000:8a2e:0370:7334")); // full IPv6 address, not IPv4
        UNIT_ASSERT(!IsIPv4("not-an-ip"));                               // arbitrary non-IP string
        UNIT_ASSERT(!IsIPv4("192.168.1"));                               // only 3 octets
        UNIT_ASSERT(!IsIPv4("192.168.1.1.1"));                           // 5 octets
        UNIT_ASSERT(!IsIPv4("256.0.0.1"));                               // first octet out of range
        UNIT_ASSERT(!IsIPv4("192.168.1.256"));                           // last octet out of range
        UNIT_ASSERT(!IsIPv4("192.168.256.1"));                           // middle octet out of range
        UNIT_ASSERT(!IsIPv4("-1.0.0.0"));                                // negative value
        UNIT_ASSERT(!IsIPv4("1.2.3.4.5"));                               // too many octets
        UNIT_ASSERT(!IsIPv4("1.2.3"));                                   // too few octets
        UNIT_ASSERT(!IsIPv4("1.2.3."));                                  // trailing dot
        UNIT_ASSERT(!IsIPv4(".1.2.3.4"));                                // leading dot
        UNIT_ASSERT(!IsIPv4("1..2.3.4"));                                // double dot
        UNIT_ASSERT(!IsIPv4(" 192.168.1.1"));                            // leading space
        UNIT_ASSERT(!IsIPv4("192.168.1.1 "));                            // trailing space
        UNIT_ASSERT(!IsIPv4("[192.168.1.1]"));                           // brackets
        UNIT_ASSERT(!IsIPv4("192.168.1.1/24"));                          // CIDR notation
        UNIT_ASSERT(!IsIPv4("192.168.1.1:80"));                          // with port
        UNIT_ASSERT(!IsIPv4("abc.def.ghi.jkl"));                         // alphabetic octets
        UNIT_ASSERT(!IsIPv4("localhost"));                               // localhost is not an IPv4 address
        UNIT_ASSERT(!IsIPv4("0x7f.0x00.0x00.0x01"));                     // hex notation
        UNIT_ASSERT(!IsIPv4("1000.0.0.1"));                              // 4-digit octet
        UNIT_ASSERT(!IsIPv4("198.51.100.1%eeth0"));                      // correct ipv4 with ipv6 zone ID
    }

    Y_UNIT_TEST(TestIsIPv6) {
        // Valid IPv6 addresses
        UNIT_ASSERT(IsIPv6("::1"));                                     // loopback address (short form)
        UNIT_ASSERT(IsIPv6("2001:0db8:85a3:0000:0000:8a2e:0370:7334")); // full 8-group notation
        UNIT_ASSERT(IsIPv6("2001:db8:85a3::8a2e:370:7334"));            // compressed with :: in the middle
        UNIT_ASSERT(IsIPv6("::ffff:192.0.2.1"));                        // IPv4-mapped IPv6 address
        UNIT_ASSERT(IsIPv6("::"));                                      // all-zeros unspecified address
        UNIT_ASSERT(IsIPv6("fe80::1"));                                 // link-local address
        UNIT_ASSERT(IsIPv6("ff02::1"));                                 // multicast all-nodes
        UNIT_ASSERT(IsIPv6("::ffff:127.0.0.1"));                        // IPv4-mapped IPv6 (loopback)
        UNIT_ASSERT(IsIPv6("::ffff:0:0"));                              // IPv4-mapped with zeros
        UNIT_ASSERT(IsIPv6("2001:db8::1"));                             // documentation address with compression
        UNIT_ASSERT(IsIPv6("2001:0db8:0000:0000:0000:0000:0000:0001")); // fully expanded
        UNIT_ASSERT(IsIPv6("0:0:0:0:0:0:0:0"));                         // all-zeros long form
        UNIT_ASSERT(IsIPv6("0:0:0:0:0:0:0:1"));                         // loopback long form
        UNIT_ASSERT(IsIPv6("fd00::1"));                                 // unique local address
        UNIT_ASSERT(IsIPv6("::ffff:10.0.0.1"));                         // IPv4-mapped private
        UNIT_ASSERT(IsIPv6("fe80::1%eth0"));                            // IPv6 with zone ID

        // Invalid IPv6 addresses
        UNIT_ASSERT(!IsIPv6(""));                                             // empty string
        UNIT_ASSERT(!IsIPv6("192.168.1.1"));                                  // IPv4 address, not IPv6
        UNIT_ASSERT(!IsIPv6("not-an-ip"));                                    // arbitrary non-IP string
        UNIT_ASSERT(!IsIPv6(":::"));                                          // triple colon is invalid syntax
        UNIT_ASSERT(!IsIPv6("2001:0db8:85a3:0000:0000:8a2e:0370:7334:1234")); // too many groups
        UNIT_ASSERT(!IsIPv6("[2001:0db8:85a3:0000:0000:8a2e:0370:7334]"));    // brackets
        UNIT_ASSERT(!IsIPv6("2001:db8::85a3::7334"));                         // double :: compression
        UNIT_ASSERT(!IsIPv6("2001:db8:85a3:0000:0000:8a2e:0370:gggg"));       // invalid hex digits
        UNIT_ASSERT(!IsIPv6("12345::1"));                                     // group exceeds 4 hex digits
        UNIT_ASSERT(!IsIPv6("[::1]"));                                        // brackets are not part of the address
        UNIT_ASSERT(!IsIPv6("localhost"));                                    // localhost is not an IPv6 address
        UNIT_ASSERT(!IsIPv6(" ::1"));                                         // leading space
        UNIT_ASSERT(!IsIPv6("::1 "));                                         // trailing space
        UNIT_ASSERT(!IsIPv6("2001:db8: :1"));                                 // space within address
        UNIT_ASSERT(!IsIPv6("::ffff:999.0.0.1"));                             // invalid IPv4 part in mapped address
        UNIT_ASSERT(!IsIPv6("::ffff:192.168.1"));                             // incomplete IPv4 part in mapped address
        UNIT_ASSERT(!IsIPv6("2001:db8::g"));                                  // single invalid hex char
    }

    Y_UNIT_TEST(TestIsGoodPeernameFormat) {
        UNIT_ASSERT(IsGoodPeernameFormat("127.0.0.1"));
        UNIT_ASSERT(IsGoodPeernameFormat("192.168.1.1"));
        UNIT_ASSERT(IsGoodPeernameFormat("255.255.255.255"));
        UNIT_ASSERT(IsGoodPeernameFormat("0.0.0.0"));

        UNIT_ASSERT(IsGoodPeernameFormat("127.0.0.1:8080"));
        UNIT_ASSERT(IsGoodPeernameFormat("192.168.1.1:1"));
        UNIT_ASSERT(IsGoodPeernameFormat("10.0.0.1:65535"));
        UNIT_ASSERT(IsGoodPeernameFormat("172.16.0.1:22"));

        UNIT_ASSERT(IsGoodPeernameFormat("ipv4:127.0.0.1"));
        UNIT_ASSERT(IsGoodPeernameFormat("ipv4:192.168.1.1"));
        UNIT_ASSERT(IsGoodPeernameFormat("ipv4:10.0.0.1"));

        UNIT_ASSERT(IsGoodPeernameFormat("ipv4:127.0.0.1:8080"));
        UNIT_ASSERT(IsGoodPeernameFormat("ipv4:192.168.1.1:3000"));
        UNIT_ASSERT(IsGoodPeernameFormat("ipv4:10.0.0.1:22"));
        UNIT_ASSERT(IsGoodPeernameFormat("ipv4:172.16.0.1:65535"));

        UNIT_ASSERT(IsGoodPeernameFormat("::1"));
        UNIT_ASSERT(IsGoodPeernameFormat("2001:db8::1"));
        UNIT_ASSERT(IsGoodPeernameFormat("fe80::1"));
        UNIT_ASSERT(IsGoodPeernameFormat("::"));
        UNIT_ASSERT(IsGoodPeernameFormat("::ffff:192.0.2.1"));

        UNIT_ASSERT(IsGoodPeernameFormat("[::1]"));
        UNIT_ASSERT(IsGoodPeernameFormat("[2001:db8::1]"));
        UNIT_ASSERT(IsGoodPeernameFormat("[fe80::1]"));
        UNIT_ASSERT(IsGoodPeernameFormat("[::]"));
        UNIT_ASSERT(IsGoodPeernameFormat("[::ffff:192.0.2.1]"));

        UNIT_ASSERT(IsGoodPeernameFormat("[::1]:8080"));
        UNIT_ASSERT(IsGoodPeernameFormat("[2001:db8::1]:3000"));
        UNIT_ASSERT(IsGoodPeernameFormat("[fe80::1]:22"));
        UNIT_ASSERT(IsGoodPeernameFormat("[::]:65535"));
        UNIT_ASSERT(IsGoodPeernameFormat("[::ffff:192.0.2.1]:80"));

        UNIT_ASSERT(IsGoodPeernameFormat("ipv6:::1"));
        UNIT_ASSERT(IsGoodPeernameFormat("ipv6:2001:0db8:85a3:0000:0000:8a2e:0370:7334"));
        UNIT_ASSERT(IsGoodPeernameFormat("ipv6:2001:db8::1"));
        UNIT_ASSERT(IsGoodPeernameFormat("ipv6:fe80::1"));
        UNIT_ASSERT(IsGoodPeernameFormat("ipv6:::"));
        UNIT_ASSERT(IsGoodPeernameFormat("ipv6:::ffff:192.0.2.1"));

        UNIT_ASSERT(IsGoodPeernameFormat("ipv6:[::1]"));
        UNIT_ASSERT(IsGoodPeernameFormat("ipv6:[2001:0db8:85a3:0000:0000:8a2e:0370:7334]"));
        UNIT_ASSERT(IsGoodPeernameFormat("ipv6:[2001:db8::1]"));
        UNIT_ASSERT(IsGoodPeernameFormat("ipv6:[fe80::1]"));
        UNIT_ASSERT(IsGoodPeernameFormat("ipv6:[::]"));
        UNIT_ASSERT(IsGoodPeernameFormat("ipv6:[::ffff:192.0.2.1]"));

        UNIT_ASSERT(IsGoodPeernameFormat("ipv6:[::1]:8080"));
        UNIT_ASSERT(IsGoodPeernameFormat("ipv6:[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:1234"));
        UNIT_ASSERT(IsGoodPeernameFormat("ipv6:[2001:db8::1]:3000"));
        UNIT_ASSERT(IsGoodPeernameFormat("ipv6:[fe80::1]:22"));
        UNIT_ASSERT(IsGoodPeernameFormat("ipv6:[::]:65535"));
        UNIT_ASSERT(IsGoodPeernameFormat("ipv6:[::ffff:192.0.2.1]:80"));

        UNIT_ASSERT(!IsGoodPeernameFormat(""));
        UNIT_ASSERT(!IsGoodPeernameFormat("invalid_format"));

        UNIT_ASSERT(!IsGoodPeernameFormat("127.0.0.1:"));
        UNIT_ASSERT(!IsGoodPeernameFormat("[127.0.0.1]"));
        UNIT_ASSERT(!IsGoodPeernameFormat("[127.0.0.1]:8080"));
        UNIT_ASSERT(!IsGoodPeernameFormat("127.0.0.1:999999"));
        UNIT_ASSERT(!IsGoodPeernameFormat("127.0.0.1:65536"));
        UNIT_ASSERT(!IsGoodPeernameFormat("256.1.1.1"));
        UNIT_ASSERT(!IsGoodPeernameFormat("1.1.1.1.1"));
        UNIT_ASSERT(!IsGoodPeernameFormat("1.1.1"));
        UNIT_ASSERT(!IsGoodPeernameFormat("1.1.1."));
        UNIT_ASSERT(!IsGoodPeernameFormat(".1.1.1"));
        UNIT_ASSERT(!IsGoodPeernameFormat("1.-1.1.1"));
        UNIT_ASSERT(!IsGoodPeernameFormat("1.a.1.1"));
        UNIT_ASSERT(!IsGoodPeernameFormat("127.0.0.1:port"));

        UNIT_ASSERT(!IsGoodPeernameFormat("ipv4:"));
        UNIT_ASSERT(!IsGoodPeernameFormat("ipv4:2001:db8::1"));
        UNIT_ASSERT(!IsGoodPeernameFormat("ipv4:[2001:db8::1]"));
        UNIT_ASSERT(!IsGoodPeernameFormat("ipv4:[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:1234"));
        UNIT_ASSERT(!IsGoodPeernameFormat("ipv4:127.0.0.1:"));
        UNIT_ASSERT(!IsGoodPeernameFormat("ipv4:[127.0.0.1]"));
        UNIT_ASSERT(!IsGoodPeernameFormat("ipv4:[127.0.0.1]:8080"));
        UNIT_ASSERT(!IsGoodPeernameFormat("ipv4:127.0.0.1:999999"));
        UNIT_ASSERT(!IsGoodPeernameFormat("ipv4:127.0.0.1:65536"));
        UNIT_ASSERT(!IsGoodPeernameFormat("ipv4:256.1.1.1"));
        UNIT_ASSERT(!IsGoodPeernameFormat("ipv4:127.0.0.1:port"));
        UNIT_ASSERT(!IsGoodPeernameFormat("ipv4:invalid"));

        UNIT_ASSERT(!IsGoodPeernameFormat("[::1]:"));
        UNIT_ASSERT(!IsGoodPeernameFormat("[::1]:999999"));
        UNIT_ASSERT(!IsGoodPeernameFormat("[::1]:65536"));
        UNIT_ASSERT(!IsGoodPeernameFormat("[::1]:port"));
        UNIT_ASSERT(!IsGoodPeernameFormat(":::1"));
        UNIT_ASSERT(!IsGoodPeernameFormat("2001:0db8:85a3:0000:0000:8a2e:0370:7334:1234"));

        UNIT_ASSERT(!IsGoodPeernameFormat("ipv6:"));
        UNIT_ASSERT(!IsGoodPeernameFormat("ipv6:192.168.1.1"));
        UNIT_ASSERT(!IsGoodPeernameFormat("ipv6:192.168.1.1:3000"));
        UNIT_ASSERT(!IsGoodPeernameFormat("ipv6:[192.168.1.1]"));
        UNIT_ASSERT(!IsGoodPeernameFormat("ipv6:[192.168.1.1]:3000"));
        UNIT_ASSERT(!IsGoodPeernameFormat("ipv6:[::1]:"));
        UNIT_ASSERT(!IsGoodPeernameFormat("ipv6:[::1]:999999"));
        UNIT_ASSERT(!IsGoodPeernameFormat("ipv6:[::1]:65536"));
        UNIT_ASSERT(!IsGoodPeernameFormat("ipv6:[::1]:port"));
        UNIT_ASSERT(!IsGoodPeernameFormat("ipv6:invalid"));
        UNIT_ASSERT(!IsGoodPeernameFormat("ipv6:2001:0db8:85a3:0000:0000:8a2e:0370:7334:1234"));
    }

    Y_UNIT_TEST(TestParsePeername) {
        {
            const TString addr{"127.0.0.1"};
            auto res = ParsePeername(addr);
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
        }
        {
            const TString addr{"192.168.1.1"};
            auto res = ParsePeername(addr);
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
        }
        {
            const TString addr{"255.255.255.255"};
            auto res = ParsePeername(addr);
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
        }
        {
            const TString addr{"0.0.0.0"};
            auto res = ParsePeername(addr);
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
        }

        {
            const TString addr{"127.0.0.1"};
            const TString port{"8080"};
            auto res = ParsePeername(addr + ":" + port);
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
            UNIT_ASSERT_STRINGS_EQUAL(addr + ":" + port, NAddr::PrintHostAndPort(*res));
        }
        {
            const TString addr{"192.168.1.1"};
            const TString port{"1"};
            auto res = ParsePeername(addr + ":" + port);
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
            UNIT_ASSERT_STRINGS_EQUAL(addr + ":" + port, NAddr::PrintHostAndPort(*res));
        }
        {
            const TString addr{"10.0.0.1"};
            const TString port{"65535"};
            auto res = ParsePeername(addr + ":" + port);
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
            UNIT_ASSERT_STRINGS_EQUAL(addr + ":" + port, NAddr::PrintHostAndPort(*res));
        }
        {
            const TString addr{"172.16.0.1"};
            const TString port{"22"};
            auto res = ParsePeername(addr + ":" + port);
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
            UNIT_ASSERT_STRINGS_EQUAL(addr + ":" + port, NAddr::PrintHostAndPort(*res));
        }

        {
            const TString addr{"127.0.0.1"};
            auto res = ParsePeername("ipv4:" + addr);
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
        }
        {
            const TString addr{"192.168.1.1"};
            auto res = ParsePeername("ipv4:" + addr);
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
        }
        {
            const TString addr{"10.0.0.1"};
            auto res = ParsePeername("ipv4:" + addr);
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
        }

        {
            const TString addr{"127.0.0.1"};
            const TString port{"8080"};
            auto res = ParsePeername("ipv4:" + addr + ":" + port);
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
            UNIT_ASSERT_STRINGS_EQUAL(addr + ":" + port, NAddr::PrintHostAndPort(*res));
        }
        {
            const TString addr{"192.168.1.1"};
            const TString port{"3000"};
            auto res = ParsePeername("ipv4:" + addr + ":" + port);
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
            UNIT_ASSERT_STRINGS_EQUAL(addr + ":" + port, NAddr::PrintHostAndPort(*res));
        }
        {
            const TString addr{"10.0.0.1"};
            const TString port{"22"};
            auto res = ParsePeername("ipv4:" + addr + ":" + port);
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
            UNIT_ASSERT_STRINGS_EQUAL(addr + ":" + port, NAddr::PrintHostAndPort(*res));
        }
        {
            const TString addr{"172.16.0.1"};
            const TString port{"65535"};
            auto res = ParsePeername("ipv4:" + addr + ":" + port);
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
            UNIT_ASSERT_STRINGS_EQUAL(addr + ":" + port, NAddr::PrintHostAndPort(*res));
        }

        {
            const TString addr{"2001:2db8:85a3:7843:dbaf:8a2e:4370:7334"};
            auto res = ParsePeername(addr);
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
        }
        {
            const TString addr{"::1"};
            auto res = ParsePeername(addr);
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
        }
        {
            const TString addr{"2001:db8::1"};
            auto res = ParsePeername(addr);
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
        }
        {
            const TString addr{"fe89::1"};
            auto res = ParsePeername(addr);
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
        }
        {
            const TString addr{"::"};
            auto res = ParsePeername(addr);
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
        }
        {
            const TString addr{"::ffff:192.0.2.1"};
            auto res = ParsePeername(addr);
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
        }

        {
            const TString addr{"2001:2db8:85a3:7843:dbaf:8a2e:4370:7334"};
            auto res = ParsePeername("[" + addr + "]");
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
        }
        {
            const TString addr{"::1"};
            auto res = ParsePeername("[" + addr + "]");
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
        }
        {
            const TString addr{"2001:db8::1"};
            auto res = ParsePeername("[" + addr + "]");
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
        }
        {
            const TString addr{"fe89::1"};
            auto res = ParsePeername("[" + addr + "]");
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
        }
        {
            const TString addr{"::"};
            auto res = ParsePeername("[" + addr + "]");
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
        }
        {
            const TString addr{"::ffff:192.0.2.1"};
            auto res = ParsePeername("[" + addr + "]");
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
        }

        {
            const TString addr{"2001:2db8:85a3:7843:dbaf:8a2e:4370:7334"};
            const TString port{"1234"};
            auto res = ParsePeername("[" + addr + "]" + ":" + port);
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
            UNIT_ASSERT_STRINGS_EQUAL("[" + addr + "]" + ":" + port, NAddr::PrintHostAndPort(*res));
        }
        {
            const TString addr{"::1"};
            const TString port{"8080"};
            auto res = ParsePeername("[" + addr + "]" + ":" + port);
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
            UNIT_ASSERT_STRINGS_EQUAL("[" + addr + "]" + ":" + port, NAddr::PrintHostAndPort(*res));
        }
        {
            const TString addr{"2001:db8::1"};
            const TString port{"3000"};
            auto res = ParsePeername("[" + addr + "]" + ":" + port);
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
            UNIT_ASSERT_STRINGS_EQUAL("[" + addr + "]" + ":" + port, NAddr::PrintHostAndPort(*res));
        }
        {
            const TString addr{"fe80::1"};
            const TString port{"22"};
            auto res = ParsePeername("[" + addr + "]" + ":" + port);
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
            UNIT_ASSERT_STRINGS_EQUAL("[" + addr + "]" + ":" + port, NAddr::PrintHostAndPort(*res));

        }
        {
            const TString addr{"::"};
            const TString port{"65535"};
            auto res = ParsePeername("[" + addr + "]" + ":" + port);
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
            UNIT_ASSERT_STRINGS_EQUAL("[" + addr + "]" + ":" + port, NAddr::PrintHostAndPort(*res));

        }
        {
            const TString addr{"::ffff:192.0.2.1"};
            const TString port{"80"};
            auto res = ParsePeername("[" + addr + "]" + ":" + port);
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
            UNIT_ASSERT_STRINGS_EQUAL("[" + addr + "]" + ":" + port, NAddr::PrintHostAndPort(*res));

        }

        {
            const TString addr{"::1"};
            auto res = ParsePeername("ipv6:" + addr);
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
        }
        {
            const TString addr{"2001:2db8:85a3:7843:dbaf:8a2e:4370:7334"};
            auto res = ParsePeername("ipv6:" + addr);
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
        }
        {
            const TString addr{"2001:db8::1"};
            auto res = ParsePeername("ipv6:" + addr);
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
        }
        {
            const TString addr{"fe80::1"};
            auto res = ParsePeername("ipv6:" + addr);
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
        }
        {
            const TString addr{"::"};
            auto res = ParsePeername("ipv6:" + addr);
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
        }
        {
            const TString addr{"::ffff:192.0.2.1"};
            auto res = ParsePeername("ipv6:" + addr);
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
        }

        {
            const TString addr{"::1"};
            auto res = ParsePeername("ipv6:[" + addr + "]");
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
        }
        {
            const TString addr{"2001:2db8:85a3:7843:dbaf:8a2e:4370:7334"};
            auto res = ParsePeername("ipv6:[" + addr + "]");
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
        }
        {
            const TString addr{"2001:db8::1"};
            auto res = ParsePeername("ipv6:[" + addr + "]");
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
        }
        {
            const TString addr{"fe80::1"};
            auto res = ParsePeername("ipv6:[" + addr + "]");
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
        }
        {
            const TString addr{"::"};
            auto res = ParsePeername("ipv6:[" + addr + "]");
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
        }
        {
            const TString addr{"::ffff:192.0.2.1"};
            auto res = ParsePeername("ipv6:[" + addr + "]");
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
        }

        {
            const TString addr{"::1"};
            const TString port{"8080"};
            auto res = ParsePeername("ipv6:[" + addr + "]" + ":" + port);
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
            UNIT_ASSERT_STRINGS_EQUAL("[" + addr + "]" + ":" + port, NAddr::PrintHostAndPort(*res));
        }
        {
            const TString addr{"2001:2db8:85a3:7843:dbaf:8a2e:4370:7334"};
            const TString port{"1234"};
            auto res = ParsePeername("ipv6:[" + addr + "]" + ":" + port);
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
            UNIT_ASSERT_STRINGS_EQUAL("[" + addr + "]" + ":" + port, NAddr::PrintHostAndPort(*res));
        }
        {
            const TString addr{"2001:db8::1"};
            const TString port{"3000"};
            auto res = ParsePeername("ipv6:[" + addr + "]" + ":" + port);
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
            UNIT_ASSERT_STRINGS_EQUAL("[" + addr + "]" + ":" + port, NAddr::PrintHostAndPort(*res));
        }
        {
            const TString addr{"fe80::1"};
            const TString port{"22"};
            auto res = ParsePeername("ipv6:[" + addr + "]" + ":" + port);
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
            UNIT_ASSERT_STRINGS_EQUAL("[" + addr + "]" + ":" + port, NAddr::PrintHostAndPort(*res));
        }
        {
            const TString addr{"::"};
            const TString port{"65535"};
            auto res = ParsePeername("ipv6:[" + addr + "]" + ":" + port);
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
            UNIT_ASSERT_STRINGS_EQUAL("[" + addr + "]" + ":" + port, NAddr::PrintHostAndPort(*res));
        }
        {
            const TString addr{"::ffff:192.0.2.1"};
            const TString port{"80"};
            auto res = ParsePeername("ipv6:[" + addr + "]" + ":" + port);
            UNIT_ASSERT(res);
            UNIT_ASSERT_STRINGS_EQUAL(addr, NAddr::PrintHost(*res));
            UNIT_ASSERT_STRINGS_EQUAL("[" + addr + "]" + ":" + port, NAddr::PrintHostAndPort(*res));
        }

        UNIT_ASSERT(!ParsePeername(""));
        UNIT_ASSERT(!ParsePeername("invalid_format"));

        UNIT_ASSERT(!ParsePeername("127.0.0.1:"));
        UNIT_ASSERT(!ParsePeername("[127.0.0.1]"));
        UNIT_ASSERT(!ParsePeername("127.0.0.1:999999"));
        UNIT_ASSERT(!ParsePeername("127.0.0.1:65536"));
        UNIT_ASSERT(!ParsePeername("256.1.1.1"));
        UNIT_ASSERT(!ParsePeername("1.1.1.1.1"));
        UNIT_ASSERT(!ParsePeername("1.1.1"));
        UNIT_ASSERT(!ParsePeername("1.1.1."));
        UNIT_ASSERT(!ParsePeername(".1.1.1"));
        UNIT_ASSERT(!ParsePeername("1.-1.1.1"));
        UNIT_ASSERT(!ParsePeername("1.a.1.1"));
        UNIT_ASSERT(!ParsePeername("127.0.0.1:port"));

        UNIT_ASSERT(!ParsePeername("ipv4:"));
        UNIT_ASSERT(!ParsePeername("ipv4:2001:db8::1"));
        UNIT_ASSERT(!ParsePeername("ipv4:[2001:db8::1]"));
        UNIT_ASSERT(!ParsePeername("ipv4:[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:1234"));
        UNIT_ASSERT(!ParsePeername("ipv4:127.0.0.1:"));
        UNIT_ASSERT(!ParsePeername("ipv4:127.0.0.1:999999"));
        UNIT_ASSERT(!ParsePeername("ipv4:127.0.0.1:65536"));
        UNIT_ASSERT(!ParsePeername("ipv4:[127.0.0.1]:8080"));
        UNIT_ASSERT(!ParsePeername("ipv4:256.1.1.1"));
        UNIT_ASSERT(!ParsePeername("ipv4:127.0.0.1:port"));
        UNIT_ASSERT(!ParsePeername("ipv4:invalid"));

        UNIT_ASSERT(!ParsePeername("[::1]:"));
        UNIT_ASSERT(!ParsePeername("[::1]:999999"));
        UNIT_ASSERT(!ParsePeername("[::1]:65536"));
        UNIT_ASSERT(!ParsePeername("[::1]:port"));
        UNIT_ASSERT(!ParsePeername(":::1"));
        UNIT_ASSERT(!ParsePeername("2001:0db8:85a3:0000:0000:8a2e:0370:7334:1234"));

        UNIT_ASSERT(!ParsePeername("ipv6:"));
        UNIT_ASSERT(!ParsePeername("ipv6:192.168.1.1"));
        UNIT_ASSERT(!ParsePeername("ipv6:192.168.1.1:3000"));
        UNIT_ASSERT(!ParsePeername("ipv6:[192.168.1.1]"));
        UNIT_ASSERT(!ParsePeername("ipv6:[192.168.1.1]:3000"));
        UNIT_ASSERT(!ParsePeername("ipv6:[::1]:"));
        UNIT_ASSERT(!ParsePeername("ipv6:[::1]:999999"));
        UNIT_ASSERT(!ParsePeername("ipv6:[::1]:65536"));
        UNIT_ASSERT(!ParsePeername("ipv6:[::1]:port"));
        UNIT_ASSERT(!ParsePeername("ipv6:invalid"));
        UNIT_ASSERT(!ParsePeername("ipv6:2001:0db8:85a3:0000:0000:8a2e:0370:7334:1234"));
    }

}

} // namespace NKikimr::NSecurity
