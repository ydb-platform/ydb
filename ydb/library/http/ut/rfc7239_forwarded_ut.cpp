#include <ydb/library/http/rfc7239_forwarded.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/maybe.h>

using namespace NKikimr::NHttp;

namespace {

void AssertParsed(
    TStringBuf name,
    TStringBuf header,
    TStringBuf host,
    TMaybe<ui16> port,
    TStringBuf proto,
    TStringBuf forNode,
    TStringBuf by)
{
    const auto got = ParseRfc7239Forwarded(header);
    UNIT_ASSERT_VALUES_EQUAL_C(got.Host, host, name);
    UNIT_ASSERT_VALUES_EQUAL_C(got.Port.Defined(), port.Defined(), name);
    if (port) {
        UNIT_ASSERT_VALUES_EQUAL_C(*got.Port, *port, name);
    }
    UNIT_ASSERT_VALUES_EQUAL_C(got.Proto, proto, name);
    UNIT_ASSERT_VALUES_EQUAL_C(got.For, forNode, name);
    UNIT_ASSERT_VALUES_EQUAL_C(got.By, by, name);
}

} // namespace

Y_UNIT_TEST_SUITE(Rfc7239Forwarded) {
    Y_UNIT_TEST(Empty) {
        AssertParsed("empty", "", "", Nothing(), "", "", "");
        AssertParsed("ows", "  \t  ", "", Nothing(), "", "", "");
        AssertParsed("empty_pairs", ";,;;,", "", Nothing(), "", "", "");
    }

    Y_UNIT_TEST(RfcSection4Examples) {
        AssertParsed(
            "obfuscated_for",
            "for=\"_gazonk\"",
            "", Nothing(), "", "_gazonk", "");
        AssertParsed(
            "ipv6_for_port",
            "For=\"[2001:db8:cafe::17]:4711\"",
            "", Nothing(), "", "[2001:db8:cafe::17]:4711", "");
        AssertParsed(
            "for_proto_by",
            "for=192.0.2.60;proto=http;by=203.0.113.43",
            "", Nothing(), "http", "192.0.2.60", "203.0.113.43");
        AssertParsed(
            "for_list",
            "for=192.0.2.43, for=198.51.100.17",
            "", Nothing(), "", "192.0.2.43", "");
    }

    Y_UNIT_TEST(RfcSection6Identifiers) {
        AssertParsed(
            "ipv4_for_port",
            "for=\"192.0.2.43:47011\"",
            "", Nothing(), "", "192.0.2.43:47011", "");
        AssertParsed(
            "unknown",
            "for=unknown",
            "", Nothing(), "", "unknown", "");
        AssertParsed(
            "unknown_port",
            "for=\"unknown:1234\"",
            "", Nothing(), "", "unknown:1234", "");
        AssertParsed(
            "obf_list",
            "for=_hidden, for=_SEVKISEK",
            "", Nothing(), "", "_hidden", "");
    }

    Y_UNIT_TEST(RfcSection71EquivalentLists) {
        const TStringBuf expectedFor = "192.0.2.43";
        AssertParsed(
            "compact",
            "for=192.0.2.43,for=\"[2001:db8:cafe::17]\",for=unknown",
            "", Nothing(), "", expectedFor, "");
        AssertParsed(
            "spaced",
            "for=192.0.2.43, for=\"[2001:db8:cafe::17]\", for=unknown",
            "", Nothing(), "", expectedFor, "");
        AssertParsed(
            "joined_fields",
            "for=192.0.2.43, for=\"[2001:db8:cafe::17]\", for=unknown",
            "", Nothing(), "", expectedFor, "");
    }

    Y_UNIT_TEST(HostAndPort) {
        AssertParsed(
            "host",
            "host=example.com",
            "example.com", Nothing(), "", "", "");
        AssertParsed(
            "host_port_quoted",
            "host=\"example.com:8080\"",
            "example.com", ui16{8080}, "", "", "");
        AssertParsed(
            "host_port_unquoted",
            "host=example.com:8080",
            "example.com", ui16{8080}, "", "", "");
        AssertParsed(
            "default_looking_port",
            "host=\"example.com:443\";proto=https",
            "example.com", ui16{443}, "https", "", "");
        AssertParsed(
            "ipv6_host_port",
            "host=\"[2001:db8:cafe::17]:8080\"",
            "[2001:db8:cafe::17]", ui16{8080}, "", "", "");
        AssertParsed(
            "ipv6_host",
            "host=\"[2001:db8:cafe::17]\"",
            "[2001:db8:cafe::17]", Nothing(), "", "", "");
    }

    Y_UNIT_TEST(FullElement) {
        AssertParsed(
            "full",
            "for=192.0.2.60;host=\"mysite.com:8080\";proto=http;by=8.8.8.8",
            "mysite.com", ui16{8080}, "http", "192.0.2.60", "8.8.8.8");
        AssertParsed(
            "proto_then_host",
            "proto=https;host=example.com;for=192.0.2.43;by=203.0.113.60",
            "example.com", Nothing(), "https", "192.0.2.43", "203.0.113.60");
    }

    Y_UNIT_TEST(FirstOccurrenceWins) {
        AssertParsed(
            "first_of_each",
            "for=192.0.2.43, for=198.51.100.17;by=203.0.113.60;proto=http;host=example.com",
            "example.com", Nothing(), "http", "192.0.2.43", "203.0.113.60");
        AssertParsed(
            "later_params_ignored",
            "host=first.example;proto=http, host=second.example;proto=https;for=10.0.0.1;by=10.0.0.2",
            "first.example", Nothing(), "http", "10.0.0.1", "10.0.0.2");
        AssertParsed(
            "duplicate_in_element",
            "host=first.example;host=second.example;proto=http;proto=https",
            "first.example", Nothing(), "http", "", "");
    }

    Y_UNIT_TEST(CaseInsensitiveNamesAndProto) {
        AssertParsed(
            "mixed_case",
            "Host=example.com;PROTO=\"HTTPS\";For=192.0.2.60;BY=203.0.113.43",
            "example.com", Nothing(), "https", "192.0.2.60", "203.0.113.43");
    }

    Y_UNIT_TEST(QuotedStringUnescape) {
        AssertParsed(
            "escaped_quote",
            R"(host="example\".com")",
            "example\".com", Nothing(), "", "", "");
        AssertParsed(
            "escaped_backslash",
            R"(for="foo\\bar")",
            "", Nothing(), "", R"(foo\bar)", "");
    }

    Y_UNIT_TEST(OptionalWhitespace) {
        AssertParsed(
            "ows",
            "for=192.0.2.43; host=\"example.com:8080\"; proto=https ; by=203.0.113.43",
            "example.com", ui16{8080}, "https", "192.0.2.43", "203.0.113.43");
        AssertParsed(
            "ows_around_equals",
            "host = example.com ; proto = HTTP",
            "example.com", Nothing(), "http", "", "");
    }

    Y_UNIT_TEST(InvalidHostSkipped) {
        AssertParsed(
            "path",
            "host=evil.com/phishing;host=example.com",
            "example.com", Nothing(), "", "", "");
        AssertParsed(
            "query",
            "host=evil.com?next=1",
            "", Nothing(), "", "", "");
        AssertParsed(
            "fragment",
            "host=evil.com#x",
            "", Nothing(), "", "", "");
        AssertParsed(
            "invalid_then_valid",
            "host=evil.com/phishing, host=example.com:8080",
            "example.com", ui16{8080}, "", "", "");
    }

    Y_UNIT_TEST(InvalidPortDropped) {
        AssertParsed(
            "non_numeric_port",
            "host=example.com:abc",
            "example.com", Nothing(), "", "", "");
        AssertParsed(
            "port_zero",
            "host=\"example.com:0\"",
            "example.com", Nothing(), "", "", "");
        AssertParsed(
            "port_overflow",
            "host=example.com:65536",
            "example.com", Nothing(), "", "", "");
    }

    Y_UNIT_TEST(ProtoNotOnlyHttp) {
        AssertParsed(
            "ftp",
            "proto=ftp;host=example.com",
            "example.com", Nothing(), "ftp", "", "");
        AssertParsed(
            "quoted_mixed",
            "proto=\"HTTP\"",
            "", Nothing(), "http", "", "");
    }

    Y_UNIT_TEST(ExtensionsIgnored) {
        AssertParsed(
            "secret",
            "for=192.0.2.43;secret=foo;host=example.com;proto=https",
            "example.com", Nothing(), "https", "192.0.2.43", "");
    }

    Y_UNIT_TEST(MalformedPairsSkipped) {
        AssertParsed(
            "missing_value",
            "host=;proto=https;host=example.com",
            "example.com", Nothing(), "https", "", "");
        AssertParsed(
            "token_without_equals",
            "host example.com;proto=https",
            "", Nothing(), "https", "", "");
        AssertParsed(
            "unclosed_quote",
            "host=\"example.com;proto=https",
            "", Nothing(), "", "", "");
        AssertParsed(
            "empty_quoted",
            "for=\"\";host=example.com",
            "example.com", Nothing(), "", "", "");
        AssertParsed(
            "name_without_equals_at_eof",
            "host=example.com;proto",
            "example.com", Nothing(), "", "", "");
    }

    Y_UNIT_TEST(InvalidTokenNameSkipped) {
        // forwarded-pair = token "=" value; token is 1*tchar (RFC 7230).
        AssertParsed(
            "at_then_valid",
            "@foo=bar;host=example.com",
            "example.com", Nothing(), "", "", "");
        AssertParsed(
            "between_list_elements",
            "for=192.0.2.43, [not-a-token], host=example.com",
            "example.com", Nothing(), "", "192.0.2.43", "");
        AssertParsed(
            "no_list_delimiter",
            ":::not-a-pair",
            "", Nothing(), "", "", "");
    }

    Y_UNIT_TEST(DanglingQuotedPair) {
        // RFC 7230 quoted-pair is "\" CHAR; a trailing backslash is invalid.
        AssertParsed(
            "backslash_at_eof",
            "for=\"abc\\",
            "", Nothing(), "", "", "");
        AssertParsed(
            "backslash_at_eof_then_host",
            "for=\"abc\\;host=example.com",
            "", Nothing(), "", "", "");
    }

    Y_UNIT_TEST(Rfc75ExampleUsage) {
        AssertParsed(
            "second_proxy",
            "for=192.0.2.43, for=198.51.100.17;by=203.0.113.60;proto=http;host=example.com",
            "example.com", Nothing(), "http", "192.0.2.43", "203.0.113.60");
    }

    Y_UNIT_TEST(ByAndObfuscatedPort) {
        AssertParsed(
            "by_obfuscated",
            "by=\"_hidden\"",
            "", Nothing(), "", "", "_hidden");
        AssertParsed(
            "by_unknown",
            "by=unknown",
            "", Nothing(), "", "", "unknown");
        AssertParsed(
            "by_ipv6_port",
            "by=\"[2001:db8:cafe::17]:47011\"",
            "", Nothing(), "", "", "[2001:db8:cafe::17]:47011");
        AssertParsed(
            "obfport",
            "for=\"_hidden:_obf1\"",
            "", Nothing(), "", "_hidden:_obf1", "");
    }

    Y_UNIT_TEST(Ipv4Host) {
        AssertParsed(
            "v4",
            "host=192.0.2.1",
            "192.0.2.1", Nothing(), "", "", "");
        AssertParsed(
            "v4_port",
            "host=\"192.0.2.1:8080\"",
            "192.0.2.1", ui16{8080}, "", "", "");
    }

    Y_UNIT_TEST(EmptyUriHostRejected) {
        // RFC 7230 Host = uri-host [ ":" port ]; empty uri-host is not a Host.
        AssertParsed(
            "port_only",
            "host=\":8080\";host=example.com",
            "example.com", Nothing(), "", "", "");
    }
}
