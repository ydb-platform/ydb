#include "http_proxy.h"
#include "http.h"
#include <library/cpp/testing/unittest/registar.h>

namespace NHttp {

Y_UNIT_TEST_SUITE(HttpProxyHelpers) {
    Y_UNIT_TEST(TestIsIPv6) {
        UNIT_ASSERT(IsIPv6("::1"));
        UNIT_ASSERT(IsIPv6("2001:0db8:85a3:0000:0000:8a2e:0370:7334"));
        UNIT_ASSERT(IsIPv6("2001:db8:85a3::8a2e:370:7334"));
        // Dual format is not valid now
        // UNIT_ASSERT(IsIPv6("::ffff:192.0.2.1"));
        
        UNIT_ASSERT(!IsIPv6("192.168.1.1"));
        UNIT_ASSERT(!IsIPv6("not-an-ip"));
        UNIT_ASSERT(!IsIPv6(""));
        // Wrong number of colons
        // UNIT_ASSERT(!IsIPv6(":::"));
    }

    Y_UNIT_TEST(TestIsIPv4) {
        UNIT_ASSERT(IsIPv4("192.168.1.1"));
        UNIT_ASSERT(IsIPv4("127.0.0.1"));
        UNIT_ASSERT(IsIPv4("255.255.255.255"));
        UNIT_ASSERT(IsIPv4("0.0.0.0"));
        
        UNIT_ASSERT(!IsIPv4("::1"));
        UNIT_ASSERT(!IsIPv4("192.168.1"));
        UNIT_ASSERT(!IsIPv4("192.168.1.1.1"));
        // Wrong octet
        // UNIT_ASSERT(!IsIPv4("256.1.1.1"));
        UNIT_ASSERT(!IsIPv4("not-an-ip"));
        UNIT_ASSERT(!IsIPv4(""));
    }

    void TestIpPort(const TString& address,
                    const TString& expectedHostname,
                    const TIpPort expectedPort) {
        TString hostname = "";
        TIpPort port = 0;
        CrackAddress(address, hostname, port);
        UNIT_ASSERT_EQUAL(expectedHostname, hostname);
        UNIT_ASSERT_EQUAL(expectedPort, port);
    }

    // TODO(vlad-serikov): I don't want to cement buggy behavior here.
    // So uncomment wrong tests and add new ones
    // in https://github.com/ydb-platform/ydb/issues/37129 to fix the contract
    Y_UNIT_TEST(TestCrackAddressIPv4Format) {
        // Without port part
        TestIpPort("ipv4:192.168.1.1", "192.168.1.1", 0);
        // TestIpPort("ipv4:", "", 0); // TODO(vlad-serikov);
        // TestIpPort("ipv4:not-valid3", "not-valid3", 0);
        // TestIpPort("ipv4:101.100.1.1.4", "101.100.1.1.4", 0);
        // TestIpPort("ipv4:101.100.:1.5", "101.100.:1.5", 0);

        // With port part
        TestIpPort("ipv4:101.29.8.1:6", "101.29.8.1", 6);
        TestIpPort("ipv4:192.168.1.3:65535", "192.168.1.3", 65535);
        // TestIpPort("ipv4::7", ":7", 0);
        // TestIpPort("ipv4:127.0.0.8:", "127.0.0.8:", 0);
        // TestIpPort("ipv4:not-valid:9", "not-valid", 9);
        // TestIpPort("ipv4:[192.168.1.1]:10", "[192.168.1.1]", 10);
        // TestIpPort("ipv4:192.168.1.1:not-valid", "192.168.1.1:not-valid", 0);
        // TestIpPort("ipv4:192.168.1.2:-1", "192.168.1.2:-1", 0);
        // TestIpPort("ipv4:192.168.1.4:65536", "192.168.1.4", 0);
    }

    // TODO(vlad-serikov): I don't want to cement buggy behavior here.
    // So uncomment wrong tests and add new ones
    // in https://github.com/ydb-platform/ydb/issues/37129 to fix the contract
    Y_UNIT_TEST(TestCrackAddressIPv6Format) {
        // Without port part
        TestIpPort("ipv6:::1", "::1", 0);
        TestIpPort("ipv6:2a07::ff:2", "2a07::ff:2", 0);
        TestIpPort("ipv6:2a07::ff:1:3", "2a07::ff:1:3", 0);
        // TestIpPort("ipv6:deadbeef4", "deadbeef4", 0);
        // TestIpPort("ipv6:[2a07::ff:1:105]", "[2a07::ff:1:105]", 0);
        // TestIpPort("ipv6:][2a07::ff:1:106", "][2a07::ff:1:106", 0);

        // With port part
        TestIpPort("ipv6:[::1]:7", "::1", 7);
        TestIpPort("ipv6:[2a07::ff:1]:8", "2a07::ff:1", 8);
        TestIpPort("ipv6:[2a00::0abf:8978]:65535", "2a00::0abf:8978", 65535);
        // TestIpPort("ipv6:[2a00::0abf]:8977]:9", "2a00::0abf]:8977", 9);
        // TestIpPort("ipv6:2a00::[0abf:8977]:10", "2a00::[0abf:8977]:10", 0);
        // TestIpPort("ipv6:[2a00::0abf[:8977:11", "[2a00::0abf[:8977:11", 0);
        // TestIpPort("ipv6:2a00::0abf:8977]:12", "2a00::0abf:8977]:12", 0);
        // TestIpPort("ipv6:2a00::0abf]:8977]:13", "2a00::0abf]:8977]:13", 0);
        // TestIpPort("ipv6:[2a00::0abf]:8977]:14", "2a00::0abf]:8977", 14);
        // TestIpPort("ipv6:2a00::0abf]:8977[:15", "2a00::0abf]:8977[:15", 0);
        // TestIpPort("ipv6:[not-valid:2a07::ff:1]:16", "not-valid:2a07::ff:1", 16);
        // TestIpPort("ipv6:[2a00::0abf]:8977]:17", "2a00::0abf]:8977", 17);
        // TestIpPort("ipv6:[2a00:[:0abf:8977]:18", "2a00:[:0abf:8977", 18);
        // TestIpPort("ipv6:[2a00::0abf:8976]:not-valid", "[2a00::0abf:8976]:not-valid", 0);
        // TestIpPort("ipv6:[2a00::0abf:8977]:-1", "[2a00::0abf:8977]:-1", 0);
        // TestIpPort("ipv6:[2a00::0abf:8979]:65536", "2a00::0abf:8979", 0);
    }

    Y_UNIT_TEST(TestCrackAddressLegacyFormat) {
        // Without port part
        TestIpPort("::1", "::1", 0);
        TestIpPort("2a02:6b8:c02:1410:0:5a59:eb1e:fe7a", "2a02:6b8:c02:1410:0:5a59:eb1e:fe7a", 0);
        TestIpPort("192.168.0.1", "192.168.0.1", 0);
        TestIpPort("not-valid", "not-valid", 0); // TODO(vlad-serikov): check wrong parsing

        // With port part
        TestIpPort("example.com:12345", "example.com", 12345); // TODO(vlad-serikov): check wrong parsing
        TestIpPort("127.0.0.1:12346", "127.0.0.1", 12346);
        TestIpPort("[::1]:23457", "::1", 23457);
        TestIpPort("[2a07::ff:1]:23456", "2a07::ff:1", 23456);
        TestIpPort("[2a02:6b8:c02:1410:0:5a59:eb1e:fe7a]:3456", "2a02:6b8:c02:1410:0:5a59:eb1e:fe7a", 3456);

    }

    Y_UNIT_TEST(TestUrlHandlerGetHandler) {
        TUrlHandler handler;
        
        TActorId actor1 = TActorId(1, "TestActor1");
        TActorId actor2 = TActorId(2, "TestActor2");
        handler.RegisterHandler("/api/v1", actor1);
        handler.RegisterHandler("/api/v1/users", actor2);
        
        UNIT_ASSERT_EQUAL(handler.GetHandler("/api/v1"), actor1);
        UNIT_ASSERT_EQUAL(handler.GetHandler("/api/v1/users"), actor2);
        
        UNIT_ASSERT_EQUAL(handler.GetHandler("/api/v1/"), actor1);
        UNIT_ASSERT_EQUAL(handler.GetHandler("/api/v1/users/123"), actor2);
        UNIT_ASSERT_EQUAL(handler.GetHandler("/api/v1/users/123/profile"), actor2);
        
        UNIT_ASSERT(!handler.GetHandler("/api/v2"));
        UNIT_ASSERT(!handler.GetHandler("/other"));
        UNIT_ASSERT(!handler.GetHandler(""));
    }

    Y_UNIT_TEST(TestTrimBegin) {
        UNIT_ASSERT_EQUAL(TrimBegin(TStringBuf("   test"), ' '), TStringBuf("test"));
        UNIT_ASSERT_EQUAL(TrimBegin(TStringBuf("test"), ' '), TStringBuf("test"));
        UNIT_ASSERT_EQUAL(TrimBegin(TStringBuf(""), ' '), TStringBuf(""));
        UNIT_ASSERT_EQUAL(TrimBegin(TStringBuf("///test"), '/'), TStringBuf("test"));
        UNIT_ASSERT_EQUAL(TrimBegin(TStringBuf("test  "), ' '), TStringBuf("test  "));
    }

    Y_UNIT_TEST(TestTrimEnd) {
        UNIT_ASSERT_EQUAL(TrimEnd(TStringBuf("test   "), ' '), TStringBuf("test"));
        UNIT_ASSERT_EQUAL(TrimEnd(TStringBuf("test"), ' '), TStringBuf("test"));
        UNIT_ASSERT_EQUAL(TrimEnd(TStringBuf(""), ' '), TStringBuf(""));
        UNIT_ASSERT_EQUAL(TrimEnd(TStringBuf("test///"), '/'), TStringBuf("test"));
        UNIT_ASSERT_EQUAL(TrimEnd(TStringBuf("  test"), ' '), TStringBuf("  test"));


        TString testStr = "test   ";
        TrimEnd(testStr, ' ');
        UNIT_ASSERT_EQUAL(testStr, "test");
        
        testStr = "test///";
        TrimEnd(testStr, '/');
        UNIT_ASSERT_EQUAL(testStr, "test");

        testStr = "  test";
        TrimEnd(testStr, ' ');
        UNIT_ASSERT_EQUAL(testStr, "  test");
    }

    Y_UNIT_TEST(TestTrim) {
        UNIT_ASSERT_EQUAL(Trim(TStringBuf("   test   "), ' '), TStringBuf("test"));
        UNIT_ASSERT_EQUAL(Trim(TStringBuf("test"), ' '), TStringBuf("test"));
        UNIT_ASSERT_EQUAL(Trim(TStringBuf(""), ' '), TStringBuf(""));
        UNIT_ASSERT_EQUAL(Trim(TStringBuf("///test///"), '/'), TStringBuf("test"));
    }
    
    Y_UNIT_TEST(TestToHex) {
        UNIT_ASSERT_EQUAL(ToHex(0), "0");
        UNIT_ASSERT_EQUAL(ToHex(10), "a");
        UNIT_ASSERT_EQUAL(ToHex(255), "ff");
        UNIT_ASSERT_EQUAL(ToHex(1000), "3e8");
    }

    Y_UNIT_TEST(TestIsReadableContent) {
        UNIT_ASSERT(IsReadableContent("text/html"));
        UNIT_ASSERT(IsReadableContent("text/plain"));
        UNIT_ASSERT(IsReadableContent("application/json"));
        UNIT_ASSERT(IsReadableContent("application/x-www-form-urlencoded"));
        UNIT_ASSERT(IsReadableContent("text/html; charset=utf-8"));
        UNIT_ASSERT(!IsReadableContent("application/octet-stream"));
        UNIT_ASSERT(!IsReadableContent("image/png"));
    }

    Y_UNIT_TEST(TestIsValidMethod) {
        UNIT_ASSERT(IsValidMethod("GET"));
        UNIT_ASSERT(IsValidMethod("POST"));
        UNIT_ASSERT(IsValidMethod("PUT"));
        UNIT_ASSERT(!IsValidMethod("GET\x01"));
        UNIT_ASSERT(!IsValidMethod(""));
    }

    Y_UNIT_TEST(TestIsValidURL) {
        UNIT_ASSERT(IsValidURL("/test"));
        UNIT_ASSERT(IsValidURL("/api/v1/users"));
        UNIT_ASSERT(IsValidURL("/test?param=value"));
        UNIT_ASSERT(!IsValidURL("/test\x80"));
        UNIT_ASSERT(!IsValidURL(""));
    }

    Y_UNIT_TEST(TestIsValidProtocol) {
        UNIT_ASSERT(IsValidProtocol("HTTP"));
        UNIT_ASSERT(IsValidProtocol("HTTPS"));
        UNIT_ASSERT(!IsValidProtocol("HTTP1"));
        UNIT_ASSERT(!IsValidProtocol("Http"));
        UNIT_ASSERT(!IsValidProtocol(""));
    }

    Y_UNIT_TEST(TestIsValidVersion) {
        UNIT_ASSERT(IsValidVersion("1.1"));
        UNIT_ASSERT(IsValidVersion("2.0"));
        UNIT_ASSERT(IsValidVersion("1"));
        UNIT_ASSERT(!IsValidVersion("1.1a"));
        UNIT_ASSERT(!IsValidVersion(""));
    }

    Y_UNIT_TEST(TestIsValidStatus) {
        UNIT_ASSERT(IsValidStatus("200"));
        UNIT_ASSERT(IsValidStatus("404"));
        UNIT_ASSERT(IsValidStatus("500"));
        UNIT_ASSERT(!IsValidStatus("200a"));
        UNIT_ASSERT(!IsValidStatus(""));
    }

    Y_UNIT_TEST(TestIsValidMessage) {
        UNIT_ASSERT(IsValidMessage("OK"));
        UNIT_ASSERT(IsValidMessage("Not Found"));
        UNIT_ASSERT(IsValidMessage(""));
        UNIT_ASSERT(!IsValidMessage("Bad\x01Message"));
    }

    Y_UNIT_TEST(TestIsValidHeaderData) {
        UNIT_ASSERT(IsValidHeaderData("text/html"));
        UNIT_ASSERT(IsValidHeaderData("application/json"));
        UNIT_ASSERT(IsValidHeaderData(""));
        UNIT_ASSERT(!IsValidHeaderData("Bad\x01Header"));
    }

    Y_UNIT_TEST(TestCrackURL) {
        TStringBuf scheme, host, uri;
        UNIT_ASSERT(CrackURL("http://example.com/path", scheme, host, uri));
        UNIT_ASSERT_EQUAL(scheme, "http");
        UNIT_ASSERT_EQUAL(host, "example.com");
        UNIT_ASSERT_EQUAL(uri, "/path");
        
        scheme = host = uri = "";
        UNIT_ASSERT(CrackURL("http://example.com", scheme, host, uri));
        UNIT_ASSERT_EQUAL(scheme, "http");
        UNIT_ASSERT_EQUAL(host, "example.com");
        UNIT_ASSERT_EQUAL(uri, "");
        
        scheme = host = uri = "";
        UNIT_ASSERT(CrackURL("https://example.com/api?v=1", scheme, host, uri));
        UNIT_ASSERT_EQUAL(scheme, "https");
        UNIT_ASSERT_EQUAL(host, "example.com");
        UNIT_ASSERT_EQUAL(uri, "/api?v=1");
    }
}

} // namespace NHttp
