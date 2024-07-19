#include <ydb/core/util/address_classifier.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>

#include <vector>

namespace NKikimr::NAddressClassifier {

struct TSubnet {
    TString Mask;
    TString Label;

    const TString& GetMask() const { return Mask; }
    const TString& GetLabel() const { return Label; }
};

struct TNetData {
    const auto& GetSubnets(const size_t i) const {
        return Subnets[i];
    }

    size_t SubnetsSize() const {
        return Subnets.size();
    }

    void AddSubnet(const TString& mask, const TString& label) {
        Subnets.push_back({mask, label});
    }

private:
    std::vector<TSubnet> Subnets;
};

Y_UNIT_TEST_SUITE(AddressClassifierTest) {
    Y_UNIT_TEST(TestAddressExtraction) {
        UNIT_ASSERT_STRINGS_EQUAL(ExtractAddress("ipv6:[::1]:58100"), "::1");
        UNIT_ASSERT_STRINGS_EQUAL(ExtractAddress("ipv4:[192.168.0.1]:1556"), "192.168.0.1");
        UNIT_ASSERT_STRINGS_EQUAL(ExtractAddress("ipv4:192.168.0.1:1550"), "192.168.0.1");
        UNIT_ASSERT_STRINGS_EQUAL(ExtractAddress("ipv4:192.168.0.1"), "192.168.0.1");
        UNIT_ASSERT_STRINGS_EQUAL(ExtractAddress("192.168.0.1:1550"), "192.168.0.1");
        UNIT_ASSERT_STRINGS_EQUAL(ExtractAddress("192.168.0.1"), "192.168.0.1");
    }

    Y_UNIT_TEST(TestAddressParsing) {
        auto parse = [](const TString& address) {
            TString host;
            ui32 port = 0;
            ParseAddress(address, host, port);
            return std::make_pair(host, port);
        };
        UNIT_ASSERT_VALUES_EQUAL(parse("[::1]:58100"), std::make_pair("::1", 58100));
        UNIT_ASSERT_VALUES_EQUAL(parse("192.168.0.1:1550"), std::make_pair("192.168.0.1", 1550));
        UNIT_ASSERT_VALUES_EQUAL(parse("192.168.0.1"), std::make_pair("192.168.0.1", 0));
        UNIT_ASSERT_VALUES_EQUAL(parse("[2a02:6b8:bf00::]"), std::make_pair("2a02:6b8:bf00::", 0));
        UNIT_ASSERT_VALUES_EQUAL(parse("[2a02:6b8:bf00::]:1551"), std::make_pair("2a02:6b8:bf00::", 1551));
    }

    Y_UNIT_TEST(TestClassfierWithAllIpTypes) {
        TAddressClassifier classifier;

        UNIT_ASSERT(!classifier.AddNetByCidrAndLabel("5....../", 34));
        UNIT_ASSERT(!classifier.AddNetByCidrAndLabel("omglol", 666));
        UNIT_ASSERT(!classifier.AddNetByCidrAndLabel("2a02:6b8:bf00::/xx", 777));

        UNIT_ASSERT(classifier.AddNetByCidrAndLabel("2a02:6b8:bf00::/40", 42));
        UNIT_ASSERT(classifier.AddNetByCidrAndLabel("5.45.196.0/24", 42));
        UNIT_ASSERT(classifier.AddNetByCidrAndLabel("5.45.217.0/24", 24));
        UNIT_ASSERT(classifier.AddNetByCidrAndLabel("2a0d:d6c0::/29", 8));
        UNIT_ASSERT(classifier.AddNetByCidrAndLabel("127.0.0.0/8", 10));
        UNIT_ASSERT(classifier.AddNetByCidrAndLabel("::1/128", 10));

        const THashMap<TString, std::pair<bool, size_t>> tests = {
            {"2a02:6b8:bf00::", {true, 42}},
            {"2a02:6b8:af00::", TAddressClassifier::UnknownAddressClass},

            {"5.45.196.0", {true, 42}},
            {"5.45.196.1", {true, 42}},
            {"5.45.196.5", {true, 42}},
            {"5.45.195.0", TAddressClassifier::UnknownAddressClass},

            {"5.45.217.0", {true, 24}},
            {"5.45.217.192", {true, 24}},
            {"5.45.215.192", TAddressClassifier::UnknownAddressClass},
            {"5.44.217.192", TAddressClassifier::UnknownAddressClass},

            {"2a0d:d6c0::", {true, 8}},
            {"2a0d:d6c0:bbbb:bbbb:bbbb:bbbb:bbbb:bbbb", {true, 8}},
            {"2a0d:d7c0:bbbb:bbbb:bbbb:bbbb:bbbb:bbbb", TAddressClassifier::UnknownAddressClass},

            {"wtflol", TAddressClassifier::UnknownAddressClass},

            {"127.255.255.255", {true, 10}},
            {"::1", {true, 10}},
        };

        for (const auto& test : tests) {
            UNIT_ASSERT_EQUAL_C(test.second, classifier.ClassifyAddress(test.first), TString("Failed at ") << test.first);
        }
    }

    const std::vector<TString> labels = { "local6", "local4",  "some6", "some4" };

    Y_UNIT_TEST(TestLabeledClassifier) {
        TAddressClassifier classifier;

        UNIT_ASSERT(classifier.AddNetByCidrAndLabel("::1/128", 0));
        UNIT_ASSERT(classifier.AddNetByCidrAndLabel("127.0.0.0/8", 1));
        UNIT_ASSERT(classifier.AddNetByCidrAndLabel("2a02:6b8:bf00::/40", 2));
        UNIT_ASSERT(classifier.AddNetByCidrAndLabel("5.45.217.0/24", 3));
        UNIT_ASSERT(!classifier.AddNetByCidrAndLabel("test", 3));

        auto labeledClassifier = TLabeledAddressClassifier::MakeLabeledAddressClassifier(std::move(classifier), std::vector<TString>(labels));
        UNIT_ASSERT(!labeledClassifier->ClassifyAddress("2a02:6b8:af00::"));
        UNIT_ASSERT(!labeledClassifier->ClassifyAddress("lolwut"));

        UNIT_ASSERT_STRINGS_EQUAL(*labeledClassifier->ClassifyAddress("127.255.255.255"), "local4");
        UNIT_ASSERT_STRINGS_EQUAL(*labeledClassifier->ClassifyAddress("::1"), "local6");
        UNIT_ASSERT_STRINGS_EQUAL(*labeledClassifier->ClassifyAddress("5.45.217.192"), "some4");
        UNIT_ASSERT_STRINGS_EQUAL(*labeledClassifier->ClassifyAddress("2a02:6b8:bf00::"), "some6");

        UNIT_ASSERT_EQUAL(labeledClassifier->GetLabels(), labels);
    }

    Y_UNIT_TEST(TestLabeledClassifierFromNetData) {
        TAddressClassifier classifier;

        TNetData netData;

        netData.AddSubnet("::1/128", labels[0]);
        netData.AddSubnet("127.0.0.0/8", labels[1]);
        netData.AddSubnet("2a02:6b8:bf00::/40", labels[2]);
        netData.AddSubnet("5.45.217.0/24", labels[3]);
        netData.AddSubnet("2a0d:d6c0::/29", labels[2]);

        auto labeledAddressClassifier = BuildLabeledAddressClassifierFromNetData(netData);
        UNIT_ASSERT(labeledAddressClassifier);
        UNIT_ASSERT_STRINGS_EQUAL(*labeledAddressClassifier->ClassifyAddress("2a0d:d6c0::"), labels[2]);
        UNIT_ASSERT_STRINGS_EQUAL(*labeledAddressClassifier->ClassifyAddress("2a02:6b8:bf00::"), labels[2]);
        UNIT_ASSERT_STRINGS_EQUAL(*labeledAddressClassifier->ClassifyAddress("::1"), labels[0]);
        UNIT_ASSERT_EQUAL(labeledAddressClassifier->GetLabels(), labels);

        netData.AddSubnet("invalid subnet", "some label");
        UNIT_ASSERT(!BuildLabeledAddressClassifierFromNetData(netData));
    }
}

} // namespace NKikimr::NAddressClassifier
