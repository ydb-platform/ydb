#include "counters.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NMonitoring;

class TCountersPrinter: public ICountableConsumer {
public:
    TCountersPrinter(IOutputStream* out)
        : Out_(out)
        , Level_(0)
    {
    }

private:
    void OnCounter(
        const TString& labelName, const TString& labelValue,
        const TCounterForPtr* counter) override {
        Indent(Out_, Level_)
            << labelName << ':' << labelValue
            << " = " << counter->Val() << '\n';
    }

    void OnHistogram(
        const TString& labelName, const TString& labelValue,
        IHistogramSnapshotPtr snapshot, bool /*derivative*/) override {
        Indent(Out_, Level_)
            << labelName << ':' << labelValue
            << " = " << *snapshot << '\n';
    }

    void OnGroupBegin(
        const TString& labelName, const TString& labelValue,
        const TDynamicCounters*) override {
        Indent(Out_, Level_++) << labelName << ':' << labelValue << " {\n";
    }

    void OnGroupEnd(
        const TString&, const TString&,
        const TDynamicCounters*) override {
        Indent(Out_, --Level_) << "}\n";
    }

    static IOutputStream& Indent(IOutputStream* out, int level) {
        for (int i = 0; i < level; i++) {
            out->Write("  ");
        }
        return *out;
    }

private:
    IOutputStream* Out_;
    int Level_ = 0;
};

Y_UNIT_TEST_SUITE(TDynamicCountersTest) {
    Y_UNIT_TEST(CountersConsumer) {
        TDynamicCounterPtr rootGroup(new TDynamicCounters());

        auto usersCounter = rootGroup->GetNamedCounter("users", "count");
        *usersCounter = 7;

        auto hostGroup = rootGroup->GetSubgroup("counters", "resources");
        auto cpuCounter = hostGroup->GetNamedCounter("resource", "cpu");
        *cpuCounter = 30;

        auto memGroup = hostGroup->GetSubgroup("resource", "mem");
        auto usedCounter = memGroup->GetCounter("used");
        auto freeCounter = memGroup->GetCounter("free");
        *usedCounter = 100;
        *freeCounter = 28;

        auto netGroup = hostGroup->GetSubgroup("resource", "net");
        auto rxCounter = netGroup->GetCounter("rx", true);
        auto txCounter = netGroup->GetCounter("tx", true);
        *rxCounter = 8;
        *txCounter = 9;

        TStringStream ss;
        TCountersPrinter printer(&ss);
        rootGroup->Accept("root", "counters", printer);

        UNIT_ASSERT_STRINGS_EQUAL(ss.Str(),
                                  "root:counters {\n"
                                  "  counters:resources {\n"
                                  "    resource:cpu = 30\n"
                                  "    resource:mem {\n"
                                  "      sensor:free = 28\n"
                                  "      sensor:used = 100\n"
                                  "    }\n"
                                  "    resource:net {\n"
                                  "      sensor:rx = 8\n"
                                  "      sensor:tx = 9\n"
                                  "    }\n"
                                  "  }\n"
                                  "  users:count = 7\n"
                                  "}\n");
    }

    Y_UNIT_TEST(MergeSubgroup) {
        TDynamicCounterPtr rootGroup(new TDynamicCounters());

        auto sensor1 = rootGroup->GetNamedCounter("sensor", "1");
        *sensor1 = 1;

        auto group1 = rootGroup->GetSubgroup("group", "1");
        auto sensor2 = group1->GetNamedCounter("sensor", "2");
        *sensor2 = 2;

        auto group2 = group1->GetSubgroup("group", "2");
        auto sensor3 = group2->GetNamedCounter("sensor", "3");
        *sensor3 = 3;

        rootGroup->MergeWithSubgroup("group", "1");

        TStringStream ss;
        TCountersPrinter printer(&ss);
        rootGroup->Accept("root", "counters", printer);

        UNIT_ASSERT_STRINGS_EQUAL(ss.Str(),
                                  "root:counters {\n"
                                  "  group:2 {\n"
                                  "    sensor:3 = 3\n"
                                  "  }\n"
                                  "  sensor:1 = 1\n"
                                  "  sensor:2 = 2\n"
                                  "}\n");
    }

    Y_UNIT_TEST(ResetCounters) {
        TDynamicCounterPtr rootGroup(new TDynamicCounters());

        auto sensor1 = rootGroup->GetNamedCounter("sensor", "1");
        *sensor1 = 1;

        auto group1 = rootGroup->GetSubgroup("group", "1");
        auto sensor2 = group1->GetNamedCounter("sensor", "2");
        *sensor2 = 2;

        auto group2 = group1->GetSubgroup("group", "2");
        auto sensor3 = group2->GetNamedCounter("sensor", "3", true);
        *sensor3 = 3;

        rootGroup->ResetCounters(true);

        TStringStream ss1;
        TCountersPrinter printer1(&ss1);
        rootGroup->Accept("root", "counters", printer1);

        UNIT_ASSERT_STRINGS_EQUAL(ss1.Str(),
                                  "root:counters {\n"
                                  "  group:1 {\n"
                                  "    group:2 {\n"
                                  "      sensor:3 = 0\n"
                                  "    }\n"
                                  "    sensor:2 = 2\n"
                                  "  }\n"
                                  "  sensor:1 = 1\n"
                                  "}\n");

        rootGroup->ResetCounters();

        TStringStream ss2;
        TCountersPrinter printer2(&ss2);
        rootGroup->Accept("root", "counters", printer2);

        UNIT_ASSERT_STRINGS_EQUAL(ss2.Str(),
                                  "root:counters {\n"
                                  "  group:1 {\n"
                                  "    group:2 {\n"
                                  "      sensor:3 = 0\n"
                                  "    }\n"
                                  "    sensor:2 = 0\n"
                                  "  }\n"
                                  "  sensor:1 = 0\n"
                                  "}\n");
    }

    Y_UNIT_TEST(RemoveCounter) {
        TDynamicCounterPtr rootGroup(new TDynamicCounters());

        rootGroup->GetNamedCounter("label", "1");
        rootGroup->GetCounter("2");
        rootGroup->GetCounter("3");
        rootGroup->GetSubgroup("group", "1");

        rootGroup->RemoveNamedCounter("label", "1");
        rootGroup->RemoveNamedCounter("label", "5");
        rootGroup->RemoveNamedCounter("group", "1");
        rootGroup->RemoveCounter("2");
        rootGroup->RemoveCounter("5");

        TStringStream ss;
        TCountersPrinter printer(&ss);
        rootGroup->Accept("root", "counters", printer);

        UNIT_ASSERT_STRINGS_EQUAL(ss.Str(),
                                  "root:counters {\n"
                                  "  group:1 {\n"
                                  "  }\n"
                                  "  sensor:3 = 0\n"
                                  "}\n");
    }

    Y_UNIT_TEST(RemoveSubgroup) {
        TDynamicCounterPtr rootGroup(new TDynamicCounters());

        rootGroup->GetSubgroup("group", "1");
        rootGroup->GetSubgroup("group", "2");
        rootGroup->GetCounter("2");

        rootGroup->RemoveSubgroup("group", "1");
        rootGroup->RemoveSubgroup("group", "3");
        rootGroup->RemoveSubgroup("sensor", "2");

        TStringStream ss;
        TCountersPrinter printer(&ss);
        rootGroup->Accept("root", "counters", printer);

        UNIT_ASSERT_STRINGS_EQUAL(ss.Str(),
                                  "root:counters {\n"
                                  "  group:2 {\n"
                                  "  }\n"
                                  "  sensor:2 = 0\n"
                                  "}\n");
    }

    Y_UNIT_TEST(ExpiringCounters) {
        TDynamicCounterPtr rootGroup{new TDynamicCounters()};

        {
            auto c = rootGroup->GetExpiringCounter("foo");
            auto h = rootGroup->GetExpiringHistogram("bar", ExplicitHistogram({1, 42}));
            h->Collect(15);

            TStringStream ss;
            TCountersPrinter printer(&ss);
            rootGroup->Accept("root", "counters", printer);
            UNIT_ASSERT_STRINGS_EQUAL(ss.Str(),
                                      "root:counters {\n"
                                      "  sensor:bar = {1: 0, 42: 1, inf: 0}\n"
                                      "  sensor:foo = 0\n"
                                      "}\n");
        }

        TStringStream ss;
        TCountersPrinter printer(&ss);
        rootGroup->Accept("root", "counters", printer);
        UNIT_ASSERT_STRINGS_EQUAL(ss.Str(),
                                  "root:counters {\n"
                                  "}\n");
    }

    Y_UNIT_TEST(ExpiringCountersDiesAfterRegistry) {
        TDynamicCounters::TCounterPtr ptr;

        {
            TDynamicCounterPtr rootGroup{new TDynamicCounters()};
            ptr = rootGroup->GetExpiringCounter("foo");

            TStringStream ss;
            TCountersPrinter printer(&ss);
            rootGroup->Accept("root", "counters", printer);
            UNIT_ASSERT_STRINGS_EQUAL(ss.Str(),
                                      "root:counters {\n"
                                      "  sensor:foo = 0\n"
                                      "}\n");
        }
    }

    Y_UNIT_TEST(HistogramCounter) {
        TDynamicCounterPtr rootGroup(new TDynamicCounters());

        auto h = rootGroup->GetHistogram("timeMillis", ExponentialHistogram(4, 2));
        for (i64 i = 1; i < 100; i++) {
            h->Collect(i);
        }

        TStringStream ss;
        TCountersPrinter printer(&ss);
        rootGroup->Accept("root", "counters", printer);
        UNIT_ASSERT_STRINGS_EQUAL(ss.Str(),
                                  "root:counters {\n"
                                  "  sensor:timeMillis = {1: 1, 2: 1, 4: 2, inf: 95}\n"
                                  "}\n");
    }

    Y_UNIT_TEST(CounterLookupCounter) {
        TDynamicCounterPtr rootGroup(new TDynamicCounters());
        TDynamicCounters::TCounterPtr lookups = rootGroup->GetCounter("Lookups", true);
        rootGroup->SetLookupCounter(lookups);

        // Create subtree and check that counter is inherited
        TDynamicCounterPtr serviceGroup = rootGroup->GetSubgroup("service", "MyService");
        UNIT_ASSERT_VALUES_EQUAL(lookups->Val(), 1);

        TDynamicCounterPtr subGroup = serviceGroup->GetSubgroup("component", "MyComponent");
        UNIT_ASSERT_VALUES_EQUAL(lookups->Val(), 2);

        auto counter = subGroup->GetNamedCounter("range", "20 msec", true);
        UNIT_ASSERT_VALUES_EQUAL(lookups->Val(), 3);

        auto hist = subGroup->GetHistogram("timeMsec", ExponentialHistogram(4, 2));
        UNIT_ASSERT_VALUES_EQUAL(lookups->Val(), 4);

        // Replace the counter for subGroup
        auto subGroupLookups = rootGroup->GetCounter("LookupsInMyComponent", true);
        UNIT_ASSERT_VALUES_EQUAL(lookups->Val(), 5);
        subGroup->SetLookupCounter(subGroupLookups);
        auto counter2 = subGroup->GetNamedCounter("range", "30 msec", true);
        UNIT_ASSERT_VALUES_EQUAL(subGroupLookups->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(lookups->Val(), 5);
    }

    Y_UNIT_TEST(FindCounters) {
        TDynamicCounterPtr rootGroup(new TDynamicCounters());

        auto counter = rootGroup->FindCounter("counter1");
        UNIT_ASSERT(!counter);
        rootGroup->GetCounter("counter1");
        counter = rootGroup->FindCounter("counter1");
        UNIT_ASSERT(counter);

        counter = rootGroup->FindNamedCounter("name", "counter2");
        UNIT_ASSERT(!counter);
        rootGroup->GetNamedCounter("name", "counter2");
        counter = rootGroup->FindNamedCounter("name", "counter2");
        UNIT_ASSERT(counter);

        auto histogram = rootGroup->FindHistogram("histogram1");
        UNIT_ASSERT(!histogram);
        rootGroup->GetHistogram("histogram1", ExponentialHistogram(4, 2));
        histogram = rootGroup->FindHistogram("histogram1");
        UNIT_ASSERT(histogram);

        histogram = rootGroup->FindNamedHistogram("name", "histogram2");
        UNIT_ASSERT(!histogram);
        rootGroup->GetNamedHistogram("name", "histogram2", ExponentialHistogram(4, 2));
        histogram = rootGroup->FindNamedHistogram("name", "histogram2");
        UNIT_ASSERT(histogram);
    }
}
