#include <ydb/core/ymq/base/counters.h>

#include <ydb/core/base/path.h>
#include <ydb/core/protos/config.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/subst.h>

namespace NKikimr::NSQS {

TString CountersString(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& root) {
    TStringStream ss;
    root->OutputPlainText(ss);
    return ss.Str();
}

std::vector<std::pair<TString, TString>> ParseCounterPath(const TString& path) {
    const auto pathComponents = SplitPath(path);
    std::vector<std::pair<TString, TString>> ret(pathComponents.size());
    for (size_t i = 0; i < pathComponents.size(); ++i) {
        const size_t pos = pathComponents[i].find('=');
        if (pos == TString::npos) {
            ret[i].first = DEFAULT_COUNTER_NAME;
            ret[i].second = pathComponents[i];
        } else {
            ret[i].first = pathComponents[i].substr(0, pos);
            ret[i].second = pathComponents[i].substr(pos + 1);
        }
    }
    return ret;
}

// Gets counters by path.
// Path can be:
// "counters" - simple counter.
// "path/counter" - simple path.
// "user=my_user/queue=my_queue/TransactionCount" - with nondefault names.
void AssertCounterValue(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters, const TString& path, i64 expectedValue) {
    const auto pathComponents = ParseCounterPath(path);
    UNIT_ASSERT_GT(pathComponents.size(), 0);
    TIntrusivePtr<::NMonitoring::TDynamicCounters> parent = counters;
    for (size_t i = 0; i < pathComponents.size() - 1; ++i) {
        parent = parent->FindSubgroup(pathComponents[i].first, pathComponents[i].second);
        UNIT_ASSERT_C(parent, "Subgroup \"" << pathComponents[i].first << "=" << pathComponents[i].second << "\" was not found. Level: " << i);
    }
    auto counter = parent->GetNamedCounter(pathComponents.back().first, pathComponents.back().second);
    UNIT_ASSERT_VALUES_EQUAL_C(counter->Val(), expectedValue,
                               "Name: \"" << path
                               << "\", Expected value: " << expectedValue
                               << ", Actual value: " << counter->Val()
                               << "\nCounters string:\n" << CountersString(counters));
}

void AssertCounterValues(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters, const std::vector<std::pair<TString, i64>>& expectedValues) {
    for (const auto& [path, expectedValue] : expectedValues) {
        AssertCounterValue(counters, path, expectedValue);
    }
}

Y_UNIT_TEST_SUITE(LazyCounterTest) {
    Y_UNIT_TEST(LazyCounterTest) {
        TIntrusivePtr<::NMonitoring::TDynamicCounters> root = new ::NMonitoring::TDynamicCounters();
        TLazyCachedCounter counter, counter2, counter3;
        counter.Init(root, ELifetime::Persistent, EValueType::Absolute, "A", ELaziness::OnDemand);
        counter3.Init(root, ELifetime::Persistent, EValueType::Absolute, "B", ELaziness::OnStart);
        UNIT_ASSERT_STRINGS_EQUAL(CountersString(root), "sensor=B: 0\n");
        ++*counter;
        ++*counter2;
        UNIT_ASSERT_STRINGS_EQUAL(CountersString(root), "sensor=A: 1\nsensor=B: 0\n");
    }

    void AggregationTest(ELaziness lazy) {
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters = new ::NMonitoring::TDynamicCounters();
        TLazyCachedCounter parent;
        parent.Init(counters, ELifetime::Persistent, EValueType::Absolute, "parent", lazy);

        TLazyCachedCounter child1;
        child1.Init(counters, ELifetime::Persistent, EValueType::Absolute, "child1", lazy);
        child1.SetAggregatedParent(&parent);

        TLazyCachedCounter child2;
        child2.Init(counters, ELifetime::Persistent, EValueType::Absolute, "child2", lazy);
        child2.SetAggregatedParent(&child1);

        TLazyCachedCounter child3;
        child3.Init(counters, ELifetime::Persistent, EValueType::Absolute, "child3", lazy);
        child3.SetAggregatedParent(&child1);

        ++*child3;
        AssertCounterValues(counters,
                            {
                                { "parent", 1 },
                                { "child1", 1 },
                                { "child3", 1 },
                            });
        if (lazy == ELaziness::OnDemand) {
            UNIT_ASSERT(CountersString(counters).find("child2") == TString::npos);
        } else {
            UNIT_ASSERT_STRING_CONTAINS(CountersString(counters), "child2");
            AssertCounterValue(counters, "child2", 0);
        }

        child3->Inc();
        AssertCounterValues(counters,
                            {
                                { "parent", 2 },
                                { "child1", 2 },
                                { "child3", 2 },
                            });

        *child2 = 10;
        AssertCounterValues(counters,
                            {
                                { "parent", 12 },
                                { "child1", 12 },
                                { "child2", 10 },
                                { "child3", 2 },
                            });

        --*child2;
        AssertCounterValues(counters,
                            {
                                { "parent", 11 },
                                { "child1", 11 },
                                { "child2", 9 },
                                { "child3", 2 },
                            });

        *child2 += -3;
        AssertCounterValues(counters,
                            {
                                { "parent", 8 },
                                { "child1", 8 },
                                { "child2", 6 },
                                { "child3", 2 },
                            });

        ++*child1;
        AssertCounterValues(counters,
                            {
                                { "parent", 9 },
                                { "child1", 9 },
                                { "child2", 6 },
                                { "child3", 2 },
                            });

        child3->Add(5);
        AssertCounterValues(counters,
                            {
                                { "parent", 14 },
                                { "child1", 14 },
                                { "child2", 6 },
                                { "child3", 7 },
                            });
    }

    Y_UNIT_TEST(AggregationLazyTest) {
        AggregationTest(ELaziness::OnDemand);
    }

    Y_UNIT_TEST(AggregationNonLazyTest) {
        AggregationTest(ELaziness::OnStart);
    }

    Y_UNIT_TEST(HistogramAggregationTest) {
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters = new ::NMonitoring::TDynamicCounters();
        const NMonitoring::TBucketBounds buckets = { 1, 3, 5 };
        TLazyCachedHistogram parent;
        parent.Init(counters, ELifetime::Persistent, buckets, "parent", ELaziness::OnStart);

        TLazyCachedHistogram child;
        child.Init(counters, ELifetime::Persistent, buckets, "child", ELaziness::OnStart);
        child.SetAggregatedParent(&parent);

        child->Collect(2);
        parent->Collect(10);

        const auto parentSnapshot = counters->GetHistogram("parent", NMonitoring::ExplicitHistogram(buckets))->Snapshot();
        const auto childSnapshot = counters->GetHistogram("child", NMonitoring::ExplicitHistogram(buckets))->Snapshot();

        UNIT_ASSERT_VALUES_EQUAL(parentSnapshot->Count(), 4);
        UNIT_ASSERT_VALUES_EQUAL(childSnapshot->Count(), 4);

        UNIT_ASSERT_VALUES_EQUAL(parentSnapshot->Value(0), 0);
        UNIT_ASSERT_VALUES_EQUAL(parentSnapshot->Value(1), 1);
        UNIT_ASSERT_VALUES_EQUAL(parentSnapshot->Value(2), 0);
        UNIT_ASSERT_VALUES_EQUAL(parentSnapshot->Value(3), 1);

        UNIT_ASSERT_VALUES_EQUAL(childSnapshot->Value(0), 0);
        UNIT_ASSERT_VALUES_EQUAL(childSnapshot->Value(1), 1);
        UNIT_ASSERT_VALUES_EQUAL(childSnapshot->Value(2), 0);
        UNIT_ASSERT_VALUES_EQUAL(childSnapshot->Value(3), 0);
    }
}

Y_UNIT_TEST_SUITE(UserCountersTest) {
#define ASSERT_STR_COUPLE_CONTAINS(string1, string2, what) \
        UNIT_ASSERT_STRING_CONTAINS(string1, what); \
        UNIT_ASSERT_STRING_CONTAINS(string2, what);

#define ASSERT_FIRST_OF_COUPLE_CONTAINS(string1, string2, what) \
        UNIT_ASSERT_STRING_CONTAINS(string1, what); \
        UNIT_ASSERT(!string2.Contains(what));

#define ASSERT_STR_COUPLE_DONT_CONTAIN(string1, string2, what) \
        UNIT_ASSERT(string1.find(what) == TString::npos); \
        UNIT_ASSERT(string2.find(what) == TString::npos);

#define ASSERT_USER_PRESENT(user) \
    ASSERT_FIRST_OF_COUPLE_CONTAINS(CountersString(core), CountersString(ymqCounters), TString("user=") + user);  \
    ASSERT_FIRST_OF_COUPLE_CONTAINS(CountersString(ymqCounters), CountersString(core), TString("cloud=") + user);

#define ASSERT_USER_ABSENT(user) \
    ASSERT_STR_COUPLE_DONT_CONTAIN(CountersString(core), CountersString(ymqCounters), TString("user=") + user);  \
    ASSERT_STR_COUPLE_DONT_CONTAIN(CountersString(core), CountersString(ymqCounters), TString("cloud=") + user);

    Y_UNIT_TEST(DisableCountersTest) {
        NKikimrConfig::TSqsConfig cfg;
        cfg.SetCreateLazyCounters(false);
        TIntrusivePtr<::NMonitoring::TDynamicCounters> core = new ::NMonitoring::TDynamicCounters();
        TIntrusivePtr<::NMonitoring::TDynamicCounters> ymqCounters = new ::NMonitoring::TDynamicCounters();
        TIntrusivePtr<TUserCounters> user1 = new TUserCounters(cfg, core, ymqCounters, nullptr, "user1", nullptr);
        TIntrusivePtr<TUserCounters> user2 = new TUserCounters(cfg, core, ymqCounters, nullptr, "user2", nullptr);
        ASSERT_USER_PRESENT("user1");
        ASSERT_USER_PRESENT("user2");

        user1->DisableCounters(false);
        ASSERT_USER_PRESENT("user1");

        user1->DisableCounters(true);
        TString sqsCntrText = CountersString(core);
        TString ymqCntrText = CountersString(ymqCounters);
        ASSERT_USER_ABSENT("user1");
        ASSERT_USER_PRESENT("user2");

        // again
        user1->DisableCounters(true);
        sqsCntrText = CountersString(core);
        ymqCntrText = CountersString(ymqCounters);
        ASSERT_USER_ABSENT("user1");
        ASSERT_USER_PRESENT("user2");

        *user1->RequestTimeouts = 1;
        *user2->RequestTimeouts = 2;

        // return back
        user1->DisableCounters(false);
        sqsCntrText = CountersString(core);
        ymqCntrText = CountersString(ymqCounters);
        ASSERT_USER_PRESENT("user1");
        ASSERT_USER_PRESENT("user2");
        ASSERT_STR_COUPLE_DONT_CONTAIN(sqsCntrText,ymqCntrText, "RequestTimeouts: 1");
        ASSERT_FIRST_OF_COUPLE_CONTAINS(sqsCntrText, ymqCntrText, "RequestTimeouts: 2");
        ASSERT_FIRST_OF_COUPLE_CONTAINS(sqsCntrText, ymqCntrText, "RequestTimeouts: 2");
        ASSERT_FIRST_OF_COUPLE_CONTAINS(sqsCntrText, ymqCntrText, "RequestTimeouts: 0");

        // again
        *user1->RequestTimeouts = 3;

        user1->DisableCounters(false);
        sqsCntrText = CountersString(core);
        ymqCntrText = CountersString(ymqCounters);
        ASSERT_USER_PRESENT("user1");
        ASSERT_USER_PRESENT("user2");
        ASSERT_FIRST_OF_COUPLE_CONTAINS(sqsCntrText, ymqCntrText, "RequestTimeouts: 2");
        ASSERT_FIRST_OF_COUPLE_CONTAINS(sqsCntrText, ymqCntrText, "RequestTimeouts: 3");
    }

    Y_UNIT_TEST(RemoveUserCountersTest) {
        NKikimrConfig::TSqsConfig cfg;
        cfg.SetCreateLazyCounters(false);
        TIntrusivePtr<::NMonitoring::TDynamicCounters> core = new ::NMonitoring::TDynamicCounters();
        TIntrusivePtr<::NMonitoring::TDynamicCounters> ymqCounters = new ::NMonitoring::TDynamicCounters();

        TIntrusivePtr<TUserCounters> user = new TUserCounters(cfg, core, ymqCounters, nullptr, "my_user", nullptr);
        ASSERT_USER_PRESENT("my_user");

        user->RemoveCounters();
        ASSERT_USER_ABSENT("my_user");
    }

    Y_UNIT_TEST(CountersAggregationTest) {
        NKikimrConfig::TSqsConfig cfg;
        cfg.SetCreateLazyCounters(false);
        TIntrusivePtr<::NMonitoring::TDynamicCounters> core = new ::NMonitoring::TDynamicCounters();
        TIntrusivePtr<::NMonitoring::TDynamicCounters> ymqCounters = new ::NMonitoring::TDynamicCounters();
        TIntrusivePtr<TUserCounters> total = new TUserCounters(cfg, core, ymqCounters, nullptr, TOTAL_COUNTER_LABEL, nullptr);
        total->ShowDetailedCounters(TInstant::Max());
        TIntrusivePtr<TUserCounters> user = new TUserCounters(cfg, core, ymqCounters, nullptr, "my_user", total);
        UNIT_ASSERT_STRING_CONTAINS(CountersString(core), "user=my_user");
        UNIT_ASSERT_STRING_CONTAINS(CountersString(core), "user=total");

        UNIT_ASSERT_VALUES_EQUAL(user->GetDetailedCounters(), total->GetDetailedCounters());
        UNIT_ASSERT_VALUES_EQUAL(user->GetTransactionCounters(), total->GetTransactionCounters());

        user->ShowDetailedCounters(TInstant::Max());
        UNIT_ASSERT_VALUES_UNEQUAL(user->GetDetailedCounters(), total->GetDetailedCounters());
        UNIT_ASSERT_VALUES_UNEQUAL(user->GetTransactionCounters(), total->GetTransactionCounters());

        // Try different types of member counters: detailed/usual/arrays/api statuses.
        ++*user->RequestTimeouts;
        ++*user->SqsActionCounters[EAction::CreateQueue].Errors;
        ++*user->GetDetailedCounters()->CreateAccountOnTheFly_Success;
        user->GetDetailedCounters()->APIStatuses.AddOk(3);
        user->GetDetailedCounters()->APIStatuses.AddError("AccessDeniedException", 2);
        user->GetDetailedCounters()->APIStatuses.AddError("AccessDeniedException1", 4); // unknown
        ++*user->GetTransactionCounters()->TransactionsCount;
        ++*user->GetTransactionCounters()->QueryTypeCounters[EQueryId::WRITE_MESSAGE_ID].TransactionsFailed;

        ++*user->YmqActionCounters[EAction::CreateQueue].Errors;

        AssertCounterValues(core,
                            {
                                { "user=total/RequestTimeouts", 1 },
                                { "user=my_user/RequestTimeouts", 1 },
                                { "user=total/CreateQueue_Errors", 1 },
                                { "user=my_user/CreateQueue_Errors", 1 },
                                { "user=total/queue=total/SendMessage_Count", 0 },
                                { "user=my_user/queue=total/SendMessage_Count", 0 },
                                { "user=total/CreateAccountOnTheFly_Success", 1 },
                                { "user=my_user/CreateAccountOnTheFly_Success", 1 },
                                { "user=total/StatusesByType/status_code=OK", 3 },
                                { "user=my_user/StatusesByType/status_code=OK", 3 },
                                { "user=total/StatusesByType/status_code=AccessDeniedException", 2 },
                                { "user=my_user/StatusesByType/status_code=AccessDeniedException", 2 },
                                { "user=total/StatusesByType/status_code=Unknown", 4 },
                                { "user=my_user/StatusesByType/status_code=Unknown", 4 },
                                { "user=total/queue=total/TransactionsCount", 1 },
                                { "user=my_user/queue=total/TransactionsCount", 1 },
                                { "user=total/queue=total/TransactionsFailedByType/query_type=WRITE_MESSAGE_ID", 1 },
                                { "user=my_user/queue=total/TransactionsFailedByType/query_type=WRITE_MESSAGE_ID", 1 },
                            });

//    auto queue = user->CreateQueueCounters("my_queue", "folder", true);

        AssertCounterValues(ymqCounters,
                            {
                                    { "cloud=my_user/method=create_queue/name=api.http.errors_count_per_second", 1 },
                            }
        );
    }
}

Y_UNIT_TEST_SUITE(QueueCountersTest) {
    Y_UNIT_TEST(InsertCountersTest) {
        NKikimrConfig::TSqsConfig cfg;
        cfg.SetCreateLazyCounters(false);
        TIntrusivePtr<::NMonitoring::TDynamicCounters> core = new ::NMonitoring::TDynamicCounters();
        TIntrusivePtr<::NMonitoring::TDynamicCounters> ymqCounters = new ::NMonitoring::TDynamicCounters();
        TIntrusivePtr<TUserCounters> user = new TUserCounters(cfg, core, ymqCounters, nullptr, "my_user", nullptr);
        ASSERT_USER_PRESENT("my_user");

        auto queue = user->CreateQueueCounters("my_queue", "folder", false);
        ASSERT_STR_COUPLE_DONT_CONTAIN(CountersString(core), CountersString(ymqCounters), "my_queue");

        queue->InsertCounters();
        ASSERT_STR_COUPLE_CONTAINS(CountersString(core), CountersString(ymqCounters), "my_queue");

        ++*queue->RequestTimeouts;
        auto sqsCntrText = CountersString(core);
        auto ymqCntrText = CountersString(ymqCounters);
        ASSERT_FIRST_OF_COUPLE_CONTAINS(sqsCntrText, ymqCntrText, "RequestTimeouts: 1");

        ++*queue->request_timeouts_count_per_second;
        sqsCntrText = CountersString(core);
        ymqCntrText = CountersString(ymqCounters);
        ASSERT_FIRST_OF_COUPLE_CONTAINS(ymqCntrText, sqsCntrText, "request_timeouts_count_per_second: 1");

        // Second time:
        queue->InsertCounters();
        ASSERT_STR_COUPLE_CONTAINS(CountersString(core), CountersString(ymqCounters), "my_queue");
    }

    void RemoveQueueCountersTest(bool leader, const TString& folderId) {
        NKikimrConfig::TSqsConfig cfg;
        cfg.SetCreateLazyCounters(false);
        TIntrusivePtr<::NMonitoring::TDynamicCounters> core = new ::NMonitoring::TDynamicCounters();
        TIntrusivePtr<::NMonitoring::TDynamicCounters> ymqCounters = new ::NMonitoring::TDynamicCounters();
        TIntrusivePtr<TUserCounters> user = new TUserCounters(cfg, core, ymqCounters, nullptr, "my_user", nullptr);
        TIntrusivePtr<TQueueCounters> queue = user->CreateQueueCounters("my_queue", folderId, true);
        if (leader) {
            queue = queue->GetCountersForLeaderNode();
        }
        ASSERT_STR_COUPLE_CONTAINS(CountersString(core), CountersString(ymqCounters), "queue=my_queue");
        queue->RemoveCounters();
        ASSERT_STR_COUPLE_DONT_CONTAIN(CountersString(core), CountersString(ymqCounters), "queue=my_queue");
    }

    Y_UNIT_TEST(RemoveQueueCountersNonLeaderWithoutFolderTest) {
        RemoveQueueCountersTest(false, "");
    }

    Y_UNIT_TEST(RemoveQueueCountersLeaderWithoutFolderTest) {
        RemoveQueueCountersTest(true, "");
    }

    Y_UNIT_TEST(RemoveQueueCountersNonLeaderWithFolderTest) {
        RemoveQueueCountersTest(false, "my_folder");
    }

    Y_UNIT_TEST(RemoveQueueCountersLeaderWithFolderTest) {
        RemoveQueueCountersTest(true, "my_folder");
    }

    void CountersAggregationTest(bool cloudMode = false) {
        NKikimrConfig::TSqsConfig cfg;
        cfg.SetYandexCloudMode(cloudMode);
        cfg.SetCreateLazyCounters(false);
        TIntrusivePtr<::NMonitoring::TDynamicCounters> core = new ::NMonitoring::TDynamicCounters();
        TIntrusivePtr<::NMonitoring::TDynamicCounters> ymqCounters = new ::NMonitoring::TDynamicCounters();
        TIntrusivePtr<TUserCounters> total = new TUserCounters(cfg, core, ymqCounters, nullptr, TOTAL_COUNTER_LABEL, nullptr);
        total->ShowDetailedCounters(TInstant::Max());
        TIntrusivePtr<TUserCounters> user = new TUserCounters(cfg, core, ymqCounters, nullptr, "my_user", total);
        TIntrusivePtr<TQueueCounters> queue = user->CreateQueueCounters("my_queue", cloudMode ? "my_folder" : "", true)->GetCountersForLeaderNode();
        UNIT_ASSERT_STRING_CONTAINS(CountersString(core), "queue=my_queue");

        queue->ShowDetailedCounters(TInstant::Max());

        // Try different types of member counters: detailed/usual/arrays/api statuses.
        ++*queue->MessagesPurged;
        ++*queue->purged_count_per_second;
        ++*queue->SqsActionCounters[EAction::SendMessageBatch].Errors;
        ++*queue->YmqActionCounters[EAction::SendMessageBatch].Errors;
        ++*queue->GetDetailedCounters()->ReceiveMessage_KeysInvalidated;
        ++*queue->GetTransactionCounters()->TransactionRetryTimeouts;
        ++*queue->GetTransactionCounters()->QueryTypeCounters[EQueryId::DELETE_MESSAGE_ID].TransactionsCount;

        // path fixer
        auto x = [cloudMode](const TString& path) -> TString {
            TString ret = path;
            if (cloudMode) {
                size_t replacements = SubstGlobal(ret, "/queue=total", "/folder=total/queue=total");
                replacements += SubstGlobal(ret, "/queue=my_queue", "/folder=my_folder/queue=my_queue");
                UNIT_ASSERT_LE(replacements, 1);
            }
            return ret;
        };

        AssertCounterValues(core,
                            {
                                { x("user=total/queue=total/MessagesPurged"), 1 },
                                { x("user=my_user/queue=total/MessagesPurged"), 1 },
                                { x("user=my_user/queue=my_queue/MessagesPurged"), 1 },

                                { x("user=total/queue=total/SendMessage_Errors"), 1 },
                                { x("user=my_user/queue=total/SendMessage_Errors"), 1 },
                                { x("user=my_user/queue=my_queue/SendMessage_Errors"), 1 },

                                { x("user=total/queue=total/ReceiveMessage_KeysInvalidated"), 1 },
                                { x("user=my_user/queue=total/ReceiveMessage_KeysInvalidated"), 1 },
                                { x("user=my_user/queue=my_queue/ReceiveMessage_KeysInvalidated"), 1 },

                                { x("user=total/queue=total/TransactionRetryTimeouts"), 1 },
                                { x("user=my_user/queue=total/TransactionRetryTimeouts"), 1 },
                                { x("user=my_user/queue=my_queue/TransactionRetryTimeouts"), 1 },

                                { x("user=total/queue=total/TransactionsByType/query_type=DELETE_MESSAGE_ID"), 1 },
                                { x("user=my_user/queue=total/TransactionsByType/query_type=DELETE_MESSAGE_ID"), 1 },
                                { x("user=my_user/queue=my_queue/TransactionsByType/query_type=DELETE_MESSAGE_ID"), 1 },
                            });

        AssertCounterValues(ymqCounters,
                            {
                                { x("cloud=my_user/queue=my_queue/name=queue.messages.purged_count_per_second"), 1 },

                                { x("cloud=my_user/queue=my_queue/method=send_message_batch/name=api.http.errors_count_per_second"), 1 },
                            });
    }

    Y_UNIT_TEST(CountersAggregationTest) {
        CountersAggregationTest();
    }

    Y_UNIT_TEST(CountersAggregationCloudTest) {
        CountersAggregationTest(true);
    }
}

Y_UNIT_TEST_SUITE(HttpCountersTest) {
    Y_UNIT_TEST(CountersAggregationTest) {
        NKikimrConfig::TSqsConfig cfg;
        cfg.SetCreateLazyCounters(false);
        TIntrusivePtr<::NMonitoring::TDynamicCounters> root = new ::NMonitoring::TDynamicCounters();
        TIntrusivePtr<THttpCounters> http = new THttpCounters(cfg, root);
        TIntrusivePtr<THttpUserCounters> user = http->GetUserCounters("my_user");

        *http->ConnectionsCount = 33;
        ++*http->GetUserCounters("my_user")->RequestExceptions;
        ++*http->GetUserCounters("your_user")->ActionCounters[EAction::SetQueueAttributes].Requests;

        AssertCounterValues(root,
                    {
                        { "ConnectionsCount", 33 },
                        { "user=total/RequestExceptions", 1 },
                        { "user=my_user/RequestExceptions", 1 },
                        { "user=total/SetQueueAttributesRequest", 1 },
                        { "user=your_user/SetQueueAttributesRequest", 1 },
                    });
    }
}

Y_UNIT_TEST_SUITE(MeteringCountersTest) {
    Y_UNIT_TEST(CountersAggregationTest) {
        NKikimrConfig::TSqsConfig config;
        config.SetCreateLazyCounters(false);
        TIntrusivePtr<::NMonitoring::TDynamicCounters> metering = new ::NMonitoring::TDynamicCounters();
        TIntrusivePtr<TMeteringCounters> counters = new TMeteringCounters(config, metering, {"inet", "yandex", "unknown", "cloud"});
        *counters->ClassifierRequestsResults["inet"] = 33;
        *counters->ClassifierRequestsResults["yandex"] = 42;
        *counters->ClassifierRequestsResults["unknown"] = 113;
        *counters->IdleClassifierRequestsResults["cloud"] = 128;

        AssertCounterValues(metering,
                    {
                        { "ConnectionsCount", 0 },
                        { "ClassifierRequests_cloud", 0},
                        { "ClassifierRequests_inet", 33},
                        { "ClassifierRequests_unknown", 113},
                        { "ClassifierRequests_yandex", 42},
                        { "IdleClassifierRequests_cloud", 128},
                        { "IdleClassifierRequests_inet", 0},
                        { "IdleClassifierRequests_unknown", 0},
                        { "IdleClassifierRequests_yandex", 0},
                        { "RequestExceptions", 0}
                    });
    }
}

} // namespace NKikimr::NSQS
