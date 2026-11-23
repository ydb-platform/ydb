#include <ydb/library/actors/core/log.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/stream/file.h>

#include <thread>

using namespace NActors;

namespace {
    class TCallbackLogBackend: public TLogBackend {
    public:
        explicit TCallbackLogBackend(std::function<void(const TLogRecord&)> write)
            : Write(std::move(write))
        {}

        void WriteData(const TLogRecord& record) override {
            Write(record);
        }

        void ReopenLog() override {}

    private:
        const std::function<void(const TLogRecord&)> Write;
    };
}

Y_UNIT_TEST_SUITE(TLogContextContractTest) {
    Y_UNIT_TEST(NestedScopesInheritOverrideAndRestoreComponent) {
        UNIT_ASSERT_VALUES_EQUAL(TLogContextGuard::GetCurrentComponent(17), 17);
        UNIT_ASSERT_VALUES_EQUAL(TLogRecordConstructor().GetResult(), "");
        {
            TLogContextGuard outer(TLogContextBuilder::Build(1)("request", 42));
            UNIT_ASSERT_VALUES_EQUAL(TLogContextGuard::GetCurrentComponent(17), 1);
            {
                TLogContextGuard inherited(TLogContextBuilder::Build()("stage", "read"));
                UNIT_ASSERT_VALUES_EQUAL(TLogContextGuard::GetCurrentComponent(17), 1);
                UNIT_ASSERT_VALUES_EQUAL(TLogRecordConstructor()("result", "ok").GetResult(),
                    "request=42;stage=read;result=ok;");
                {
                    // Component zero is an explicit override, not an absent component.
                    TLogContextGuard overridden(TLogContextBuilder::Build(0)("attempt", 2));
                    UNIT_ASSERT_VALUES_EQUAL(TLogContextGuard::GetCurrentComponent(17), 0);
                    UNIT_ASSERT_VALUES_EQUAL(TLogRecordConstructor().GetResult(),
                        "request=42;stage=read;attempt=2;");
                }
                UNIT_ASSERT_VALUES_EQUAL(TLogContextGuard::GetCurrentComponent(17), 1);
                UNIT_ASSERT_VALUES_EQUAL(TLogRecordConstructor().GetResult(), "request=42;stage=read;");
            }
            UNIT_ASSERT_VALUES_EQUAL(TLogRecordConstructor().GetResult(), "request=42;");
        }
        UNIT_ASSERT_VALUES_EQUAL(TLogContextGuard::GetCurrentComponent(17), 17);
        UNIT_ASSERT_VALUES_EQUAL(TLogRecordConstructor().GetResult(), "");
    }

    Y_UNIT_TEST(ExceptionUnwindingRestoresOuterContext) {
        TLogContextGuard outer(TLogContextBuilder::Build()("request", 42));
        UNIT_ASSERT_VALUES_EQUAL(TLogContextGuard::GetCurrentComponent(17), 17);
        UNIT_ASSERT_EXCEPTION([&] {
            TLogContextGuard inner(TLogContextBuilder::Build(3)("stage", "failing"));
            UNIT_ASSERT_VALUES_EQUAL(TLogContextGuard::GetCurrentComponent(17), 3);
            UNIT_ASSERT_VALUES_EQUAL(TLogRecordConstructor().GetResult(), "request=42;stage=failing;");
            ythrow yexception() << "failure";
        }(), yexception);
        UNIT_ASSERT_VALUES_EQUAL(TLogContextGuard::GetCurrentComponent(17), 17);
        UNIT_ASSERT_VALUES_EQUAL(TLogRecordConstructor().GetResult(), "request=42;");
    }

    Y_UNIT_TEST(ContextIsIsolatedBetweenThreads) {
        TLogContextGuard outer(TLogContextBuilder::Build(1)("request", "main"));
        TString before;
        TString during;
        TString after;
        int beforeComponent = -1;
        int duringComponent = -1;
        int afterComponent = -1;
        std::thread worker([&] {
            before = TLogRecordConstructor().GetResult();
            beforeComponent = TLogContextGuard::GetCurrentComponent(17);
            {
                TLogContextGuard context(TLogContextBuilder::Build(2)("request", "worker"));
                during = TLogRecordConstructor().GetResult();
                duringComponent = TLogContextGuard::GetCurrentComponent(17);
            }
            after = TLogRecordConstructor().GetResult();
            afterComponent = TLogContextGuard::GetCurrentComponent(17);
        });
        worker.join();
        UNIT_ASSERT_VALUES_EQUAL(before, "");
        UNIT_ASSERT_VALUES_EQUAL(beforeComponent, 17);
        UNIT_ASSERT_VALUES_EQUAL(during, "request=worker;");
        UNIT_ASSERT_VALUES_EQUAL(duringComponent, 2);
        UNIT_ASSERT_VALUES_EQUAL(after, "");
        UNIT_ASSERT_VALUES_EQUAL(afterComponent, 17);
        UNIT_ASSERT_VALUES_EQUAL(TLogRecordConstructor().GetResult(), "request=main;");
        UNIT_ASSERT_VALUES_EQUAL(TLogContextGuard::GetCurrentComponent(17), 1);
    }
}

Y_UNIT_TEST_SUITE(TLogBackendContractTest) {
    Y_UNIT_TEST(FileBackendAppendsExactlyOneNewlineAndPreservesRecordLength) {
        TTempDir temp;
        const TString path = temp.Path().Child("actor.log").GetPath();
        {
            auto backend = CreateFileBackend(path);
            backend->WriteData(TLogRecord(TLOG_INFO, "first", 5));
            backend->WriteData(TLogRecord(TLOG_WARNING, "second\n", 7));
            backend->WriteData(TLogRecord(TLOG_DEBUG, "", 0));
            const char binary[] = {'a', '\0', 'b', 'x'};
            backend->WriteData(TLogRecord(TLOG_ERR, binary, 3));
        }
        const char expected[] = "first\nsecond\na\0b\n";
        UNIT_ASSERT_VALUES_EQUAL(TFileInput(path).ReadAll(), TString(expected, sizeof(expected) - 1));
        // A newly created backend must append rather than truncate existing logs.
        {
            auto backend = CreateFileBackend(path);
            backend->WriteData(TLogRecord(TLOG_INFO, "last", 4));
        }
        UNIT_ASSERT_VALUES_EQUAL(TFileInput(path).ReadAll(), TString(expected, sizeof(expected) - 1) + "last\n");
    }

    Y_UNIT_TEST(CompositeBackendForwardsRecordAndMetadataInOrder) {
        TVector<int> order;
        const TString message("a\0b", 3);
        const TLogRecord::TMetaFlags flags{{"database", "test"}};
        TVector<TAutoPtr<TLogBackend>> backends;
        for (int i = 0; i < 2; ++i) {
            backends.emplace_back(new TCallbackLogBackend([&, i](const TLogRecord& record) {
                order.push_back(i);
                UNIT_ASSERT_VALUES_EQUAL(record.Priority, TLOG_WARNING);
                UNIT_ASSERT_VALUES_EQUAL(TString(record.Data, record.Len), message);
                UNIT_ASSERT_VALUES_EQUAL(record.MetaFlags, flags);
            }));
        }
        auto backend = CreateCompositeLogBackend(std::move(backends));
        backend->WriteData(TLogRecord(TLOG_WARNING, message.data(), message.size(), flags));
        UNIT_ASSERT_VALUES_EQUAL(order, (TVector<int>{0, 1}));
    }

    Y_UNIT_TEST(CompositeBackendPropagatesFailureWithoutWritingLaterBackends) {
        TVector<int> order;
        TVector<TAutoPtr<TLogBackend>> backends;
        backends.emplace_back(new TCallbackLogBackend([&](const TLogRecord&) { order.push_back(0); }));
        backends.emplace_back(new TCallbackLogBackend([&](const TLogRecord&) {
            order.push_back(1);
            ythrow yexception() << "backend failure";
        }));
        backends.emplace_back(new TCallbackLogBackend([&](const TLogRecord&) { order.push_back(2); }));
        auto backend = CreateCompositeLogBackend(std::move(backends));
        UNIT_ASSERT_EXCEPTION_CONTAINS(backend->WriteData(TLogRecord(TLOG_INFO, "message", 7)),
            yexception, "backend failure");
        UNIT_ASSERT_VALUES_EQUAL(order, (TVector<int>{0, 1}));
    }
}
