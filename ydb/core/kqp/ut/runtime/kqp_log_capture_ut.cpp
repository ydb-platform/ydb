#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <library/cpp/logger/record.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>

#include <array>
#include <atomic>
#include <optional>
#include <thread>
#include <vector>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpCapturedLog) {
    Y_UNIT_TEST(SnapshotAndBackendLifetime) {
        const TString record("first\0record", 12);
        THolder<TLogBackend> first;
        THolder<TLogBackend> second;
        std::optional<TCapturedLog> reader;
        TString snapshot;
        {
            TCapturedLog capture(/* appendNewline */ true);
            first = capture.CreateBackend();
            second = capture.CreateBackend();
            reader = capture;
            UNIT_ASSERT(capture.Snapshot().empty());
            first->WriteData(TLogRecord(TLOG_INFO, record.data(), record.size()));
            snapshot = capture.Snapshot();
            UNIT_ASSERT_VALUES_EQUAL(snapshot, record + "\n");
        }

        // Destroying the original handle/one backend leaves the others usable.
        first.Reset();
        const TString next(65536, 'x');
        second->WriteData(TLogRecord(TLOG_INFO, next.data(), next.size()));
        UNIT_ASSERT_VALUES_EQUAL(reader->Snapshot(), record + "\n" + next + "\n");
        UNIT_ASSERT_VALUES_EQUAL(snapshot, record + "\n");
        // The backend can still write after every public capture handle is gone.
        reader.reset();
        second->WriteData(TLogRecord(TLOG_INFO, next.data(), next.size()));
        second.Reset();
        UNIT_ASSERT_VALUES_EQUAL(snapshot, record + "\n");
    }

    Y_UNIT_TEST(ConcurrentBackendsAndSnapshots) {
        constexpr size_t writers = 3;
        constexpr size_t recordsPerWriter = 1024;
        constexpr size_t recordSize = 513;
        TCapturedLog capture;
        std::atomic<bool> start = false;
        std::atomic<bool> resume = false;
        std::atomic<size_t> ready = 0;
        std::atomic<size_t> finished = 0;
        std::vector<std::thread> threads;
        Y_DEFER {
            start.store(true);
            resume.store(true);
            for (auto& thread : threads) {
                thread.join();
            }
        };

        for (size_t writer = 0; writer < writers; ++writer) {
            threads.emplace_back([&, writer, backend = capture.CreateBackend()] {
                TString record(recordSize, 'a' + writer);
                record[recordSize - 2] = '\0';
                record[recordSize - 1] = '\n';
                while (!start.load()) {
                    std::this_thread::yield();
                }
                for (size_t i = 0; i < recordsPerWriter; ++i) {
                    backend->WriteData(TLogRecord(TLOG_INFO, record.data(), record.size()));
                    if (i == 0) {
                        ++ready;
                        while (!resume.load()) {
                            std::this_thread::yield();
                        }
                    }
                    if (i % 32 == 0) {
                        std::this_thread::yield();
                    }
                }
                ++finished;
            });
        }

        auto validate = [&](const TString& snapshot) {
            UNIT_ASSERT_VALUES_EQUAL(snapshot.size() % recordSize, 0);
            std::array<size_t, writers> counts{};
            for (size_t pos = 0; pos < snapshot.size(); pos += recordSize) {
                const size_t writer = static_cast<unsigned char>(snapshot[pos]) - 'a';
                UNIT_ASSERT(writer < writers);
                for (size_t i = 0; i < recordSize - 2; ++i) {
                    UNIT_ASSERT_VALUES_EQUAL(snapshot[pos + i], 'a' + writer);
                }
                UNIT_ASSERT_VALUES_EQUAL(snapshot[pos + recordSize - 2], '\0');
                UNIT_ASSERT_VALUES_EQUAL(snapshot[pos + recordSize - 1], '\n');
                ++counts[writer];
            }
            return counts;
        };

        start.store(true);
        while (ready.load() != writers) {
            std::this_thread::yield();
        }
        const TString initialSnapshot = capture.Snapshot();
        resume.store(true);
        do {
            // Writers can append/reallocate while we inspect an older snapshot.
            const TString snapshot = capture.Snapshot();
            validate(snapshot);
            std::this_thread::yield();
        } while (finished.load() != writers);

        const auto counts = validate(capture.Snapshot());
        for (size_t count : counts) {
            UNIT_ASSERT_VALUES_EQUAL(count, recordsPerWriter);
        }
        for (size_t count : validate(initialSnapshot)) {
            UNIT_ASSERT_VALUES_EQUAL(count, 1);
        }
    }
}

} // namespace NKikimr::NKqp
