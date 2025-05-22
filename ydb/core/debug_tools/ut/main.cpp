#include <library/cpp/testing/unittest/registar.h>
#include <util/random/random.h>

#include <ydb/core/debug_tools/operation_log.h>

#include <deque>
#include <thread>

Y_UNIT_TEST_SUITE(OperationLog) {

    TString RandomData(ui32 size) {
        TString data = "";
        for (ui32 i = 0; i < size; ++i) {
            data += 'a' + RandomNumber<ui32>() % ('z' - 'a' + 1);
        }
        return std::move(data);
    }

    template<ui32 Size>
    void Test() {
        constexpr ui32 dataSize = 10;
        NKikimr::TOperationLog<Size> log;
        std::deque<TString> mock;
        ui32 size = 0;
        for (ui32 i = 0; i < Size * 10; i++) {
            UNIT_ASSERT_VALUES_EQUAL(log.Size(), mock.size());
            for (ui32 i = 0; i < log.Size(); ++i) {
                auto record = log.BorrowByIdx(i);
                UNIT_ASSERT_VALUES_EQUAL(*record, mock[i]);
                log.ReturnBorrowedRecord(record);
                // no concurrent access, so it's safe to dereference pointer
            }

            auto data = RandomData(dataSize);
            auto data1 = std::make_unique<TString>(data);
            mock.push_front(data);
            log.AddRecord(data1);
            if (size < Size) {
                size++;
            } else {
                mock.pop_back();
            }
        }
        // Cerr << log.ToString() << Endl;
    }

    Y_UNIT_TEST(Size1) {
        Test<1>();
    }

    Y_UNIT_TEST(Size8) {
        Test<8>();
    }

    Y_UNIT_TEST(Size29) {
        Test<29>();
    }

    Y_UNIT_TEST(Size1000) {
        Test<1000>();
    }

    Y_UNIT_TEST(ConcurrentWrites) {
        const ui32 logSize = 8;
        NKikimr::TOperationLog<logSize> log;

        const ui32 writesNum = 100'000;
        const ui32 readsNum = 100'000;
        const ui32 writersNum = 10;

        std::atomic<ui32> recordIdx = 0;
        std::vector<std::thread> writers;

        for (ui32 i = 0; i < writersNum; ++i) {
            writers.emplace_back([&]() {
                for (ui32 ctr = 0; ctr < writesNum; ++ctr) {
                    auto newRecord = std::make_unique<TString>(ToString(recordIdx.fetch_add(1)));
                    log.AddRecord(newRecord);
                }
            });
        }

        std::thread reader([&]() {
            for (ui32 i = 0; i < readsNum; ++i) {
                ui32 logIdx = RandomNumber<ui32>() % logSize;
                if (logIdx < log.Size()) {
                    NKikimr::TOperationLog<logSize>::BorrowedRecord record = log.BorrowByIdx(logIdx);
                    if (record) {
                        ui32 recordNum = IntFromString<ui32, 10>(*record);
                        Y_ABORT_UNLESS(recordNum <= recordIdx.load());
                    }
                    log.ReturnBorrowedRecord(record);
                }
            }
        });

        for (auto& thread : writers) {
            thread.join();
        }
        reader.join();
    }
}
