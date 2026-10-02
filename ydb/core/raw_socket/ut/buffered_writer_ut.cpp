#include "library/cpp/testing/unittest/registar.h"
#include "ydb/core/raw_socket/sock_impl.h"

namespace NKikimr::NRawSocket {

class TDummySocketDescriptor {
public:
    ssize_t Send(const void*, size_t size) {
        LastSendCalls.push_back(size);

        if (!SendResults) {
            TotalWrittenToSocket += size;
            return size; // pretend everything was written successfully
        } else {
            ui32 index = SendCallIndex++ % SendResults.size();
            ssize_t res = SendResults[index];
            if (res > 0) {
                TotalWrittenToSocket += res;
            }
            return res;
        }
    }

    void ReturnOnSend(const TVector<ssize_t>& results) {
        SendResults = results;
    }

    void VerifyLastSendCalls(const TVector<ssize_t>& expected) {
        UNIT_ASSERT_C(LastSendCalls.size() >= expected.size(), TStringBuilder() << "Verify of send calls failed. Expected " << expected.size() << "calls, but got only  " << LastSendCalls.size() << " calls");
        for (size_t i = 0; i < expected.size(); ++i) {
            ui32 callIndex = LastSendCalls.size() - expected.size() + i;
            UNIT_ASSERT_VALUES_EQUAL(LastSendCalls[callIndex], expected[i]);
        }
    }

    size_t GetTotalWritten() const {
        return TotalWrittenToSocket;
    }

    ui32 GetSendCallsCount() const {
        return LastSendCalls.size();
    }

private:
    size_t TotalWrittenToSocket = 0;
    TVector<ssize_t> SendResults;
    ui32 SendCallIndex = 0;
    TVector<ssize_t> LastSendCalls = {};
};

Y_UNIT_TEST_SUITE(TBufferedWriter) {
     Y_UNIT_TEST(Flush_AfterEAGAIN_ShouldRestartFromSavedPositionInBuffer_1) {
        TDummySocketDescriptor socket;
        TBufferedWriter<TDummySocketDescriptor> writer(&socket, 256);
        TString data(240, 'x');
        writer.write(data.data(), data.size());
        socket.ReturnOnSend({200, -EAGAIN, 40});

        ssize_t res2 = writer.flush();
        socket.VerifyLastSendCalls({240, 40});
        UNIT_ASSERT_VALUES_EQUAL(res2, static_cast<ssize_t>(-EAGAIN));

        ssize_t res3 = writer.flush();
        socket.VerifyLastSendCalls({40});
        UNIT_ASSERT_VALUES_EQUAL(res3, 40);
        UNIT_ASSERT_VALUES_EQUAL(socket.GetTotalWritten(), 240);
    }

     Y_UNIT_TEST(Flush_AfterEAGAIN_ShouldRestartFromSavedPositionInBuffer_2) {
        TDummySocketDescriptor socket;
        TBufferedWriter<TDummySocketDescriptor> writer(&socket, 256);
        socket.ReturnOnSend({256, 240, -EAGAIN, 16});
        TString data(512, 'x');
        writer.write(data.data(), data.size());

        ssize_t res = writer.flush();
        // on first attempt socket will successfuly write full buffer - 256
        // on second attempt socket will write only part of data - 240
        // on third attempt socket will return error - EAGAIN
        socket.VerifyLastSendCalls({256, 256, 16});
        UNIT_ASSERT_VALUES_EQUAL(res, static_cast<ssize_t>(-EAGAIN));

        ssize_t res2 = writer.flush();
        UNIT_ASSERT_VALUES_EQUAL(socket.GetSendCallsCount(), 4);
        // verify the whole chain of calls with the last retry
        socket.VerifyLastSendCalls({256, 256, 16, 16});
        UNIT_ASSERT_VALUES_EQUAL(res2, 16);
        UNIT_ASSERT_VALUES_EQUAL(socket.GetTotalWritten(), 512);
    }
}

} // namespace NKikimr::NRawSocket