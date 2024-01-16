#include "reader.h"
#include "reader_mock.h"

#include <functional>

#include <util/generic/vector.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NChangeMirroring {

using namespace NActors;

class TExampleReaderClientImpl
    : public TReaderClientMock
{
public:
    using TReaderClientMock::TReaderClientMock;

    void PollResult(AIReader::Tag tag, const AIReader::TEvReader::TEvPollResult& result) override {
        Data = result.Data;
        Offset = 0;
        Polled = true;
        TReaderClientMock::PollResult(tag, result);
    }

    bool HasNext() const {
        return Offset < Data.size();
    }

    ui64 GetRemaining() const {
        return Data.size() - Offset;
    }

    ui64 GetGlobalOffset() const {
        return GlobalOffset;
    }

    TRcBuf ReadNext() {
        Y_VERIFY(Offset < Data.size());
        GlobalOffset++;
        return Data[Offset++];
    }

    bool NeedPoll() const {
        return Polled && !Eof() && (Data.size() == Offset);
    }

    bool Eof() const {
        return Polled && (Data.size() == 0);
    }

private:
    TVector<TRcBuf> Data;
    ui64 Offset = 0;
    bool Polled = false;
    ui64 GlobalOffset = 0;
};

class TExampleReaderServerImpl
    : public TReaderServerMock
{
public:
    void Poll(AIReader::Tag tag, const AIReader::TEvReader::TEvPoll::TPtr& request) override {
        if (Offset != Data.size()) {
            Reply(request, PollResult(Data[Offset++]).release());
        } else {
            Reply(request, PollResult({}).release());
        }
        TReaderServerMock::Poll(tag, request);
    }

    void SetData(TVector<TVector<TRcBuf>> data) {
        Data = data;
    }

private:
    TVector<TVector<TRcBuf>> Data;
    ui64 Offset = 0;
};

TVector<TRcBuf> Slice(const TString& str) {
    TVector<TRcBuf> slicedData;
    auto buf = TRcBuf(str);
    // slice data into pieces [1] [2, 3] [4, 5, 6] ...
    for(ui64 i = 1, offset = 0; offset < str.size(); ++i) {
        TRcBuf piece(buf);
        piece.TrimFront(str.size() - offset);
        ui64 size = std::min(i, str.size() - offset);
        piece.TrimBack(size);
        Y_ABORT_UNLESS(piece.size() == i || piece.size() == str.size() - offset);
        offset += i;
        slicedData.push_back(piece);
    }
    return slicedData;
}

Y_UNIT_TEST_SUITE(Reader) {
    Y_UNIT_TEST(TestActorReaderClient) {
        TTestActorRuntime runtime;
        runtime.Initialize(NKikimr::TAppPrepare().Unwrap());
        const auto edge = runtime.AllocateEdgeActor(0);

        TString result;

        auto* readerClient = new TExampleReaderClientImpl(AIReader::TReaderActorHandle{edge});
        readerClient->OnBootstrap = [&](auto&) {
            readerClient->BecomeStateWork();
            readerClient->ActorOps().Send(readerClient->ReaderActor(), readerClient->Poll().release());
        };
        readerClient->OnPollResult = [&](auto&, const AIReader::TEvReader::TEvPollResult&) {
            while (!readerClient->Eof()) {
                if (readerClient->NeedPoll()) {
                    readerClient->ActorOps().Send(readerClient->ReaderActor(), readerClient->Poll().release());
                    return;
                }

                while (readerClient->HasNext()) {
                    TRcBuf piece = readerClient->ReadNext();
                    result += TString(piece.Data(), piece.size());
                }
            }

            readerClient->ActorOps().Send(edge, new TEvents::TEvWakeup());
        };
        runtime.Register(readerClient);

        TAutoPtr<IEventHandle> handle;
        TString expectedResult;

        TString data1 = "Lorem ipsum dolor sit amet,"
            " consectetur adipiscing elit, sed do eiusmod tempor"
            " incididunt ut labore et dolore magna aliqua.";
        expectedResult += data1;
        auto slicedData1 = Slice(data1);

        runtime.GrabEdgeEventRethrow<AIReader::TEvReader::TEvPoll>(handle);
        runtime.Send(new IEventHandle(
                            handle->Sender,
                            handle->Recipient,
                            new AIReader::TEvReader::TEvPollResult(slicedData1),
                            0, // flags
                            handle->Cookie
                        ), 0);

        TString data2 = "Another non-empty string";
        expectedResult += data2;
        auto slicedData2 = Slice(data2);

        runtime.GrabEdgeEventRethrow<AIReader::TEvReader::TEvPoll>(handle);
        runtime.Send(new IEventHandle(
                            handle->Sender,
                            handle->Recipient,
                            new AIReader::TEvReader::TEvPollResult(slicedData2),
                            0, // flags
                            handle->Cookie
                        ), 0);

        runtime.GrabEdgeEventRethrow<AIReader::TEvReader::TEvPoll>(handle);
        runtime.Send(new IEventHandle(
                            handle->Sender,
                            handle->Recipient,
                            new AIReader::TEvReader::TEvPollResult({}),
                            0, // flags
                            handle->Cookie
                        ), 0);

        runtime.GrabEdgeEventRethrow<TEvents::TEvWakeup>(handle);

        UNIT_ASSERT_VALUES_EQUAL(expectedResult, result);
    }

    Y_UNIT_TEST(TestActorReaderServer) {
        TTestActorRuntime runtime;
        runtime.Initialize(NKikimr::TAppPrepare().Unwrap());
        const auto edge = runtime.AllocateEdgeActor(0);

        TString expectedResult;
        TString data1 = "Lorem ipsum dolor sit amet,"
            " consectetur adipiscing elit, sed do eiusmod tempor"
            " incididunt ut labore et dolore magna aliqua.";
        expectedResult += data1;
        auto slicedData1 = Slice(data1);
        TString data2 = "Another non-empty string";
        expectedResult += data2;
        auto slicedData2 = Slice(data2);

        auto* readerServer = new TExampleReaderServerImpl();
        readerServer->OnBootstrap = [&](auto& mock) {
            readerServer->BecomeStateWork();
            mock.ActorOps().Send(edge, new TEvents::TEvWakeup);
        };
        readerServer->SetData({slicedData1, slicedData2});
        auto readerServerId = runtime.Register(readerServer);

        TAutoPtr<IEventHandle> handle;

        runtime.GrabEdgeEventRethrow<TEvents::TEvWakeup>(handle);

        TString result;
        NKikimr::NChangeMirroring::AIReader::TEvReader::TEvPollResult* pollResult = nullptr;
        do {
            runtime.Send(new IEventHandle(
                                readerServerId,
                                edge,
                                new AIReader::TEvReader::TEvPoll(),
                                0, // flags
                                0
                         ), 0);
            pollResult = runtime.GrabEdgeEventRethrow<AIReader::TEvReader::TEvPollResult>(handle);
            for (auto& data : pollResult->Data) {
                result += TString(data.data(), data.size());
            }
        } while(pollResult && pollResult->Data.size() != 0);

        UNIT_ASSERT_VALUES_EQUAL(expectedResult, result);
    }
}

} // namespace NKikimr::NChangeMirroring
