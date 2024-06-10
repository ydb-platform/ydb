#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/net/connection.h>
#include <yt/yt/core/net/listener.h>
#include <yt/yt/core/net/dialer.h>
#include <yt/yt/core/net/config.h>
#include <yt/yt/core/net/private.h>

#include <yt/yt/core/concurrency/poller.h>
#include <yt/yt/core/concurrency/thread_pool_poller.h>

#include <util/network/pollerimpl.h>

namespace NYT::NNet {
namespace {

using namespace NYT::NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TNetTest
    : public ::testing::Test
{
protected:
    IThreadPoolPollerPtr Poller_;

    void SetUp() override
    {
        Poller_ = CreateThreadPoolPoller(2, "nettest");
    }

    void TearDown() override
    {
        Poller_->Shutdown();
    }

    IDialerPtr CreateDialer()
    {
        return NNet::CreateDialer(
            New<TDialerConfig>(),
            Poller_,
            NetLogger());
    }
};

TEST_F(TNetTest, ReconfigurePoller)
{
    for (int i = 1; i <= 10; ++i) {
        Poller_->Reconfigure(i);
    }
    for (int i = 9; i >= 10; --i) {
        Poller_->Reconfigure(i);
    }
}

TEST_F(TNetTest, CreateConnectionPair)
{
    IConnectionPtr a, b;
    std::tie(a, b) = CreateConnectionPair(Poller_);
}

TEST_F(TNetTest, TransferFourBytes)
{
    IConnectionPtr a, b;
    std::tie(a, b) = CreateConnectionPair(Poller_);

    a->Write(TSharedRef::FromString("ping")).Get();

    auto buffer = TSharedMutableRef::Allocate(10);
    ASSERT_EQ(4u, b->Read(buffer).Get().ValueOrThrow());
    ASSERT_EQ(ToString(buffer.Slice(0, 4)), TString("ping"));
}

TEST_F(TNetTest, TransferFourBytesUsingWriteV)
{
    IConnectionPtr a, b;
    std::tie(a, b) = CreateConnectionPair(Poller_);

    a->WriteV(TSharedRefArray(std::vector<TSharedRef>{
        TSharedRef::FromString("p"),
        TSharedRef::FromString("i"),
        TSharedRef::FromString("n"),
        TSharedRef::FromString("g")
    }, TSharedRefArray::TMoveParts{})).Get().ThrowOnError();

    auto buffer = TSharedMutableRef::Allocate(10);
    ASSERT_EQ(4u, b->Read(buffer).Get().ValueOrThrow());
    ASSERT_EQ(ToString(buffer.Slice(0, 4)), TString("ping"));
}

TEST_F(TNetTest, BigTransfer)
{
// Select-based poller implementation is much slower there.
#if defined(HAVE_EPOLL_POLLER)
    const int N = 1024, K = 256 * 1024;
#else
    const int N = 32, K = 256 * 1024;
#endif

    IConnectionPtr a, b;
    std::tie(a, b) = CreateConnectionPair(Poller_);

    auto sender = BIND([=] {
        auto buffer = TSharedRef::FromString(TString(K, 'f'));
        for (int i = 0; i < N; ++i) {
            WaitFor(a->Write(buffer)).ThrowOnError();
        }

        WaitFor(a->CloseWrite()).ThrowOnError();
    })
        .AsyncVia(Poller_->GetInvoker())
        .Run();

    auto receiver = BIND([=] {
        auto buffer = TSharedMutableRef::Allocate(3 * K);
        ssize_t received = 0;
        while (true) {
            int res = WaitFor(b->Read(buffer)).ValueOrThrow();
            if (res == 0) break;
            received += res;
        }

        EXPECT_EQ(N * K, received);
    })
        .AsyncVia(Poller_->GetInvoker())
        .Run();

    sender.Get().ThrowOnError();
    receiver.Get().ThrowOnError();
}

TEST_F(TNetTest, BidirectionalTransfer)
{
// Select-based poller implementation is much slower there.
#if defined(HAVE_EPOLL_POLLER)
    const int N = 1024, K = 256 * 1024;
#else
    const int N = 32, K = 256 * 1024;
#endif

    IConnectionPtr a, b;
    std::tie(a, b) = CreateConnectionPair(Poller_);

    auto startSender = [&] (IConnectionPtr conn) {
        return BIND([=] {
            auto buffer = TSharedRef::FromString(TString(K, 'f'));
            for (int i = 0; i < N; ++i) {
                WaitFor(conn->Write(buffer)).ThrowOnError();
            }

            WaitFor(conn->CloseWrite()).ThrowOnError();
        })
            .AsyncVia(Poller_->GetInvoker())
            .Run();
    };

    auto startReceiver = [&] (IConnectionPtr conn) {
        return BIND([=] {
            auto buffer = TSharedMutableRef::Allocate(K * 4);
            ssize_t received = 0;
            while (true) {
                int res = WaitFor(conn->Read(buffer)).ValueOrThrow();
                if (res == 0) break;
                received += res;
            }

            EXPECT_EQ(N * K, received);
        })
            .AsyncVia(Poller_->GetInvoker())
            .Run();
    };

    std::vector<TFuture<void>> futures = {
        startSender(a),
        startReceiver(a),
        startSender(b),
        startReceiver(b)
    };

    AllSucceeded(futures).Get().ThrowOnError();
}

class TContinueReadInCaseOfWriteErrorsTest
    : public TNetTest
    , public testing::WithParamInterface<bool>
{ };

TEST_P(TContinueReadInCaseOfWriteErrorsTest, ContinueReadInCaseOfWriteErrors)
{
    IConnectionPtr a, b;
    std::tie(a, b) = CreateConnectionPair(Poller_);

    auto data = TSharedRef::FromString(TString(16 * 1024, 'f'));
    bool gracefulConnectionClose = GetParam();
    // If server closes the connection without reading the entire request,
    // it causes an error 'Connection reset by peer' on client's side right after reading response.
    if (!gracefulConnectionClose) {
        b->Write(data).Get().ThrowOnError();
    }
    a->Write(data).Get().ThrowOnError();
    a->Close().Get().ThrowOnError();

    {
        auto data = TSharedRef::FromString(TString(16 * 1024, 'a'));
        #ifndef _win_
            EXPECT_THROW(b->Write(data).Get().ThrowOnError(), TErrorException);
        #endif
    }

    auto buffer = TSharedMutableRef::Allocate(32 * 1024);
    auto read = b->Read(buffer).Get().ValueOrThrow();

    EXPECT_EQ(data.size(), read);
    ASSERT_EQ(ToString(buffer.Slice(0, 4)), TString("ffff"));
}

INSTANTIATE_TEST_SUITE_P(
    TContinueReadInCaseOfWriteErrorsTest,
    TContinueReadInCaseOfWriteErrorsTest,
    testing::Values(
        false,
        true)
);

TEST_F(TNetTest, StressConcurrentClose)
{
    for (int i = 0; i < 10; i++) {
        IConnectionPtr a, b;
        std::tie(a, b) = CreateConnectionPair(Poller_);

        auto runSender = [&] (IConnectionPtr conn) {
            return BIND([=] {
                auto buffer = TSharedRef::FromString(TString(16 * 1024, 'f'));
                while (true) {
                    WaitFor(conn->Write(buffer)).ThrowOnError();
                }
            })
                .AsyncVia(Poller_->GetInvoker())
                .Run();
        };

        auto runReceiver = [&] (IConnectionPtr conn) {
            return BIND([=] {
                auto buffer = TSharedMutableRef::Allocate(16 * 1024);
                while (true) {
                    int res = WaitFor(conn->Read(buffer)).ValueOrThrow();
                    if (res == 0) break;
                }
            })
                .AsyncVia(Poller_->GetInvoker())
                .Run();
        };

        YT_UNUSED_FUTURE(runSender(a));
        YT_UNUSED_FUTURE(runReceiver(a));
        YT_UNUSED_FUTURE(runSender(b));
        YT_UNUSED_FUTURE(runReceiver(b));

        Sleep(TDuration::MilliSeconds(10));
        a->Close().Get().ThrowOnError();
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TNetTest, Bind)
{
    // TODO(aleexfi): test is broken.
    #ifdef _win_
        return;
    #endif
    BIND([&] {
        auto address = TNetworkAddress::CreateIPv6Loopback(0);
        auto listener = CreateListener(address, Poller_, Poller_);

    // On Windows option SO_REUSEADDR (which is enabled by CreateTcpSocket) behaves exactly like combination
    // SO_REUSEADDR | SO_REUSEPORT, so consecutive binds on the same address will succeed.
    // https://stackoverflow.com/questions/14388706/how-do-so-reuseaddr-and-so-reuseport-differ
    #ifdef _linux_
        EXPECT_THROW(CreateListener(listener->GetAddress(), Poller_, Poller_), TErrorException);
    #else
        EXPECT_NO_THROW(CreateListener(listener->GetAddress(), Poller_, Poller_));
    #endif
    })
        .AsyncVia(Poller_->GetInvoker())
        .Run()
        .Get()
        .ThrowOnError();
}

TEST_F(TNetTest, DialError)
{
    BIND([&] {
        auto address = TNetworkAddress::CreateIPv6Loopback(4000);
        auto dialer = CreateDialer();
        EXPECT_THROW(dialer->Dial(address).Get().ValueOrThrow(), TErrorException);
    })
        .AsyncVia(Poller_->GetInvoker())
        .Run()
        .Get()
        .ThrowOnError();
}

TEST_F(TNetTest, DialSuccess)
{
    BIND([&] {
        auto address = TNetworkAddress::CreateIPv6Loopback(0);
        auto listener = CreateListener(address, Poller_, Poller_);
        auto dialer = CreateDialer();

        auto futureDial = dialer->Dial(listener->GetAddress());
        auto futureAccept = listener->Accept();

        WaitFor(futureDial).ValueOrThrow();
        WaitFor(futureAccept).ValueOrThrow();
    })
        .AsyncVia(Poller_->GetInvoker())
        .Run()
        .Get()
        .ThrowOnError();
}

TEST_F(TNetTest, ManyDials)
{
    BIND([&] {
        auto address = TNetworkAddress::CreateIPv6Loopback(0);
        auto listener = CreateListener(address, Poller_, Poller_);
        auto dialer = CreateDialer();

        auto futureAccept1 = listener->Accept();
        auto futureAccept2 = listener->Accept();

        auto futureDial1 = dialer->Dial(listener->GetAddress());
        auto futureDial2 = dialer->Dial(listener->GetAddress());

        WaitFor(AllSucceeded(std::vector<TFuture<IConnectionPtr>>{futureDial1, futureDial2, futureAccept1, futureAccept2})).ValueOrThrow();
    })
        .AsyncVia(Poller_->GetInvoker())
        .Run()
        .Get()
        .ThrowOnError();
}

TEST_F(TNetTest, AbandonDial)
{
    BIND([&] {
        auto address = TNetworkAddress::CreateIPv6Loopback(0);
        auto listener = CreateListener(address, Poller_, Poller_);
        auto dialer = CreateDialer();

        YT_UNUSED_FUTURE(dialer->Dial(listener->GetAddress()));
    })
        .AsyncVia(Poller_->GetInvoker())
        .Run()
        .Get()
        .ThrowOnError();
}

TEST_F(TNetTest, AbandonAccept)
{
    BIND([&] {
        auto address = TNetworkAddress::CreateIPv6Loopback(0);
        auto listener = CreateListener(address, Poller_, Poller_);

        YT_UNUSED_FUTURE(listener->Accept());
    })
        .AsyncVia(Poller_->GetInvoker())
        .Run()
        .Get()
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NNet
