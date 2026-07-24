#include <ydb/core/raw_socket/sock64.h>
#include <ydb/core/raw_socket/sock_impl.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <algorithm>
#include <cerrno>
#include <cstring>

namespace {

class TDummySocketDescriptor {
public:
    ssize_t Send(const void*, size_t size) {
        Calls_.push_back(size);

        if (Results_.empty()) {
            TotalWritten_ += size;
            return size;
        }

        const ssize_t result = Results_[Index_++ % Results_.size()];
        if (result > 0) {
            const ssize_t written = std::min<ssize_t>(result, size);
            TotalWritten_ += static_cast<size_t>(written);
            return written;
        }
        return result;
    }

    void SetResults(TVector<ssize_t> results) {
        Results_ = std::move(results);
    }

    size_t GetTotalWritten() const {
        return TotalWritten_;
    }

    size_t GetCallCount() const {
        return Calls_.size();
    }

private:
    size_t TotalWritten_ = 0;
    size_t Index_ = 0;
    TVector<ssize_t> Results_;
    TVector<size_t> Calls_;
};

TString ConsumeString(FuzzedDataProvider& fdp, size_t maxLen) {
    return TString(fdp.ConsumeRandomLengthString(maxLen));
}

void FuzzBufferedWriter(FuzzedDataProvider& fdp) {
    TDummySocketDescriptor socket;
    TVector<ssize_t> results;
    const size_t resultCount = fdp.ConsumeIntegralInRange<size_t>(0, 8);
    results.reserve(resultCount);
    for (size_t i = 0; i < resultCount; ++i) {
        if (fdp.ConsumeBool()) {
            results.push_back(fdp.ConsumeIntegralInRange<ssize_t>(1, 512));
        } else {
            results.push_back(fdp.ConsumeBool() ? -EAGAIN : -EWOULDBLOCK);
        }
    }
    socket.SetResults(std::move(results));

    NKikimr::NRawSocket::TBufferedWriter<TDummySocketDescriptor> writer(
        &socket,
        fdp.ConsumeIntegralInRange<size_t>(1, 512));

    const size_t writes = fdp.ConsumeIntegralInRange<size_t>(0, 8);
    for (size_t i = 0; i < writes; ++i) {
        const TString data = ConsumeString(fdp, 256);
        if (!data.empty()) {
            writer.write(data.data(), data.size());
        }
    }

    const size_t flushes = fdp.ConsumeIntegralInRange<size_t>(0, 8);
    for (size_t i = 0; i < flushes; ++i) {
        (void)writer.flush();
    }

    (void)socket.GetCallCount();
    (void)socket.GetTotalWritten();
}

void FuzzSocketAddress(FuzzedDataProvider& fdp) {
    const TString host = ConsumeString(fdp, 64);
    (void)NKikimr::NRawSocket::TInet64StreamSocket::IsIPv6(host);

    sockaddr_storage storage = {};
    if (fdp.ConsumeBool()) {
        auto* addr4 = reinterpret_cast<sockaddr_in*>(&storage);
        addr4->sin_family = AF_INET;
        addr4->sin_port = htons(fdp.ConsumeIntegral<ui16>());
        addr4->sin_addr.s_addr = htonl(fdp.ConsumeIntegral<ui32>());
    } else {
        auto* addr6 = reinterpret_cast<sockaddr_in6*>(&storage);
        addr6->sin6_family = AF_INET6;
        addr6->sin6_port = htons(fdp.ConsumeIntegral<ui16>());
        const auto bytes = fdp.ConsumeBytes<uint8_t>(16);
        std::memcpy(&addr6->sin6_addr, bytes.data(), bytes.size());
    }

    auto address = NKikimr::NRawSocket::TInet64StreamSocket::MakeAddress(storage);
    if (address) {
        (void)address->Size();
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    FuzzedDataProvider fdp(data, size);
    FuzzBufferedWriter(fdp);
    FuzzSocketAddress(fdp);
    return 0;
}
