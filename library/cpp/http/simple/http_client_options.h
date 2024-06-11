#pragma once

#include <util/datetime/base.h>
#include <library/cpp/string_utils/url/url.h>

class TSimpleHttpClientOptions {
    using TSelf = TSimpleHttpClientOptions;

public:
    TSimpleHttpClientOptions() = default;

    explicit TSimpleHttpClientOptions(TStringBuf url) {
        TStringBuf scheme, host;
        GetSchemeHostAndPort(url, scheme, host, Port_);
        Host_ = url.Head(scheme.size() + host.size());
    }

    TSelf& Host(TStringBuf host) {
        Host_ = host;
        return *this;
    }

    const TString& Host() const noexcept {
        return Host_;
    }

    TSelf& Port(ui16 port) {
        Port_ = port;
        return *this;
    }

    ui16 Port() const noexcept {
        return Port_;
    }

    TSelf& SocketTimeout(TDuration timeout) {
        SocketTimeout_ = timeout;
        return *this;
    }

    TDuration SocketTimeout() const noexcept {
        return SocketTimeout_;
    }

    TSelf& ConnectTimeout(TDuration timeout) {
        ConnectTimeout_ = timeout;
        return *this;
    }

    TDuration ConnectTimeout() const noexcept {
        return ConnectTimeout_;
    }

    TSelf& MaxRedirectCount(int count) {
        MaxRedirectCount_ = count;
        return *this;
    }

    ui16 MaxRedirectCount() const noexcept {
        return MaxRedirectCount_;
    }

private:
    TString Host_;
    ui16 Port_;
    TDuration SocketTimeout_ = TDuration::Seconds(5);
    TDuration ConnectTimeout_ = TDuration::Seconds(30);
    int MaxRedirectCount_ = INT_MAX;
};
