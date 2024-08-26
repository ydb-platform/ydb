#pragma once

#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/core/test_framework/test_memory_tracker.h>
#include <yt/yt/core/test_framework/test_server_host.h>

#include <yt/yt/core/bus/bus.h>
#include <yt/yt/core/bus/server.h>

#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/bus/tcp/client.h>
#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/crypto/config.h>

#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/thread_pool_poller.h>

#include <yt/yt/core/bus/public.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/memory_usage_tracker.h>

#include <yt/yt/core/rpc/bus/channel.h>
#include <yt/yt/core/rpc/bus/server.h>

#include <yt/yt/core/rpc/client.h>
#include <yt/yt/core/rpc/retrying_channel.h>
#include <yt/yt/core/rpc/caching_channel_factory.h>
#include <yt/yt/core/rpc/static_channel_factory.h>
#include <yt/yt/core/rpc/server.h>
#include <yt/yt/core/rpc/local_server.h>
#include <yt/yt/core/rpc/local_channel.h>
#include <yt/yt/core/rpc/service_detail.h>
#include <yt/yt/core/rpc/stream.h>

#include <yt/yt/core/rpc/unittests/lib/test_service.h>
#include <yt/yt/core/rpc/unittests/lib/no_baggage_service.h>

#include <yt/yt/core/rpc/grpc/config.h>
#include <yt/yt/core/rpc/grpc/channel.h>
#include <yt/yt/core/rpc/grpc/server.h>
#include <yt/yt/core/rpc/grpc/proto/grpc.pb.h>

#include <yt/yt/core/http/server.h>
#include <yt/yt/core/https/config.h>
#include <yt/yt/core/https/server.h>
#include <yt/yt/core/rpc/http/server.h>
#include <yt/yt/core/rpc/http/channel.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/shutdown.h>

#include <yt/yt/core/tracing/public.h>

#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/helpers.h>

#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/common/network.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
class TRpcTestBase
    : public ::testing::Test
{
public:
    void SetUp() final
    {
        bool secure = TImpl::Secure;

        WorkerPool_ = NConcurrency::CreateThreadPool(4, "Worker");
        MemoryUsageTracker_ = New<TTestNodeMemoryTracker>(32_MB);
        TestService_ = CreateTestService(WorkerPool_->GetInvoker(), secure, {}, MemoryUsageTracker_);

        auto services = std::vector<IServicePtr>{
            TestService_,
            CreateNoBaggageService(WorkerPool_->GetInvoker())
        };

        Host_ = TImpl::CreateTestServerHost(
            NTesting::GetFreePort(),
            std::move(services),
            MemoryUsageTracker_);
    }

    void TearDown() final
    {
        Host_->TearDown();
    }

    IChannelPtr CreateChannel(
        const std::optional<TString>& address = std::nullopt,
        THashMap<TString, NYTree::INodePtr> grpcArguments = {})
    {
        if (address) {
            return TImpl::CreateChannel(*address, Host_->GetAddress(), std::move(grpcArguments));
        } else {
            return TImpl::CreateChannel(Host_->GetAddress(), Host_->GetAddress(), std::move(grpcArguments));
        }
    }

    TTestNodeMemoryTrackerPtr GetMemoryUsageTracker() const
    {
        return Host_->GetMemoryUsageTracker();
    }

    ITestServicePtr GetTestService() const
    {
        return TestService_;
    }

    IServerPtr GetServer() const
    {
        return Host_->GetServer();
    }

    static bool CheckCancelCode(TErrorCode code)
    {
        if (code == NYT::EErrorCode::Canceled) {
            return true;
        }
        if (code == NYT::NRpc::EErrorCode::TransportError && TImpl::AllowTransportErrors) {
            return true;
        }
        return false;
    }

    static bool CheckTimeoutCode(TErrorCode code)
    {
        if (code == NYT::EErrorCode::Timeout) {
            return true;
        }
        if (code == NYT::NRpc::EErrorCode::TransportError && TImpl::AllowTransportErrors) {
            return true;
        }
        return false;
    }

private:
    NConcurrency::IThreadPoolPtr WorkerPool_;
    TTestNodeMemoryTrackerPtr MemoryUsageTracker_;
    TTestServerHostPtr Host_;
    ITestServicePtr TestService_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
class TRpcOverBus
{
public:
    static constexpr bool AllowTransportErrors = false;
    static constexpr bool Secure = false;

    static TTestServerHostPtr CreateTestServerHost(
        NTesting::TPortHolder port,
        std::vector<IServicePtr> services,
        TTestNodeMemoryTrackerPtr memoryUsageTracker)
    {
        auto busServer = MakeBusServer(port, memoryUsageTracker);
        auto server = NRpc::NBus::CreateBusServer(busServer);

        return New<TTestServerHost>(
            std::move(port),
            server,
            services,
            memoryUsageTracker);
    }

    static IChannelPtr CreateChannel(
        const std::string& address,
        const std::string& serverAddress,
        THashMap<TString, NYTree::INodePtr> grpcArguments)
    {
        return TImpl::CreateChannel(address, serverAddress, std::move(grpcArguments));
    }

    static NYT::NBus::IBusServerPtr MakeBusServer(ui16 port, IMemoryUsageTrackerPtr memoryUsageTracker)
    {
        return TImpl::MakeBusServer(port, memoryUsageTracker);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <bool ForceTcp>
class TRpcOverBusImpl
{
public:
    static IChannelPtr CreateChannel(
        const std::string& address,
        const std::string& /*serverAddress*/,
        THashMap<TString, NYTree::INodePtr> /*grpcArguments*/)
    {
        auto client = CreateBusClient(NYT::NBus::TBusClientConfig::CreateTcp(address));
        return NRpc::NBus::CreateBusChannel(client);
    }

    static NYT::NBus::IBusServerPtr MakeBusServer(ui16 port, IMemoryUsageTrackerPtr memoryUsageTracker)
    {
        auto busConfig = NYT::NBus::TBusServerConfig::CreateTcp(port);
        return CreateBusServer(
            busConfig,
            NYT::NBus::GetYTPacketTranscoderFactory(),
            memoryUsageTracker);
    }
};

////////////////////////////////////////////////////////////////////////////////

/*
 * openssl genrsa -out root_key.pem 2048
 * openssl req -x509 -new -nodes -key root_key.pem -sha256 -days 10000 -out root_cert.pem
 * openssl genrsa -out server_key.pem 2048
 * openssl genrsa -out client_key.pem 2048
 * openssl req -new -key server_key.pem -out server.csr
 * openssl req -new -key client_key.pem -out client.csr
 * openssl x509 -in server.csr -req -days 10000 -out server_cert.pem -CA root_cert.pem -CAkey root_key.pem -CAcreateserial
 * openssl x509 -in client.csr -req -days 10000 -out client_cert.pem -CA root_cert.pem -CAkey root_key.pem -CAserial root_cert.srl
 */
inline TString RootCert(
    "-----BEGIN CERTIFICATE-----\n"
    "MIID9DCCAtygAwIBAgIJAJLU9fgmNTujMA0GCSqGSIb3DQEBCwUAMFkxCzAJBgNV\n"
    "BAYTAlJVMRMwEQYDVQQIEwpTb21lLVN0YXRlMSEwHwYDVQQKExhJbnRlcm5ldCBX\n"
    "aWRnaXRzIFB0eSBMdGQxEjAQBgNVBAMTCWxvY2FsaG9zdDAeFw0xODAzMDQxMzUx\n"
    "MjdaFw00NTA3MjAxMzUxMjdaMFkxCzAJBgNVBAYTAlJVMRMwEQYDVQQIEwpTb21l\n"
    "LVN0YXRlMSEwHwYDVQQKExhJbnRlcm5ldCBXaWRnaXRzIFB0eSBMdGQxEjAQBgNV\n"
    "BAMTCWxvY2FsaG9zdDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAMEq\n"
    "JYLsNKAnO6uENyLjRww3pITvtEEg8uDi1W+87hZE8XNQ1crhJZDXcMaoWaVLOcQT\n"
    "6x2z5DAnn5/0CUXLrJgrwbfrZ82VwihQIpovPX91bA7Bd5PdlBI5ojtUrY9Fb6xB\n"
    "eAmfsp7z7rKDBheLe7KoNMth4OSHWp5GeHLzp336AB7TA6EQSTd3T7oDrRjdqZTr\n"
    "X35vF0n6+iOMSe5CJYuNX9fd5GkO6mwGV5BzEoUWwqTocfkLa2BfE+pvfsuWleNc\n"
    "sU8vMoAdlkKUrnHbbQ7xuwR+3XhKpRCU+wmzM6Tvm6dnYJhhTck8yxGNCuAfgKu+\n"
    "7k9Ur4rdPXYkSTUMbbcCAwEAAaOBvjCBuzAdBgNVHQ4EFgQUkZyJrYSMi34fw8wk\n"
    "sLSQyzg8a/swgYsGA1UdIwSBgzCBgIAUkZyJrYSMi34fw8wksLSQyzg8a/uhXaRb\n"
    "MFkxCzAJBgNVBAYTAlJVMRMwEQYDVQQIEwpTb21lLVN0YXRlMSEwHwYDVQQKExhJ\n"
    "bnRlcm5ldCBXaWRnaXRzIFB0eSBMdGQxEjAQBgNVBAMTCWxvY2FsaG9zdIIJAJLU\n"
    "9fgmNTujMAwGA1UdEwQFMAMBAf8wDQYJKoZIhvcNAQELBQADggEBADpYkiJ4XsdV\n"
    "w3JzDZCJX644cCzx3/l1N/ItVllVTbFU9MSrleifhBj21t4xUfHT2uhbQ21N6enA\n"
    "Qx24wcLo9IRL61XEkLrTRPo1ZRrF8rwAYLxFgHgWimcocG+c/8++he7tXrjyYzS1\n"
    "JyMKBgQcsrWn+3pCxSLHGuoH4buX3cMqrEepqdThIOTI12YW7xmD7vSguusroRFj\n"
    "OH5RO4hhHIn/tR2G/lHS1u+YG5NyX94v8kN+SfAchZmeb54miANYBGzOFqYRgKs4\n"
    "LfyFanmeXFJaj1M+37Lsm0TlxP6I7fa0Kag6FvlxpYvhblRJzsRHZE5Xe+KZzanV\n"
    "I2TYYgHjI3I=\n"
    "-----END CERTIFICATE-----\n");

inline TString ClientKey(
    "-----BEGIN RSA PRIVATE KEY-----\n"
    "MIIEpAIBAAKCAQEArZpqucOdMlwZyyTWq+Sz3EGXpAX/4nMpH7s/05d9O4tm0MsK\n"
    "QUhUXRzt3VzOfMOb4cXAVwovHxiQ7NZIFBdmeyCHlT0HVkaqC76Tgi53scUMVKtE\n"
    "lXJB5soc8PbFDjT21MOGzL3+Tqy47ecdZhCiaXYeD3exHFd+VDJXvC3O/GDc3/Fo\n"
    "Gwh6iUxkAAa11duUoFfCs3p+XFN216V9jqfkEmf/KU2utMjzSmAvwaGh0WCSTnb2\n"
    "lcByiPJWK6w8yx/CeY9Ks+sjI2hWw43jxUCcSa2pPimGLWgu9TYRiG4jWZln9FLW\n"
    "hskfF/Ra0Xp0ptxrnuih0DTQ+ZxTscNlg27nuwIDAQABAoIBAFRhD6rG52sI1Qim\n"
    "GSlnefx+bSQuPldkvgJMUxOXOClu8kRdy9g7PbYcT4keiMaflO7B3WDw9EJbAGX9\n"
    "KP+K+Ca0gvIIvb4zjocy1COcTlU7f2jP7f/tjxaL+lEswE7Nc4OqnaR6XFcFIMWR\n"
    "Zfqr7yTvYmEGPjGWXTKzXW17nnWQGYiK5IhLyzR+MQowCIVDK8ByJl18FRZOROtn\n"
    "O+Bbm/MCsLsevAJPlKefY8kG/aG6VbrJO0sTvYe/j2QpeSfPOYtSlcDnTdx2Y5za\n"
    "HFo+2mHvurhetl7Ba2gyTGu3XoMHtBXQ8jifyv8s3h+iz94twpsWp6D5CkPUw9oB\n"
    "OOx/ttECgYEA4z3L7mSJQANvodlWfJUKAIWgdO54Vq6yZ9gELaCCoUXZLwHwjw+v\n"
    "3k/WNbCv7lIL/DVzVh/RfFaG4Qe/c/Bu2tgwBv4fcAepvegUznwcY4Q1FB3sPMpm\n"
    "fYcYPOy7jwEO7fvG8rjlZCXo6JuyJJsfyC+z+qWuPSpNgY+lj5MPe3UCgYEAw5LX\n"
    "VZYnoghqMQGAi2CldxQ5Iz4RtZpIMgJH7yfu7jt1b3VGiBChwwbyfYrvPJTBpfP9\n"
    "U05iffC8P8NVVL8KtjNRJLmQftLdssoqCncqdALnBGJ/jRNpxEFOcodReodkmUT/\n"
    "vwQOfQXx0JayeRbUmPKgkEfaqcJL2Y2O41iq4G8CgYEA14kYsb/4EphvvLLZfoca\n"
    "mo4kOGSsDYPbwfU5WVGiNXd73UNYuUjmxdUx13EEHecCaTEFeY3qc6XafvyLUlud\n"
    "ucNOIoPMq8UI8hB8E7HSd23BrpgHJ03O0oddrQPZjnUxhPbHqBdJtKjkdiSfXmso\n"
    "RQdCDZ4yWt+R7i6imUCicbUCgYEApg6iY/tQv5XhhKa/3Jg9JnS3ZyMmqknLjxq8\n"
    "tWX0y7cUqYSsVI+6qfvWHZ7AL3InUp9uszNVEZY8YO+cHo7vq3C7LzGYbPbiYxKg\n"
    "y64PD93/BYwUvVaEcaz5zOj019LqKfGaLThmjOVlQzURaRtnfE5W4ur/0TA2cwxt\n"
    "DMCWpmUCgYBKZhCPxpJmJVFdiM6c5CpsEBdoJ7NpPdDR8xlY7m2Nb38szAFRAFhk\n"
    "gMk6gXG+ObmPd4H6Up2lrpJgH3GDPIoiBZPOJefa0IXAmYCpqPH+HLG2lspuNyFL\n"
    "OY4A1p2EvY8/L6PmPXAURfsE8RTL0y4ww/7mPJTQXsteTawAPDdVKQ==\n"
    "-----END RSA PRIVATE KEY-----\n");

inline TString ClientCert(
    "-----BEGIN CERTIFICATE-----\n"
    "MIIDLjCCAhYCCQCZd28+0jJVLTANBgkqhkiG9w0BAQUFADBZMQswCQYDVQQGEwJS\n"
    "VTETMBEGA1UECBMKU29tZS1TdGF0ZTEhMB8GA1UEChMYSW50ZXJuZXQgV2lkZ2l0\n"
    "cyBQdHkgTHRkMRIwEAYDVQQDEwlsb2NhbGhvc3QwHhcNMTgwMzA0MTM1MjU2WhcN\n"
    "NDUwNzIwMTM1MjU2WjBZMQswCQYDVQQGEwJBVTETMBEGA1UECBMKU29tZS1TdGF0\n"
    "ZTEhMB8GA1UEChMYSW50ZXJuZXQgV2lkZ2l0cyBQdHkgTHRkMRIwEAYDVQQDEwls\n"
    "b2NhbGhvc3QwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCtmmq5w50y\n"
    "XBnLJNar5LPcQZekBf/icykfuz/Tl307i2bQywpBSFRdHO3dXM58w5vhxcBXCi8f\n"
    "GJDs1kgUF2Z7IIeVPQdWRqoLvpOCLnexxQxUq0SVckHmyhzw9sUONPbUw4bMvf5O\n"
    "rLjt5x1mEKJpdh4Pd7EcV35UMle8Lc78YNzf8WgbCHqJTGQABrXV25SgV8Kzen5c\n"
    "U3bXpX2Op+QSZ/8pTa60yPNKYC/BoaHRYJJOdvaVwHKI8lYrrDzLH8J5j0qz6yMj\n"
    "aFbDjePFQJxJrak+KYYtaC71NhGIbiNZmWf0UtaGyR8X9FrRenSm3Gue6KHQNND5\n"
    "nFOxw2WDbue7AgMBAAEwDQYJKoZIhvcNAQEFBQADggEBAImeUspGIeL24U5sK2PR\n"
    "1BcWUBHtfUtXozaPK/q6WbEMObPxuNenNjnEYdp7b8JT2g91RqYd645wIPGaDAnc\n"
    "EFz3b2piUZIG8YfCCLdntqwrYLxdGuHt/47RoSCZ2WrTZA6j7wP5ldQZTfefq3VC\n"
    "ncz985cJ5AgEOZJmEdcleraoE1ZHb7O/kVxdxA6g93v9n3mm+kVYh3hth2646I8P\n"
    "Bn8Gucf3jySWsN5H74lnp4VaA0xyJh2hC/4e/RnYod7TkXaqKeeLc93suIXgHHKt\n"
    "jGvMhVuIWj3zzRi5e8Z1Ww5uHbiVyo4+GZMuV6w5ePgZpQ+5hUeD8PYf1AqDZet4\n"
    "3SA=\n"
    "-----END CERTIFICATE-----\n");

inline TString ServerKey(
    "-----BEGIN RSA PRIVATE KEY-----\n"
    "MIIEowIBAAKCAQEAzbAyEJFSmPNJ3pLNNSWQVF53Ltof1Wc4JIfvNazl41LjNyuO\n"
    "SQV7+6GVFMIybBBoeWQ58hVJ/d8KxFBf6XIV6uGH9WtN38hWrxR6UEGkHxpUSfvg\n"
    "TZ2FSsusus5sYDXjW+liQg5P9X/O69z/vmrIuyS8GckNq4/sA+Pw5GgCWDS05e72\n"
    "N8r6DG7UlzKm5ynCGI8pRh/EdmxHTP4G8bEKF25x4FRy3Mg7bAaif9owliC2+BLI\n"
    "IRNMtZs9BWp0U8GzEv2wY8xzkJEFD37xBiwHOWDj9KAmJpXQMM48PoXgvQsUo0ed\n"
    "/a+GHvumeb3tBtsqLALhLFQBEFykA9X4SF93jwIDAQABAoIBAQC488Bw6VuuMMWx\n"
    "n6tqKLbZRoBA3t5VFBWFs73DNA8bE8NALqgovQe5Qpg9LEoOpcprrVX1enMoFtEl\n"
    "qWg1D+Lpa5bHdY92tDxN/knltMCRPymfxR7ya7wZf394EnmdIZepY/h4kUoQ5LX5\n"
    "nKVSYc7RiLyjKwhhxm5hKSvJFkVVbaKvb9jFPEpYJHNWktl9Hh6XLs/DQLZwEVy0\n"
    "rR7KSV00XyNPtMlt6EBXLW7/ysYBiDdcGZ+lIp36fDkoC+kmfbNxsmsEO7x/63NW\n"
    "yCmhj4qz9hELbuOMNyoX0jzWMXdfEba/t/Gk7klB1/bQZ8VBn4Nd9PTEPHFLhNG2\n"
    "s/bQoH3RAoGBAOguUWbVar200VcPwnRjDg2Gw+N+xTR5ONvanIsJaf6xBW8Ymtsl\n"
    "J6GDJrJ391L0Zs2+fxLXDUebS8CF8CghL1KtqZxoTSwjBz8G4kn3DKlyZgNJZgyi\n"
    "GppY4ttaP1ys1LwO/xzPUJb9pqm84KDjE9JL1czv3Psk5PVzxV/PQlyzAoGBAOLK\n"
    "HElPA4rWw79AW9Kzr9mykZzyqalvAmobz8Q/nINnVGUcQu6UY5vDQ6KCOg2vbTl1\n"
    "shDrzEyD/mityBZWUgFiKp+KEYjD5CKE8XuryM3MHr9Dvb+zt2JMC6TVBrYJNG91\n"
    "OnMjGACRJ0i5SoB2kxiruTwyc2bzWyB6Dw9TfN+1AoGACHwg12w3MWWZPOBDj/NK\n"
    "wS3KnNa2KDvB2y77B425hOg9NZkll5qc/ycG1ADUVgC+fQhYJn0bbCF9vDRo2V6V\n"
    "FyVnjGK3Z0SEcEY1INTZbpvSpI4bH50Q8dELwU5kAGQEhjbaFdhxroLog012vApw\n"
    "YAALeSjO35Kyl1G6xcySNUcCgYBX+rAegFiPc+FcQEte4fZGLc/vYvQOltII9+ER\n"
    "8Nt23o8O6nfMtiQuOQHz+TEsPfHRaKc7iT4oMMxxL3l/sNz/TGXcnmNO+y91dL15\n"
    "jJrJu3XyHQVvaPirWXTq7Pk9hTSiSIf0Qpj9H1JuE/OjAlzuJTAm+itqtN2VK8TL\n"
    "3UeEQQKBgA61gNqGc8uCm58vg76qjMw6dlxBrpjWxYC5QsNh/OUITtWXqKiwTThE\n"
    "wkLMtumpDoioIp/cv8xyV7yvdNM0pxB5UtXBK/3P91lKbiyIfpertqMNxs5XzoeG\n"
    "CyxY8hFTw3FSk+UYdAAm5qYabGY1DiuvyD1yVAX9aWjAHdbP3H5O\n"
    "-----END RSA PRIVATE KEY-----\n");

inline TString ServerCert(
    "-----BEGIN CERTIFICATE-----\n"
    "MIIDLjCCAhYCCQCZd28+0jJVLDANBgkqhkiG9w0BAQUFADBZMQswCQYDVQQGEwJS\n"
    "VTETMBEGA1UECBMKU29tZS1TdGF0ZTEhMB8GA1UEChMYSW50ZXJuZXQgV2lkZ2l0\n"
    "cyBQdHkgTHRkMRIwEAYDVQQDEwlsb2NhbGhvc3QwHhcNMTgwMzA0MTM1MjUwWhcN\n"
    "NDUwNzIwMTM1MjUwWjBZMQswCQYDVQQGEwJBVTETMBEGA1UECBMKU29tZS1TdGF0\n"
    "ZTEhMB8GA1UEChMYSW50ZXJuZXQgV2lkZ2l0cyBQdHkgTHRkMRIwEAYDVQQDEwls\n"
    "b2NhbGhvc3QwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDNsDIQkVKY\n"
    "80neks01JZBUXncu2h/VZzgkh+81rOXjUuM3K45JBXv7oZUUwjJsEGh5ZDnyFUn9\n"
    "3wrEUF/pchXq4Yf1a03fyFavFHpQQaQfGlRJ++BNnYVKy6y6zmxgNeNb6WJCDk/1\n"
    "f87r3P++asi7JLwZyQ2rj+wD4/DkaAJYNLTl7vY3yvoMbtSXMqbnKcIYjylGH8R2\n"
    "bEdM/gbxsQoXbnHgVHLcyDtsBqJ/2jCWILb4EsghE0y1mz0FanRTwbMS/bBjzHOQ\n"
    "kQUPfvEGLAc5YOP0oCYmldAwzjw+heC9CxSjR539r4Ye+6Z5ve0G2yosAuEsVAEQ\n"
    "XKQD1fhIX3ePAgMBAAEwDQYJKoZIhvcNAQEFBQADggEBAJ1bjP+J+8MgSeHvpCES\n"
    "qo49l8JgpFV9h/1dUgz2fYhrVy7QCp8/3THoZcjErKYyzTdOlTzCy1OB4sRNLBiy\n"
    "ftGGTm1KHWal9CNMwAN00+ebhwdqKjNCWViI45o5OSfPWUvGAkwxUENrOqLoGBvR\n"
    "cVvvMIV5KeaZLTtvrPzfVCMq/B41Mu5ZslDZOTRmSpVlbxmFjUq3WM+wf1sLu2cw\n"
    "DDk8O2UQpxJeiowu9XBkQCEkvxU3/5bPBvY/+3sikj8IqaknakEXBKH1e/ZTN3/l\n"
    "F6/pV9FE34DC9mIlzIFQyMGKJd4cju6970Pv3blQabuNHJTd570JdMBYbUGJp/mI\n"
    "6sI=\n"
    "-----END CERTIFICATE-----\n");

////////////////////////////////////////////////////////////////////////////////

template <bool EnableSsl, bool EnableUds>
class TRpcOverGrpcImpl
{
public:
    static constexpr bool AllowTransportErrors = true;
    static constexpr bool Secure = EnableSsl;

    static IChannelPtr CreateChannel(
        const std::string& address,
        const std::string& /*serverAddress*/,
        THashMap<TString, NYTree::INodePtr> grpcArguments)
    {
        auto channelConfig = New<NGrpc::TChannelConfig>();
        if (EnableSsl) {
            channelConfig->Credentials = New<NGrpc::TChannelCredentialsConfig>();
            channelConfig->Credentials->PemRootCerts = New<NCrypto::TPemBlobConfig>();
            channelConfig->Credentials->PemRootCerts->Value = RootCert;
            channelConfig->Credentials->PemKeyCertPair = New<NGrpc::TSslPemKeyCertPairConfig>();
            channelConfig->Credentials->PemKeyCertPair->PrivateKey = New<NCrypto::TPemBlobConfig>();
            channelConfig->Credentials->PemKeyCertPair->PrivateKey->Value = ClientKey;
            channelConfig->Credentials->PemKeyCertPair->CertChain = New<NCrypto::TPemBlobConfig>();
            channelConfig->Credentials->PemKeyCertPair->CertChain->Value = ClientCert;
        }

        if (EnableUds) {
            channelConfig->Address = Format("unix:%v", address);
        } else {
            channelConfig->Address = address;
        }

        channelConfig->GrpcArguments = std::move(grpcArguments);
        return NGrpc::CreateGrpcChannel(channelConfig);
    }

    static TTestServerHostPtr CreateTestServerHost(
        NTesting::TPortHolder port,
        std::vector<IServicePtr> services,
        TTestNodeMemoryTrackerPtr memoryUsageTracker)
    {
        auto serverAddressConfig = New<NGrpc::TServerAddressConfig>();
        if (EnableSsl) {
            serverAddressConfig->Credentials = New<NGrpc::TServerCredentialsConfig>();
            serverAddressConfig->Credentials->PemRootCerts = New<NCrypto::TPemBlobConfig>();
            serverAddressConfig->Credentials->PemRootCerts->Value = RootCert;
            serverAddressConfig->Credentials->PemKeyCertPairs.push_back(New<NGrpc::TSslPemKeyCertPairConfig>());
            serverAddressConfig->Credentials->PemKeyCertPairs[0]->PrivateKey = New<NCrypto::TPemBlobConfig>();
            serverAddressConfig->Credentials->PemKeyCertPairs[0]->PrivateKey->Value = ServerKey;
            serverAddressConfig->Credentials->PemKeyCertPairs[0]->CertChain = New<NCrypto::TPemBlobConfig>();
            serverAddressConfig->Credentials->PemKeyCertPairs[0]->CertChain->Value = ServerCert;
        }

        if (EnableUds) {
            serverAddressConfig->Address = Format("unix:localhost:%v", port);
        } else {
            serverAddressConfig->Address = Format("localhost:%v", port);
        }

        auto serverConfig = New<NGrpc::TServerConfig>();
        serverConfig->Addresses.push_back(serverAddressConfig);

        auto server = NGrpc::CreateServer(serverConfig);
        return New<TTestServerHost>(
            std::move(port),
            std::move(server),
            std::move(services),
            std::move(memoryUsageTracker));
    }
};

////////////////////////////////////////////////////////////////////////////////

// TRpcOverUdsImpl creates unix domain sockets, supported only on Linux.
class TRpcOverUdsImpl
{
public:
    static NYT::NBus::IBusServerPtr MakeBusServer(ui16 port, IMemoryUsageTrackerPtr memoryUsageTracker)
    {
        SocketPath_ = GetWorkPath() + "/socket_" + ToString(port);
        auto busConfig = NYT::NBus::TBusServerConfig::CreateUds(SocketPath_);
        return CreateBusServer(
            busConfig,
            NYT::NBus::GetYTPacketTranscoderFactory(),
            memoryUsageTracker);
    }

    static IChannelPtr CreateChannel(
        const std::string& address,
        const std::string& serverAddress,
        THashMap<TString, NYTree::INodePtr> /*grpcArguments*/)
    {
        auto clientConfig = NYT::NBus::TBusClientConfig::CreateUds(
            address == serverAddress ? SocketPath_ : address);
        auto client = CreateBusClient(clientConfig);
        return NRpc::NBus::CreateBusChannel(client);
    }

private:
    static inline std::string SocketPath_;
};

////////////////////////////////////////////////////////////////////////////////

template <bool EnableSsl>
class TRpcOverHttpImpl
{
public:
    static constexpr bool AllowTransportErrors = true;

    // NOTE: Some minor functionality is still missing from the HTTPs server.
    // TODO(melkov): Fill ssl_credentials_ext in server code and enable the Secure flag.
    static constexpr bool Secure = false;

    static IChannelPtr CreateChannel(
        const std::string& address,
        const std::string& /*serverAddress*/,
        THashMap<TString, NYTree::INodePtr> /*grpcArguments*/)
    {
        static auto poller = NConcurrency::CreateThreadPoolPoller(4, "HttpChannelTest");
        auto credentials = New<NHttps::TClientCredentialsConfig>();
        credentials->PrivateKey = New<NCrypto::TPemBlobConfig>();
        credentials->PrivateKey->Value = ServerKey;
        credentials->CertChain = New<NCrypto::TPemBlobConfig>();
        credentials->CertChain->Value = ServerCert;
        return NHttp::CreateHttpChannel(address, poller, EnableSsl, credentials);
    }

    static TTestServerHostPtr CreateTestServerHost(
        NTesting::TPortHolder port,
        std::vector<IServicePtr> services,
        TTestNodeMemoryTrackerPtr memoryUsageTracker)
    {
        auto config = New<NHttps::TServerConfig>();
        config->Port = port;
        config->CancelFiberOnConnectionClose = true;
        config->ServerName = "HttpServerTest";

        NYT::NHttp::IServerPtr httpServer;
        if (EnableSsl) {
            config->Credentials = New<NHttps::TServerCredentialsConfig>();
            config->Credentials->PrivateKey = New<NCrypto::TPemBlobConfig>();
            config->Credentials->PrivateKey->Value = ServerKey;
            config->Credentials->CertChain = New<NCrypto::TPemBlobConfig>();
            config->Credentials->CertChain->Value = ServerCert;
            httpServer = NYT::NHttps::CreateServer(config, 4);
        } else {
            httpServer = NYT::NHttp::CreateServer(config, 4);
        }

        auto httpRpcServer = NYT::NRpc::NHttp::CreateServer(httpServer);
        return New<TTestServerHost>(
            std::move(port),
            httpRpcServer,
            services,
            memoryUsageTracker);
    }
};

////////////////////////////////////////////////////////////////////////////////

using TAllTransports = ::testing::Types<
#ifdef _linux_
    TRpcOverBus<TRpcOverUdsImpl>,
    TRpcOverBus<TRpcOverBusImpl<true>>,
#endif
    TRpcOverBus<TRpcOverBusImpl<false>>,
    TRpcOverGrpcImpl<false, false>,
    TRpcOverGrpcImpl<false, true>,
    TRpcOverGrpcImpl<true, false>,
    TRpcOverGrpcImpl<true, true>,
    TRpcOverHttpImpl<false>,
    TRpcOverHttpImpl<true>
>;

using TWithAttachments = ::testing::Types<
#ifdef _linux_
    TRpcOverBus<TRpcOverUdsImpl>,
    TRpcOverBus<TRpcOverBusImpl<true>>,
#endif
    TRpcOverBus<TRpcOverBusImpl<false>>,
    TRpcOverGrpcImpl<false, false>,
    TRpcOverGrpcImpl<false, true>,
    TRpcOverGrpcImpl<true, false>,
    TRpcOverGrpcImpl<true, true>
>;

using TWithoutUds = ::testing::Types<
#ifdef _linux_
    TRpcOverBus<TRpcOverBusImpl<true>>,
#endif
    TRpcOverBus<TRpcOverBusImpl<false>>,
    TRpcOverGrpcImpl<false, false>,
    TRpcOverGrpcImpl<true, false>,
    TRpcOverHttpImpl<false>,
    TRpcOverHttpImpl<true>
>;

using TWithoutGrpc = ::testing::Types<
#ifdef _linux_
    TRpcOverBus<TRpcOverUdsImpl>,
    TRpcOverBus<TRpcOverBusImpl<true>>,
#endif
    TRpcOverBus<TRpcOverBusImpl<false>>
>;

using TGrpcOnly = ::testing::Types<
    TRpcOverGrpcImpl<false, false>,
    TRpcOverGrpcImpl<false, true>,
    TRpcOverGrpcImpl<true, false>,
    TRpcOverGrpcImpl<true, true>
>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
