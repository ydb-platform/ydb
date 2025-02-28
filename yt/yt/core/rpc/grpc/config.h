#pragma once

#include "public.h"

#include <yt/yt/core/crypto/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <contrib/libs/grpc/include/grpc/grpc_security_constants.h>

namespace NYT::NRpc::NGrpc {

////////////////////////////////////////////////////////////////////////////////

struct TDispatcherConfig
    : public NYTree::TYsonStruct
{
    int DispatcherThreadCount;
    int GrpcThreadCount;
    int GrpcEventEngineThreadCount;

    REGISTER_YSON_STRUCT(TDispatcherConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDispatcherConfig)

////////////////////////////////////////////////////////////////////////////////

struct TSslPemKeyCertPairConfig
    : public NYTree::TYsonStruct
{
    NCrypto::TPemBlobConfigPtr PrivateKey;
    NCrypto::TPemBlobConfigPtr CertChain;

    REGISTER_YSON_STRUCT(TSslPemKeyCertPairConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSslPemKeyCertPairConfig)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EClientCertificateRequest,
    ((DontRequestClientCertificate)(GRPC_SSL_DONT_REQUEST_CLIENT_CERTIFICATE))
    ((RequestClientCertificateButDontVerify)(GRPC_SSL_REQUEST_CLIENT_CERTIFICATE_BUT_DONT_VERIFY))
    ((RequestClientCertificateAndVerify)(GRPC_SSL_REQUEST_CLIENT_CERTIFICATE_AND_VERIFY))
    ((RequestAndRequireClientCertificateButDontVerify)(GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_BUT_DONT_VERIFY))
    ((RequestAndRequireClientCertificateAndVerify)(GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_AND_VERIFY))
);

////////////////////////////////////////////////////////////////////////////////

struct TServerCredentialsConfig
    : public NYTree::TYsonStruct
{
    NCrypto::TPemBlobConfigPtr PemRootCerts;
    std::vector<TSslPemKeyCertPairConfigPtr> PemKeyCertPairs;
    EClientCertificateRequest ClientCertificateRequest;

    REGISTER_YSON_STRUCT(TServerCredentialsConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TServerCredentialsConfig)

////////////////////////////////////////////////////////////////////////////////

struct TServerAddressConfig
    : public NYTree::TYsonStruct
{
    std::string Address;
    TServerCredentialsConfigPtr Credentials;

    REGISTER_YSON_STRUCT(TServerAddressConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TServerAddressConfig)

////////////////////////////////////////////////////////////////////////////////

struct TServerConfig
    : public NYTree::TYsonStruct
{
    std::string ProfilingName;

    std::vector<TServerAddressConfigPtr> Addresses;
    THashMap<std::string, NYTree::INodePtr> GrpcArguments;

    REGISTER_YSON_STRUCT(TServerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TServerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TChannelCredentialsConfig
    : public NYTree::TYsonStruct
{
    NCrypto::TPemBlobConfigPtr PemRootCerts;
    TSslPemKeyCertPairConfigPtr PemKeyCertPair;
    bool VerifyServerCert;

    REGISTER_YSON_STRUCT(TChannelCredentialsConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChannelCredentialsConfig)

////////////////////////////////////////////////////////////////////////////////

class TChannelConfigTemplate
    : public NYTree::TYsonStruct
{
public:
    TChannelCredentialsConfigPtr Credentials;
    THashMap<std::string, NYTree::INodePtr> GrpcArguments;

    REGISTER_YSON_STRUCT(TChannelConfigTemplate);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChannelConfigTemplate)

////////////////////////////////////////////////////////////////////////////////

struct TChannelConfig
    : public TChannelConfigTemplate
{
    std::string Address;

    REGISTER_YSON_STRUCT(TChannelConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChannelConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc::NGrpc
