#pragma once

#include "public.h"

#include <yt/yt/core/crypto/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <contrib/libs/grpc/include/grpc/grpc_security_constants.h>

namespace NYT::NRpc::NGrpc {

////////////////////////////////////////////////////////////////////////////////

class TDispatcherConfig
    : public NYTree::TYsonStruct
{
public:
    int DispatcherThreadCount;
    int GrpcThreadCount;

    REGISTER_YSON_STRUCT(TDispatcherConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDispatcherConfig)

////////////////////////////////////////////////////////////////////////////////

class TSslPemKeyCertPairConfig
    : public NYTree::TYsonStruct
{
public:
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

class TServerCredentialsConfig
    : public NYTree::TYsonStruct
{
public:
    NCrypto::TPemBlobConfigPtr PemRootCerts;
    std::vector<TSslPemKeyCertPairConfigPtr> PemKeyCertPairs;
    EClientCertificateRequest ClientCertificateRequest;

    REGISTER_YSON_STRUCT(TServerCredentialsConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TServerCredentialsConfig)

////////////////////////////////////////////////////////////////////////////////

class TServerAddressConfig
    : public NYTree::TYsonStruct
{
public:
    TString Address;
    TServerCredentialsConfigPtr Credentials;

    REGISTER_YSON_STRUCT(TServerAddressConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TServerAddressConfig)

////////////////////////////////////////////////////////////////////////////////

class TServerConfig
    : public NYTree::TYsonStruct
{
public:
    TString ProfilingName;

    std::vector<TServerAddressConfigPtr> Addresses;
    THashMap<TString, NYTree::INodePtr> GrpcArguments;

    REGISTER_YSON_STRUCT(TServerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TServerConfig)

////////////////////////////////////////////////////////////////////////////////

class TChannelCredentialsConfig
    : public NYTree::TYsonStruct
{
public:
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
    THashMap<TString, NYTree::INodePtr> GrpcArguments;

    REGISTER_YSON_STRUCT(TChannelConfigTemplate);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChannelConfigTemplate)

////////////////////////////////////////////////////////////////////////////////

class TChannelConfig
    : public TChannelConfigTemplate
{
public:
    TString Address;

    REGISTER_YSON_STRUCT(TChannelConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChannelConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc::NGrpc
