#include <util/system/tempfile.h>
#include "ldap_socket_wrapper.h"

namespace LdapMock {

namespace {
const TString serverCert {R"___(-----BEGIN CERTIFICATE-----
MIIDjTCCAnWgAwIBAgIURt5IBx0J3xgEaQvmyrFH2A+NkpMwDQYJKoZIhvcNAQEL
BQAwVjELMAkGA1UEBhMCUlUxDzANBgNVBAgMBk1vc2NvdzEPMA0GA1UEBwwGTW9z
Y293MQ8wDQYDVQQKDAZZYW5kZXgxFDASBgNVBAMMC3Rlc3Qtc2VydmVyMB4XDTE5
MDkyMDE3MTQ0MVoXDTQ3MDIwNDE3MTQ0MVowVjELMAkGA1UEBhMCUlUxDzANBgNV
BAgMBk1vc2NvdzEPMA0GA1UEBwwGTW9zY293MQ8wDQYDVQQKDAZZYW5kZXgxFDAS
BgNVBAMMC3Rlc3Qtc2VydmVyMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKC
AQEAs0WY6HTuwKntcEcjo+pBuoNp5/GRgMX2qOJi09Iw021ZLK4Vf4drN7pXS5Ba
OVqzUPFmXvoiG13hS7PLTuobJc63qPbIodiB6EXB+Sp0v+mE6lYUUyW9YxNnTPDc
GG8E4vk9j3tBawT4yJIFTudIALWJfQvn3O9ebmYkilvq0ZT+TqBU8Mazo4lNu0T2
YxWMlivcEyNRLPbka5W2Wy5eXGOnStidQFYka2mmCgljtulWzj1i7GODg93vmVyH
NzjAs+mG9MJkT3ietG225BnyPDtu5A3b+vTAFhyJtMmDMyhJ6JtXXHu6zUDQxKiX
6HLGCLIPhL2sk9ckPSkwXoMOywIDAQABo1MwUTAdBgNVHQ4EFgQUDv/xuJ4CvCgG
fPrZP3hRAt2+/LwwHwYDVR0jBBgwFoAUDv/xuJ4CvCgGfPrZP3hRAt2+/LwwDwYD
VR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEAinKpMYaA2tjLpAnPVbjy
/ZxSBhhB26RiQp3Re8XOKyhTWqgYE6kldYT0aXgK9x9mPC5obQannDDYxDc7lX+/
qP/u1X81ZcDRo/f+qQ3iHfT6Ftt/4O3qLnt45MFM6Q7WabRm82x3KjZTqpF3QUdy
tumWiuAP5DMd1IRDtnKjFHO721OsEsf6NLcqdX89bGeqXDvrkwg3/PNwTyW5E7cj
feY8L2eWtg6AJUnIBu11wvfzkLiH3QKzHvO/SIZTGf5ihDsJ3aKEE9UNauTL3bVc
CRA/5XcX13GJwHHj6LCoc3sL7mt8qV9HKY2AOZ88mpObzISZxgPpdKCfjsrdm63V
6g==
-----END CERTIFICATE-----)___"
};

const TString serverPrivateKey {R"___(-----BEGIN PRIVATE KEY-----
MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQCzRZjodO7Aqe1w
RyOj6kG6g2nn8ZGAxfao4mLT0jDTbVksrhV/h2s3uldLkFo5WrNQ8WZe+iIbXeFL
s8tO6hslzreo9sih2IHoRcH5KnS/6YTqVhRTJb1jE2dM8NwYbwTi+T2Pe0FrBPjI
kgVO50gAtYl9C+fc715uZiSKW+rRlP5OoFTwxrOjiU27RPZjFYyWK9wTI1Es9uRr
lbZbLl5cY6dK2J1AViRraaYKCWO26VbOPWLsY4OD3e+ZXIc3OMCz6Yb0wmRPeJ60
bbbkGfI8O27kDdv69MAWHIm0yYMzKEnom1dce7rNQNDEqJfocsYIsg+EvayT1yQ9
KTBegw7LAgMBAAECggEBAKaOCrotqYQmXArsjRhFFDwMy+BKdzyEr93INrlFl0dX
WHpCYobRcbOc1G3H94tB0UdqgAnNqtJyLlb+++ydZAuEOu4oGc8EL+10ofq0jzOd
6Xct8kQt0/6wkFDTlii9PHUDy0X65ZRgUiNGRtg/2I2QG+SpowmI+trm2xwQueFs
VaWrjc3cVvXx0b8Lu7hqZUv08kgC38stzuRk/n2T5VWSAr7Z4ZWQbO918Dv35HUw
Wy/0jNUFP9CBCvFJ4l0OoH9nYhWFG+HXWzNdw6/Hca4jciRKo6esCiOZ9uWYv/ec
/NvX9rgFg8G8/SrTisX10+Bbeq+R1RKwq/IG409TH4ECgYEA14L+3QsgNIUMeYAx
jSCyk22R/tOHI1BM+GtKPUhnwHlAssrcPcxXMJovl6WL93VauYjym0wpCz9urSpA
I2CqTsG8GYciA6Dr3mHgD6cK0jj9UPAU6EnZ5S0mjhPqKZqutu9QegzD2uESvuN8
36xezwQthzAf0nI/P3sJGjVXjikCgYEA1POm5xcV6SmM6HnIdadEebhzZIJ9TXQz
ry3Jj3a7CKyD5C7fAdkHUTCjgT/2ElxPi9ABkZnC+d/cW9GtJFa0II5qO/agm3KQ
ZXYiutu9A7xACHYFXRiJEjVUdGG9dKMVOHUEa8IHEgrrcUVM/suy/GgutywIfaXs
y58IFP24K9MCgYEAk6zjz7wL+XEiNy+sxLQfKf7vB9sSwxQHakK6wHuY/L8Zomp3
uLEJHfjJm/SIkK0N2g0JkXkCtv5kbKyC/rsCeK0wo52BpVLjzaLr0k34kE0U6B1b
dkEE2pGx1bG3x4KDLj+Wuct9ecK5Aa0IqIyI+vo16GkFpUM8K9e3SQo8UOECgYEA
sCZYAkILYtJ293p9giz5rIISGasDAUXE1vxWBXEeJ3+kneTTnZCrx9Im/ewtnWR0
fF90XL9HFDDD88POqAd8eo2zfKR2l/89SGBfPBg2EtfuU9FkgGyiPciVcqvC7q9U
B15saMKX3KnhtdGwbfeLt9RqCCTJZT4SUSDcq5hwdvcCgYAxY4Be8mNipj8Cgg22
mVWSolA0TEzbtUcNk6iGodpi+Z0LKpsPC0YRqPRyh1K+rIltG1BVdmUBHcMlOYxl
lWWvbJH6PkJWy4n2MF7PO45kjN3pPZg4hgH63JjZeAineBwEArUGb9zHnvzcdRvF
wuQ2pZHL/HJ0laUSieHDJ5917w==
-----END PRIVATE KEY-----)___"
};
}

TLdapSocketWrapper::TLdapSocketWrapper(TAtomicSharedPtr<TInetStreamSocket> listenSocket, bool isSecureConnection)
    : ListenSocket(listenSocket)
    , Socket()
    , Ctx(CreateSslContext())
    , Ssl(SSL_new(Ctx.Get()))
    , IsSecureConnection(isSecureConnection)
{
    SetupCerts();
    SSL_set_accept_state(Ssl.Get());
}

void TLdapSocketWrapper::Receive(void* buf, size_t len) {
    TBaseSocket::Check(ReceiveMsg(*this, buf, len), "Receive");
}

void TLdapSocketWrapper::Send(const void* msg, size_t len) {
    TBaseSocket::Check(SendMsg(*this, msg, len), "Send");
}

void TLdapSocketWrapper::OnAccept() {
    TSockAddrInet Addr;
    TBaseSocket::Check(ListenSocket->Accept(&Socket, &Addr), "accept");
    if (IsSecureConnection) {
        EnableSecureConnection();
    } else {
        ReceiveMsg = &TLdapSocketWrapper::InsecureReceive;
        SendMsg = &TLdapSocketWrapper::InsecureSend;
    }
}

void TLdapSocketWrapper::EnableSecureConnection() {
    SSL_set_fd(Ssl.Get(), Socket);
    TBaseSocket::Check(SSL_accept(Ssl.Get()), "SslAccept");
    ReceiveMsg = &TLdapSocketWrapper::SecureReceive;
    SendMsg = &TLdapSocketWrapper::SecureSend;
}
bool TLdapSocketWrapper::IsSecure() const {
    return IsSecureConnection;
}

void TLdapSocketWrapper::Close() {
    Socket.Close();
}

ssize_t TLdapSocketWrapper::InsecureReceive(void* buf, size_t len) {
    return Socket.Recv(buf, len);
}

ssize_t TLdapSocketWrapper::InsecureSend(const void* msg, size_t len) {
    return Socket.Send(msg, len);
}

ssize_t TLdapSocketWrapper::SecureReceive(void* buf, size_t len) {
    return SSL_read(Ssl.Get(), buf, len);
}

ssize_t TLdapSocketWrapper::SecureSend(const void* msg, size_t len) {
    return SSL_write(Ssl.Get(), msg, len);
}

void TLdapSocketWrapper::SetupCerts() {
    TTempFileHandle certificateFile;
    certificateFile.Write(serverCert.data(), serverCert.size());

    TTempFileHandle privateKeyFile;
    privateKeyFile.Write(serverPrivateKey.data(), serverPrivateKey.size());

    SSL_use_certificate_file(Ssl.Get(), certificateFile.Name().c_str(), SSL_FILETYPE_PEM);
    SSL_use_PrivateKey_file(Ssl.Get(), privateKeyFile.Name().c_str(), SSL_FILETYPE_PEM);
}

TLdapSocketWrapper::TSslHolder<SSL_CTX> TLdapSocketWrapper::CreateSslContext() {
    TSslHolder<SSL_CTX> context(SSL_CTX_new(SSLv23_server_method()));
    if (context) {
        SSL_CTX_set_options(context.Get(), SSL_OP_NO_SSLv2);
        SSL_CTX_set_options(context.Get(), SSL_OP_NO_SSLv3);
        SSL_CTX_set_options(context.Get(), SSL_OP_MICROSOFT_SESS_ID_BUG);
        SSL_CTX_set_options(context.Get(), SSL_OP_NETSCAPE_CHALLENGE_BUG);
    }
    return context;
}

} // LdapMock
