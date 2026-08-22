#include "socket.h"
#include <util/generic/string.h>
#include <openssl/err.h>

namespace LdapMock {

TSocket::TSocket(int fd)
    : Fd(fd)
    , UseTls(false)
{}

bool TSocket::isTls() const {
    return UseTls;
}

bool TSocket::Receive(void* buf, size_t len) {
    if(UseTls) {
        return ReceiveTls(buf, len);
    }
    return ReceivePlain(buf, len);
}

bool TSocket::Send(const void* msg, size_t len) {
    if (UseTls) {
        return SendTls(msg, len);
    }
    return SendPlain(msg, len);
}

bool TSocket::UpgradeToTls(SSL_CTX* ctx) {
    Ssl.Reset(SSL_new(ctx));
    if (!Ssl) {
        return false;
    }

    SSL_set_fd(Ssl.Get(), Fd);

    if (SSL_accept(Ssl.Get()) != 1) {
        ERR_print_errors_fp(stderr);
        Ssl.Reset(nullptr);
        return false;
    }

    TSslHolder<X509> clientCert(SSL_get_peer_certificate(Ssl.Get()));
    if (clientCert) {
        char buf[1024];
        X509_NAME_oneline(X509_get_subject_name(clientCert.Get()), buf, sizeof(buf));
        ClientSubjectName = TString(buf);
    }

    UseTls = true;
    return true;
}

TString TSocket::GetClientCertSubjectName() const {
    return ClientSubjectName;
}

bool TSocket::ReceivePlain(void* buf, size_t len) {
    uint8_t* p = (uint8_t*)buf;
    while (len) {
        ssize_t r = ::recv(Fd, p, len, 0);
        if (r <= 0) {
            return false;
        }
        p += (size_t)r;
        len -= (size_t)r;
    }
    return true;
}

bool TSocket::SendPlain(const void* msg, size_t len) {
    uint8_t* p = (uint8_t*)msg;
    while (len) {
        ssize_t w = ::send(Fd, p, len, 0);
        if (w <= 0) {
            return false;
        }
        p += (size_t)w;
        len -= (size_t)w;
    }
    return true;
}

bool TSocket::ReceiveTls(void* buf, size_t len) {
    uint8_t* p = (uint8_t*)buf;
    while (len) {
        ssize_t r = SSL_read(Ssl.Get(), p, len);
        if (r <= 0) {
            return false;
        }
        p += (size_t)r;
        len -= (size_t)r;
    }
    return true;
}

bool TSocket::SendTls(const void* msg, size_t len) {
    uint8_t* p = (uint8_t*)msg;
    while (len) {
        ssize_t w = SSL_write(Ssl.Get(), p, len);
        if (w <= 0) {
            return false;
        }
        p += (size_t)w;
        len -= (size_t)w;
    }
    return true;
}

} // LdapMock
