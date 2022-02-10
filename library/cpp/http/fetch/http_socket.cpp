#include "httpload.h"
#include "http_digest.h"

/************************************************************/

#ifdef USE_GNUTLS

#include <gcrypt.h>
#include <gnutls/gnutls.h>
#include <util/network/init.h>
#include <util/network/socket.h>
#include <util/system/mutex.h>

/********************************************************/
// HTTPS handler is used as implementation of
// socketAbstractHandler for work through HTTPS protocol

class socketSecureHandler: public socketRegularHandler {
protected:
    bool IsValid_;
    gnutls_session Session_;
    gnutls_certificate_credentials Credits_;

public:
    socketSecureHandler();
    virtual ~socketSecureHandler();

    virtual bool Good();
    virtual int Connect(const TAddrList& addrs, TDuration Timeout);
    virtual void Disconnect();
    virtual void shutdown();
    virtual bool send(const char* message, ssize_t messlen);
    virtual bool peek();
    virtual ssize_t read(void* buffer, ssize_t buflen);
};

/********************************************************/
/********************************************************/
static int gcry_pthread_mutex_init(void** priv) {
    int err = 0;

    try {
        TMutex* lock = new TMutex;
        *priv = lock;
    } catch (...) {
        err = -1;
    }

    return err;
}

static int gcry_pthread_mutex_destroy(void** lock) {
    delete static_cast<TMutex*>(*lock);

    return 0;
}

static int gcry_pthread_mutex_lock(void** lock) {
    static_cast<TMutex*>(*lock)->Acquire();

    return 0;
}

static int gcry_pthread_mutex_unlock(void** lock) {
    static_cast<TMutex*>(*lock)->Release();

    return 0;
}

static struct gcry_thread_cbs gcry_threads_pthread =
    {
        GCRY_THREAD_OPTION_PTHREAD, NULL,
        gcry_pthread_mutex_init, gcry_pthread_mutex_destroy,
        gcry_pthread_mutex_lock, gcry_pthread_mutex_unlock,
        NULL, NULL, NULL, NULL,
        NULL, NULL, NULL, NULL};

/********************************************************/
struct https_initor {
    https_initor() {
        gcry_control(GCRYCTL_SET_THREAD_CBS, &gcry_threads_pthread);
        gnutls_global_init();
        InitNetworkSubSystem();
    }

    ~https_initor() {
        gnutls_global_deinit();
    }
};

static https_initor _initor;

/********************************************************/
socketSecureHandler::socketSecureHandler()
    : socketRegularHandler()
    , IsValid_(false)
    , Session_()
    , Credits_()
{
}

/********************************************************/
socketSecureHandler::~socketSecureHandler() {
    if (IsValid_)
        Disconnect();
}

/********************************************************/
bool socketSecureHandler::Good() {
    return Socket_.Good() && IsValid_;
}

/********************************************************/
int socketSecureHandler::Connect(const TAddrList& addrs, TDuration Timeout) {
    IsValid_ = false;

    int ret = socketRegularHandler::Connect(addrs, Timeout);
    if (ret)
        return ret;

    gnutls_certificate_allocate_credentials(&Credits_);
    gnutls_init(&Session_, GNUTLS_CLIENT);
    gnutls_set_default_priority(Session_);
    gnutls_credentials_set(Session_, GNUTLS_CRD_CERTIFICATE, Credits_);

    SOCKET fd = Socket_;
    gnutls_transport_set_ptr(Session_, (gnutls_transport_ptr)fd);

    ret = gnutls_handshake(Session_);

    if (ret < 0) {
        fprintf(stderr, "*** Handshake failed\n");
        gnutls_perror(ret);

        gnutls_deinit(Session_);
        if (Credits_) {
            gnutls_certificate_free_credentials(Credits_);
            Credits_ = 0;
        }
        return 1;
    }

    IsValid_ = true;
    return !IsValid_;
}

/********************************************************/
void socketSecureHandler::Disconnect() {
    if (IsValid_) {
        gnutls_bye(Session_, GNUTLS_SHUT_RDWR);
        IsValid_ = false;
        gnutls_deinit(Session_);
    }

    if (Credits_) {
        gnutls_certificate_free_credentials(Credits_);
        Credits_ = 0;
    }

    socketRegularHandler::Disconnect();
}

/********************************************************/
void socketSecureHandler::shutdown() {
}

/********************************************************/
bool socketSecureHandler::send(const char* message, ssize_t messlen) {
    if (!IsValid_)
        return false;
    ssize_t rv = gnutls_record_send(Session_, message, messlen);
    return rv >= 0;
}

/********************************************************/
bool socketSecureHandler::peek() {
    //ssize_t rv = gnutls_record_check_pending(mSession);
    //return rv>0;
    return true;
}

/********************************************************/
ssize_t socketSecureHandler::read(void* buffer, ssize_t buflen) {
    if (!IsValid_)
        return false;
    return gnutls_record_recv(Session_, (char*)buffer, buflen);
}

#endif

/************************************************************/
socketAbstractHandler* socketHandlerFactory::chooseHandler(const THttpURL& url) {
    if (url.IsValidGlobal() && url.GetScheme() == THttpURL::SchemeHTTP)
        return new socketRegularHandler;

#ifdef USE_GNUTLS
    if (url.IsValidGlobal() && url.GetScheme() == THttpURL::SchemeHTTPS)
        return new socketSecureHandler;
#endif

    return nullptr;
}

/************************************************************/
socketHandlerFactory socketHandlerFactory::sInstance;
/************************************************************/
