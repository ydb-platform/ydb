#pragma once

#include "httpagent.h"
#include "httpparser.h"
#include "http_digest.h"

#include <util/system/compat.h>
#include <util/string/vector.h>
#include <util/network/ip.h>
#include <library/cpp/uri/http_url.h>
#include <library/cpp/http/misc/httpcodes.h>

/********************************************************/
// Section 1: socket handlers
/********************************************************/
// The following classes allows to adopt template scheme
// THttpAgent for work with socket by flexible
// object-style scheme.

/********************************************************/
// This class is used as a base one for flexible
// socket handling
class socketAbstractHandler {
public:
    virtual bool Good() = 0;

    virtual int Connect(const TAddrList& addrs, TDuration Timeout) = 0;

    virtual void Disconnect() = 0;

    virtual void shutdown() = 0;

    virtual bool send(const char* message, ssize_t messlen) = 0;

    virtual bool peek() = 0;

    virtual ssize_t read(void* buffer, ssize_t buflen) = 0;

    virtual ~socketAbstractHandler() {
    }

protected:
    socketAbstractHandler() {
    }
};

/********************************************************/
// This class is used as a proxy between THttpAgent and
// socketAbstractHandler
// (it is used by template scheme,
//  so it does not have virtual methods)
class TSocketHandlerPtr {
protected:
    socketAbstractHandler* Handler_;

public:
    TSocketHandlerPtr()
        : Handler_(nullptr)
    {
    }

    virtual ~TSocketHandlerPtr() {
        delete Handler_;
    }

    int Good() {
        return (Handler_ && Handler_->Good());
    }

    int Connect(const TAddrList& addrs, TDuration Timeout) {
        return (Handler_) ? Handler_->Connect(addrs, Timeout) : 1;
    }

    void Disconnect() {
        if (Handler_)
            Handler_->Disconnect();
    }

    void shutdown() {
        if (Handler_)
            Handler_->shutdown();
    }

    bool send(const char* message, ssize_t messlen) {
        return (Handler_) ? Handler_->send(message, messlen) : false;
    }

    virtual bool peek() {
        return (Handler_) ? Handler_->peek() : false;
    }

    virtual ssize_t read(void* buffer, ssize_t buflen) {
        return (Handler_) ? Handler_->read(buffer, buflen) : 0;
    }

    void setHandler(socketAbstractHandler* handler) {
        if (Handler_)
            delete Handler_;
        Handler_ = handler;
    }
};

/********************************************************/
// Here is httpAgent that uses socketAbstractHandler class
// ant its derivatives
using httpSpecialAgent = THttpAgent<TSocketHandlerPtr>;

/********************************************************/
// Regular handler is used as implementation of
// socketAbstractHandler for work through HTTP protocol
class socketRegularHandler: public socketAbstractHandler {
protected:
    TSimpleSocketHandler Socket_;

public:
    socketRegularHandler()
        : Socket_()
    {
    }

    bool Good() override {
        return Socket_.Good();
    }

    int Connect(const TAddrList& addrs, TDuration Timeout) override {
        return Socket_.Connect(addrs, Timeout);
    }

    void Disconnect() override {
        Socket_.Disconnect();
    }

    void shutdown() override {
        //Do not block writing to socket
        //There are servers that works in a bad way with this
        //mSocket.shutdown();
    }

    bool send(const char* message, ssize_t messlen) override {
        return Socket_.send(message, messlen);
    }

    bool peek() override {
        return Socket_.peek();
    }

    ssize_t read(void* buffer, ssize_t buflen) override {
        return Socket_.read(buffer, buflen);
    }
};

/********************************************************/
// The base factory that allows to choose an appropriate
// socketAbstractHandler implementation by url schema

class socketHandlerFactory {
public:
    virtual ~socketHandlerFactory() {
    }

    //returns mHandler_HTTP for correct HTTP-based url
    virtual socketAbstractHandler* chooseHandler(const THttpURL& url);

    static socketHandlerFactory sInstance;
};

/********************************************************/
// Section 2: the configurates tool to parse an HTTP-response
/********************************************************/

class httpAgentReader: public THttpParserGeneric<1> {
protected:
    THttpAuthHeader Header_;
    httpSpecialAgent& Agent_;

    char* Buffer_;
    void* BufPtr_;
    int BufSize_;
    long BufRest_;

    void readBuf();

    bool step() {
        if (BufRest_ == 0)
            readBuf();
        if (eof())
            return false;
        return true;
    }

public:
    httpAgentReader(httpSpecialAgent& agent,
                    const char* baseUrl,
                    bool assumeConnectionClosed,
                    bool use_auth = false,
                    int bufSize = 0x1000);

    ~httpAgentReader();

    bool eof() {
        return BufRest_ < 0;
    }

    int error() {
        return Header_.error;
    }

    void setError(int errCode) {
        Header_.error = errCode;
    }

    const THttpAuthHeader* getAuthHeader() {
        return &Header_;
    }

    const THttpHeader* readHeader();
    long readPortion(void*& buf);
    bool skipTheRest();
};

/********************************************************/
// Section 3: the main class
/********************************************************/
class httpLoadAgent: public httpSpecialAgent {
protected:
    socketHandlerFactory& Factory_;
    bool HandleAuthorization_;
    THttpURL URL_;
    bool PersistentConn_;
    httpAgentReader* Reader_;
    TVector<TString> Headers_;
    int ErrCode_;
    char* RealHost_;
    httpDigestHandler Digest_;

    void clearReader();
    bool doSetHost(const TAddrList& addrs);
    bool doStartRequest();

public:
    httpLoadAgent(bool handleAuthorization = false,
                  socketHandlerFactory& factory = socketHandlerFactory::sInstance);
    ~httpLoadAgent();

    void setRealHost(const char* host);
    void setIMS(const char* ifModifiedSince);
    void addHeaderInstruction(const char* instr);
    void dropHeaderInstructions();

    bool startRequest(const char* url,
                      const char* url_to_merge = nullptr,
                      bool persistent = false,
                      const TAddrList& addrs = TAddrList());

    // deprecated v4-only
    bool startRequest(const char* url,
                      const char* url_to_merge,
                      bool persistent,
                      ui32 ip);

    bool startRequest(const THttpURL& url,
                      bool persistent = false,
                      const TAddrList& addrs = TAddrList());

    bool setHost(const char* host_url,
                 const TAddrList& addrs = TAddrList());

    bool startOneRequest(const char* local_url);

    const THttpAuthHeader* getAuthHeader() {
        if (Reader_ && Reader_->getAuthHeader()->use_auth)
            return Reader_->getAuthHeader();
        return nullptr;
    }

    const THttpHeader* getHeader() {
        if (Reader_)
            return Reader_->getAuthHeader();
        return nullptr;
    }

    const THttpURL& getURL() {
        return URL_;
    }

    bool eof() {
        if (Reader_)
            return Reader_->eof();
        return true;
    }

    int error() {
        if (ErrCode_)
            return ErrCode_;
        if (Reader_)
            return Reader_->error();
        return HTTP_BAD_URL;
    }

    long readPortion(void*& buf) {
        if (Reader_)
            return Reader_->readPortion(buf);
        return -1;
    }
};

/********************************************************/
