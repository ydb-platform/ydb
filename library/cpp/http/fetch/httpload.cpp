#include "httpload.h"

/************************************************************/
/************************************************************/
httpAgentReader::httpAgentReader(httpSpecialAgent& agent,
                                 const char* baseUrl,
                                 bool assumeConnectionClosed,
                                 bool use_auth,
                                 int bufSize)
    : Header_()
    , Agent_(agent)
    , Buffer_(new char[bufSize])
    , BufPtr_(Buffer_)
    , BufSize_(bufSize)
    , BufRest_(0)
{
    HeadRequest = false;
    Header = &Header_;
    if (use_auth)
        HeaderParser.Init(&Header_);
    else
        HeaderParser.Init(Header);
    setAssumeConnectionClosed(assumeConnectionClosed ? 1 : 0);
    Header_.SetBase(baseUrl);

    if (Header_.error)
        State = hp_error;
    else
        State = hp_in_header;
}

/************************************************************/
httpAgentReader::~httpAgentReader() {
    delete[] Buffer_;
}

/************************************************************/
void httpAgentReader::readBuf() {
    assert(BufRest_ == 0);
    if (!BufPtr_) {
        BufRest_ = -1;
        return;
    }

    BufRest_ = Agent_.read(Buffer_, BufSize_);
    if (BufRest_ <= 0) {
        BufRest_ = -1;
        BufPtr_ = nullptr;
    } else {
        BufPtr_ = Buffer_;

        //cout << "BUF: " << mBuffer << endl << endl;
    }
}

/************************************************************/
const THttpHeader* httpAgentReader::readHeader() {
    while (State == hp_in_header) {
        if (!step()) {
            Header_.error = HTTP_CONNECTION_LOST;
            return nullptr;
        }
        ParseGeneric(BufPtr_, BufRest_);
    }
    if (State == hp_eof || State == hp_error) {
        BufPtr_ = nullptr;
        BufRest_ = -1;
    }
    if (State == hp_error || Header_.error)
        return nullptr;
    return &Header_;
}

/************************************************************/
long httpAgentReader::readPortion(void*& buf) {
    assert(State != hp_in_header);

    long Chunk = 0;
    do {
        if (BufSize_ == 0 && !BufPtr_)
            return 0;

        if (!step())
            return 0;

        Chunk = ParseGeneric(BufPtr_, BufRest_);
        buf = BufPtr_;

        if (State == hp_error && Header_.entity_size > Header_.content_length) {
            Chunk -= (Header_.entity_size - Header_.content_length);
            BufPtr_ = (char*)BufPtr_ + Chunk;
            BufRest_ = 0;
            State = hp_eof;
            Header_.error = 0;
            break;
        }

        BufPtr_ = (char*)BufPtr_ + Chunk;
        BufRest_ -= Chunk;

        if (State == hp_eof || State == hp_error) {
            BufRest_ = -1;
            BufPtr_ = nullptr;
        }
    } while (!Chunk);
    return Chunk;
}

/************************************************************/
bool httpAgentReader::skipTheRest() {
    void* b;
    while (!eof())
        readPortion(b);
    return (State == hp_eof);
}

/************************************************************/
/************************************************************/
httpLoadAgent::httpLoadAgent(bool handleAuthorization,
                             socketHandlerFactory& factory)
    : Factory_(factory)
    , HandleAuthorization_(handleAuthorization)
    , URL_()
    , PersistentConn_(false)
    , Reader_(nullptr)
    , Headers_()
    , ErrCode_(0)
    , RealHost_(nullptr)
{
}

/************************************************************/
httpLoadAgent::~httpLoadAgent() {
    delete Reader_;
    free(RealHost_);
}

/************************************************************/
void httpLoadAgent::clearReader() {
    if (Reader_) {
        bool opened = false;
        if (PersistentConn_) {
            const THttpHeader* H = Reader_->readHeader();
            if (H && !H->connection_closed) {
                Reader_->skipTheRest();
                opened = true;
            }
        }
        if (!opened)
            Disconnect();
        delete Reader_;
        Reader_ = nullptr;
    }
    ErrCode_ = 0;
}
/************************************************************/
void httpLoadAgent::setRealHost(const char* hostname) {
    free(RealHost_);
    if (hostname)
        RealHost_ = strdup(hostname);
    else
        RealHost_ = nullptr;
    ErrCode_ = 0;
}

/************************************************************/
void httpLoadAgent::setIMS(const char* ifModifiedSince) {
    char ims_buf[100];
    snprintf(ims_buf, 100, "If-Modified-Since: %s\r\n",
             ifModifiedSince);
    Headers_.push_back(ims_buf);
}

/************************************************************/
void httpLoadAgent::addHeaderInstruction(const char* instr) {
    Headers_.push_back(instr);
}

/************************************************************/
void httpLoadAgent::dropHeaderInstructions() {
    Headers_.clear();
}

/************************************************************/
bool httpLoadAgent::startRequest(const THttpURL& url,
                                 bool persistent,
                                 const TAddrList& addrs)

{
    clearReader();
    ErrCode_ = 0;

    URL_.Clear();
    URL_ = url;
    PersistentConn_ = persistent;
    if (!URL_.IsValidAbs())
        return false;
    if (!HandleAuthorization_ && !URL_.IsNull(THttpURL::FlagAuth))
        return false;

    return doSetHost(addrs) && doStartRequest();
}

/************************************************************/
bool httpLoadAgent::startRequest(const char* url,
                                 const char* url_to_merge,
                                 bool persistent,
                                 const TAddrList& addrs) {
    clearReader();

    URL_.Clear();
    PersistentConn_ = persistent;

    ui64 flags = THttpURL::FeatureSchemeKnown | THttpURL::FeaturesNormalizeSet;
    if (HandleAuthorization_)
        flags |= THttpURL::FeatureAuthSupported;

    if (URL_.Parse(url, flags, url_to_merge) || !URL_.IsValidGlobal())
        return false;

    return doSetHost(addrs) && doStartRequest();
}

/************************************************************/
bool httpLoadAgent::startRequest(const char* url,
                                 const char* url_to_merge,
                                 bool persistent,
                                 ui32 ip) {
    clearReader();

    URL_.Clear();
    PersistentConn_ = persistent;

    ui64 flags = THttpURL::FeatureSchemeKnown | THttpURL::FeaturesNormalizeSet;
    if (HandleAuthorization_)
        flags |= THttpURL::FeatureAuthSupported;

    if (URL_.Parse(url, flags, url_to_merge) || !URL_.IsValidGlobal())
        return false;

    return doSetHost(TAddrList::MakeV4Addr(ip, URL_.GetPort())) && doStartRequest();
}

/************************************************************/
bool httpLoadAgent::doSetHost(const TAddrList& addrs) {
    socketAbstractHandler* h = Factory_.chooseHandler(URL_);
    if (!h)
        return false;
    Socket.setHandler(h);

    if (addrs.size()) {
        ErrCode_ = SetHost(URL_.Get(THttpURL::FieldHost),
                           URL_.GetPort(), addrs);
    } else {
        ErrCode_ = SetHost(URL_.Get(THttpURL::FieldHost),
                           URL_.GetPort());
    }
    if (ErrCode_)
        return false;

    if (RealHost_) {
        size_t reqHostheaderLen = strlen(RealHost_) + 20;
        free(Hostheader);
        Hostheader = (char*)malloc((HostheaderLen = reqHostheaderLen));
        snprintf(Hostheader, HostheaderLen, "Host: %s\r\n", RealHost_);
    }

    if (!URL_.IsNull(THttpURL::FlagAuth)) {
        if (!HandleAuthorization_) {
            ErrCode_ = HTTP_UNAUTHORIZED;
            return false;
        }

        Digest_.setAuthorization(URL_.Get(THttpURL::FieldUsername),
                                 URL_.Get(THttpURL::FieldPassword));
    }

    return true;
}

/************************************************************/
bool httpLoadAgent::setHost(const char* host_url,
                            const TAddrList& addrs) {
    clearReader();

    URL_.Clear();
    PersistentConn_ = true;

    ui64 flags = THttpURL::FeatureSchemeKnown | THttpURL::FeaturesNormalizeSet;
    if (HandleAuthorization_)
        flags |= THttpURL::FeatureAuthSupported;

    if (URL_.Parse(host_url, flags) || !URL_.IsValidGlobal())
        return false;

    return doSetHost(addrs);
}

/************************************************************/
bool httpLoadAgent::startOneRequest(const char* local_url) {
    clearReader();

    THttpURL lURL;
    if (lURL.Parse(local_url, THttpURL::FeaturesNormalizeSet) || lURL.IsValidGlobal())
        return false;

    URL_.SetInMemory(THttpURL::FieldPath, lURL.Get(THttpURL::FieldPath));
    URL_.SetInMemory(THttpURL::FieldQuery, lURL.Get(THttpURL::FieldQuery));
    URL_.Rewrite();

    return doStartRequest();
}

/************************************************************/
bool httpLoadAgent::doStartRequest() {
    TString urlStr = URL_.PrintS(THttpURL::FlagPath | THttpURL::FlagQuery);
    if (!urlStr)
        urlStr = "/";

    for (int step = 0; step < 10; step++) {
        const char* digestHeader = Digest_.getHeaderInstruction();

        unsigned i = (digestHeader) ? 2 : 1;
        const char** headers =
            (const char**)(alloca((i + Headers_.size()) * sizeof(char*)));

        for (i = 0; i < Headers_.size(); i++)
            headers[i] = Headers_[i].c_str();
        if (digestHeader)
            headers[i++] = digestHeader;
        headers[i] = nullptr;

        ErrCode_ = RequestGet(urlStr.c_str(), headers, PersistentConn_);

        if (ErrCode_) {
            Disconnect();
            return false;
        }

        TString urlBaseStr = URL_.PrintS(THttpURL::FlagNoFrag);

        clearReader();
        Reader_ = new httpAgentReader(*this, urlBaseStr.c_str(),
                                      !PersistentConn_, !Digest_.empty());

        if (Reader_->readHeader()) {
            //mReader->getHeader()->Print();
            if (getHeader()->http_status == HTTP_UNAUTHORIZED &&
                step < 1 &&
                Digest_.processHeader(getAuthHeader(),
                                      urlStr.c_str(),
                                      "GET")) {
                //mReader->skipTheRest();
                delete Reader_;
                Reader_ = nullptr;
                ErrCode_ = 0;
                Disconnect();
                continue;
            }

            return true;
        }
        Disconnect();
        clearReader();

        return false;
    }

    ErrCode_ = HTTP_UNAUTHORIZED;
    return false;
}

/************************************************************/
/************************************************************/
