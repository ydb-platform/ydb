#pragma once

#include "httpfsm.h"
#include "httpheader.h"

#include <library/cpp/mime/types/mime.h>
#include <util/system/yassert.h>
#include <library/cpp/http/misc/httpcodes.h>

template <size_t headermax = 100 << 10, size_t bodymax = 1 << 20>
struct TFakeCheck {
    bool Check(THttpHeader* /*header*/) {
        return false;
    }
    void CheckDocPart(void* /*buf*/, size_t /*len*/, THttpHeader* /*header*/) {
    } //for every part of DocumentBody will be called
    void CheckEndDoc(THttpHeader* /*header*/) {
    }
    size_t GetMaxHeaderSize() {
        return headermax;
    }
    size_t GetMaxBodySize(THttpHeader*) {
        return bodymax;
    }
};

class THttpParserBase {
public:
    enum States {
        hp_error,
        hp_eof,
        hp_in_header,
        hp_read_alive,
        hp_read_closed,
        hp_begin_chunk_header,
        hp_chunk_header,
        hp_read_chunk
    };

    States GetState() {
        return State;
    }

    void setAssumeConnectionClosed(int value) {
        AssumeConnectionClosed = value;
    }

    THttpHeader* GetHttpHeader() const {
        return Header;
    }

protected:
    int CheckHeaders() {
        if (Header->http_status < HTTP_OK || Header->http_status == HTTP_NO_CONTENT || Header->http_status == HTTP_NOT_MODIFIED) {
            Header->content_length = 0;
            Header->transfer_chunked = 0;
        }
        if (Header->transfer_chunked < -1) {
            Header->error = HTTP_BAD_ENCODING;
            return 1;
        } else if (Header->transfer_chunked == -1) {
            Header->transfer_chunked = 0;
        }
        if (!Header->transfer_chunked && Header->content_length < -1) {
            Header->error = HTTP_BAD_CONTENT_LENGTH;
            return 1;
        }
        if (Header->http_status == HTTP_OK) {
            if (Header->compression_method != HTTP_COMPRESSION_UNSET &&
                Header->compression_method != HTTP_COMPRESSION_IDENTITY &&
                Header->compression_method != HTTP_COMPRESSION_GZIP &&
                Header->compression_method != HTTP_COMPRESSION_DEFLATE)
            {
                Header->error = HTTP_BAD_CONTENT_ENCODING;
                return 1;
            }
        }
        if (Header->connection_closed == -1)
            Header->connection_closed = (Header->http_minor == 0 ||
                                         AssumeConnectionClosed);
        if (!Header->transfer_chunked && !Header->connection_closed && Header->content_length < 0 && !HeadRequest) {
            Header->error = HTTP_LENGTH_UNKNOWN;
            return 1;
        }
        if (Header->http_time < 0)
            Header->http_time = 0;
        if (Header->mime_type < 0)
            Header->mime_type = MIME_UNKNOWN;
        return 0;
    }

    THttpHeaderParser HeaderParser;
    THttpChunkParser ChunkParser;
    States State;
    long ChunkSize;
    THttpHeader* Header;
    int AssumeConnectionClosed;
    bool HeadRequest;
};

template <int isReader, typename TCheck = TFakeCheck<>>
class THttpParserGeneric: public THttpParserBase, public TCheck {
protected:
    long ParseGeneric(void*& buf, long& size) {
        if (!size) {
            switch (State) {
                case hp_error:
                case hp_eof:
                    break;
                case hp_read_closed:
                    State = hp_eof;
                    break;
                case hp_in_header:
                    Header->error = HTTP_HEADER_EOF;
                    State = hp_error;
                    break;
                case hp_read_alive:
                case hp_read_chunk:
                    if (HeadRequest)
                        State = hp_eof;
                    else {
                        Header->error = HTTP_MESSAGE_EOF;
                        State = hp_error;
                    }
                    break;
                case hp_begin_chunk_header:
                case hp_chunk_header:
                    if (HeadRequest)
                        State = hp_eof;
                    else {
                        Header->error = HTTP_CHUNK_EOF;
                        State = hp_error;
                    }
                    break;
            }
            return 0;
        }
        while (size) {
            int ret;

            switch (State) {
                case hp_error:
                    return 0;

                case hp_eof:
                    return 0;

                case hp_in_header:
                    if ((ret = HeaderParser.Execute(buf, size)) < 0) {
                        Header->error = HTTP_BAD_HEADER_STRING;
                        State = hp_error;
                        return 0;
                    } else if (ret == 2) {
                        Header->header_size += i32(HeaderParser.lastchar - (char*)buf + 1);
                        size -= long(HeaderParser.lastchar - (char*)buf + 1);
                        buf = HeaderParser.lastchar + 1;
                        State = CheckHeaders() ? hp_error
                                               : Header->transfer_chunked ? hp_begin_chunk_header
                                                                          : Header->content_length == 0 ? hp_eof
                                                                                                        : Header->content_length > 0 ? hp_read_alive
                                                                                                                                     : hp_read_closed;
                        if (State == hp_begin_chunk_header) {
                            // unget \n for chunk reader
                            buf = (char*)buf - 1;
                            size++;
                        }
                        if (isReader)
                            return size;
                    } else {
                        Header->header_size += size;
                        size = 0;
                    }
                    break;

                case hp_read_alive:
                    Header->entity_size += size;
                    if (Header->entity_size >= Header->content_length) {
                        State = hp_eof;
                    }

                    TCheck::CheckDocPart(buf, size, Header);
                    if (isReader)
                        return size;
                    size = 0;
                    break;

                case hp_read_closed:
                    Header->entity_size += size;
                    TCheck::CheckDocPart(buf, size, Header);
                    if (isReader)
                        return size;
                    size = 0;
                    break;

                case hp_begin_chunk_header:
                    ChunkParser.Init();
                    State = hp_chunk_header;
                    [[fallthrough]];

                case hp_chunk_header:
                    if ((ret = ChunkParser.Execute(buf, size)) < 0) {
                        Header->error = i16(ret == -2 ? HTTP_CHUNK_TOO_LARGE : HTTP_BAD_CHUNK);
                        State = hp_error;
                        return 0;
                    } else if (ret == 2) {
                        Header->entity_size += i32(ChunkParser.lastchar - (char*)buf + 1);
                        size -= long(ChunkParser.lastchar - (char*)buf + 1);
                        buf = ChunkParser.lastchar + 1;
                        ChunkSize = ChunkParser.chunk_length;
                        Y_ASSERT(ChunkSize >= 0);
                        State = ChunkSize ? hp_read_chunk : hp_eof;
                    } else {
                        Header->entity_size += size;
                        size = 0;
                    }
                    break;

                case hp_read_chunk:
                    if (size >= ChunkSize) {
                        Header->entity_size += ChunkSize;
                        State = hp_begin_chunk_header;
                        TCheck::CheckDocPart(buf, ChunkSize, Header);
                        if (isReader)
                            return ChunkSize;
                        size -= ChunkSize;
                        buf = (char*)buf + ChunkSize;
                    } else {
                        Header->entity_size += size;
                        ChunkSize -= size;
                        TCheck::CheckDocPart(buf, size, Header);
                        if (isReader)
                            return size;
                        size = 0;
                    }
                    break;
            }
        }
        return size;
    }
};

template <class TCheck = TFakeCheck<>>
class THttpParser: public THttpParserGeneric<0, TCheck> {
    typedef THttpParserGeneric<0, TCheck> TBaseT; //sorry avoiding gcc 3.4.6 BUG!
public:
    void Init(THttpHeader* H, bool head_request = false) {
        TBaseT::Header = H;
        TBaseT::HeaderParser.Init(TBaseT::Header);
        TBaseT::State = TBaseT::hp_in_header;
        TBaseT::AssumeConnectionClosed = 0;
        TBaseT::HeadRequest = head_request;
    }

    void Parse(void* buf, long size) {
        TBaseT::ParseGeneric(buf, size);
    }
};

class TMemoReader {
public:
    int Init(void* buf, long bufsize) {
        Buf = buf;
        Bufsize = bufsize;
        return 0;
    }
    long Read(void*& buf) {
        Y_ASSERT(Bufsize >= 0);
        if (!Bufsize) {
            Bufsize = -1;
            return 0;
        }
        buf = Buf;
        long ret = Bufsize;
        Bufsize = 0;
        return ret;
    }

protected:
    long Bufsize;
    void* Buf;
};

template <class Reader>
class THttpReader: public THttpParserGeneric<1>, public Reader {
    typedef THttpParserGeneric<1> TBaseT;

public:
    using TBaseT::AssumeConnectionClosed;
    using TBaseT::Header;
    using TBaseT::ParseGeneric;
    using TBaseT::State;

    int Init(THttpHeader* H, int parsHeader, int assumeConnectionClosed = 0, bool headRequest = false) {
        Header = H;
        Eoferr = 1;
        Size = 0;
        AssumeConnectionClosed = assumeConnectionClosed;
        HeadRequest = headRequest;
        return parsHeader ? ParseHeader() : SkipHeader();
    }

    long Read(void*& buf) {
        long Chunk;
        do {
            if (!Size) {
                if (Eoferr != 1)
                    return Eoferr;
                else if ((Size = (long)Reader::Read(Ptr)) < 0) {
                    Header->error = HTTP_CONNECTION_LOST;
                    return Eoferr = -1;
                }
            }
            Chunk = ParseGeneric(Ptr, Size);
            buf = Ptr;
            Ptr = (char*)Ptr + Chunk;
            Size -= Chunk;
            if (State == hp_eof) {
                Size = 0;
                Eoferr = 0;
            } else if (State == hp_error)
                return Eoferr = -1;
        } while (!Chunk);
        return Chunk;
    }

protected:
    int ParseHeader() {
        HeaderParser.Init(Header);
        State = hp_in_header;
        while (State == hp_in_header) {
            if ((Size = (long)Reader::Read(Ptr)) < 0)
                return Eoferr = -1;
            ParseGeneric(Ptr, Size);
        }
        if (State == hp_error)
            return Eoferr = -1;
        if (State == hp_eof)
            Eoferr = 0;
        return 0;
    }

    int SkipHeader() {
        long hdrsize = Header->header_size;
        while (hdrsize) {
            if ((Size = (long)Reader::Read(Ptr)) <= 0)
                return Eoferr = -1;
            if (Size >= hdrsize) {
                Size -= hdrsize;
                Ptr = (char*)Ptr + hdrsize;
                break;
            }
            hdrsize -= Size;
        }
        State = Header->transfer_chunked ? hp_begin_chunk_header
                                         : Header->content_length == 0 ? hp_eof
                                                                       : Header->content_length > 0 ? hp_read_alive
                                                                                                    : hp_read_closed;
        Header->entity_size = 0;
        if (State == hp_eof)
            Eoferr = 0;
        else if (State == hp_begin_chunk_header) {
            // unget \n for chunk reader
            Ptr = (char*)Ptr - 1;
            ++Size;
        }
        return 0;
    }

    void* Ptr;
    long Size;
    int Eoferr;
};
