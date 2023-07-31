#include "header.h"

#include <util/stream/output.h>
#include <util/stream/format.h>

TString ToString(EMbdbErrors error) {
    TString ret;
    switch (error) {
        case MBDB_ALREADY_INITIALIZED:
            ret = "already initialized";
            break;
        case MBDB_NOT_INITIALIZED:
            ret = "not initialized";
            break;
        case MBDB_BAD_DESCRIPTOR:
            ret = "bad descriptor";
            break;
        case MBDB_OPEN_ERROR:
            ret = "open error";
            break;
        case MBDB_READ_ERROR:
            ret = "read error";
            break;
        case MBDB_WRITE_ERROR:
            ret = "write error";
            break;
        case MBDB_CLOSE_ERROR:
            ret = "close error";
            break;
        case MBDB_EXPECTED_EOF:
            ret = "expected eof";
            break;
        case MBDB_UNEXPECTED_EOF:
            ret = "unxepected eof";
            break;
        case MBDB_BAD_FILENAME:
            ret = "bad filename";
            break;
        case MBDB_BAD_METAPAGE:
            ret = "bad metapage";
            break;
        case MBDB_BAD_RECORDSIG:
            ret = "bad recordsig";
            break;
        case MBDB_BAD_FILE_SIZE:
            ret = "bad file size";
            break;
        case MBDB_BAD_PAGESIG:
            ret = "bad pagesig";
            break;
        case MBDB_BAD_PAGESIZE:
            ret = "bad pagesize";
            break;
        case MBDB_BAD_PARM:
            ret = "bad parm";
            break;
        case MBDB_BAD_SYNC:
            ret = "bad sync";
            break;
        case MBDB_PAGE_OVERFLOW:
            ret = "page overflow";
            break;
        case MBDB_NO_MEMORY:
            ret = "no memory";
            break;
        case MBDB_MEMORY_LEAK:
            ret = "memory leak";
            break;
        case MBDB_NOT_SUPPORTED:
            ret = "not supported";
            break;
        default:
            ret = "unknown";
            break;
    }
    return ret;
}

TString ErrorMessage(int error, const TString& text, const TString& path, ui32 recordSig, ui32 gotRecordSig) {
    TStringStream str;
    str << text;
    if (path.size())
        str << " '" << path << "'";
    str << ": " << ToString(static_cast<EMbdbErrors>(error));
    if (recordSig && (!gotRecordSig || recordSig != gotRecordSig))
        str << ". Expected RecordSig: " << Hex(recordSig, HF_ADDX);
    if (recordSig && gotRecordSig && recordSig != gotRecordSig)
        str << ", got: " << Hex(gotRecordSig, HF_ADDX);
    str << ". Last system error text: " << LastSystemErrorText();
    return str.Str();
}
