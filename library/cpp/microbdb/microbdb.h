#pragma once

#include <util/folder/dirut.h>

#if defined(_MSC_VER)
#pragma warning(push)
#pragma warning(disable : 4706) /*assignment within conditional expression*/
#pragma warning(disable : 4267) /*conversion from 'size_t' to 'type', possible loss of data*/
#endif

#include "align.h"
#include "extinfo.h"
#include "header.h"
#include "reader.h"
#include "heap.h"
#include "file.h"
#include "sorter.h"
#include "input.h"
#include "output.h"
#include "sorterdef.h"

inline int MakeSorterTempl(char path[/*FILENAME_MAX*/], const char* prefix) {
    int ret = MakeTempDir(path, prefix);
    if (!ret && strlcat(path, "%06d", FILENAME_MAX) > FILENAME_MAX - 100)
        ret = EINVAL;
    if (ret)
        path[0] = 0;
    return ret;
}

inline int GetMeta(TFile& file, TDatMetaPage* meta) {
    ui8 buf[METASIZE], *ptr = buf;
    ssize_t size = sizeof(buf), ret;
    while (size && (ret = file.Read(ptr, size)) > 0) {
        size -= ret;
        ptr += ret;
    }
    if (size)
        return MBDB_BAD_FILE_SIZE;
    ptr = buf; // gcc 4.4 warning fix
    *meta = *(TDatMetaPage*)ptr;
    return (meta->MetaSig == METASIG) ? 0 : MBDB_BAD_METAPAGE;
}

template <class TRec>
inline bool IsDatFile(const char* fname) {
    TDatMetaPage meta;
    TFile f(fname, RdOnly);
    return !GetMeta(f, &meta) && meta.RecordSig == TRec::RecordSig;
}

#if defined(_MSC_VER)
#pragma warning(pop)
#endif
