#pragma once

#include "microbdb.h"

template <class TRecord, template <class T> class TCompare>
int SortData(const TFile& ifile, const TFile& ofile, const TDatMetaPage* meta, size_t memory, const char* tmpDir = nullptr) {
    char templ[FILENAME_MAX];
    TInDatFileImpl<TRecord> datin;
    TOutDatFileImpl<TRecord> datout;
    TDatSorterImpl<TRecord, TCompare<TRecord>, TFakeCompression, TFakeSieve<TRecord>> sorter;
    const TRecord* u;
    int ret;

    const size_t minMemory = (2u << 20);
    memory = Max(memory, minMemory + minMemory / 2);
    if (datin.Open(ifile, meta, memory - minMemory, 0))
        err(1, "can't read input file");

    size_t outpages = Max((size_t)2u, minMemory / datin.GetPageSize());
    memory -= outpages * datin.GetPageSize();

    if (ret = MakeSorterTempl(templ, tmpDir))
        err(1, "can't create tempdir in \"%s\"; error: %d\n", templ, ret);

    if (sorter.Open(templ, datin.GetPageSize(), outpages)) {
        *strrchr(templ, LOCSLASH_C) = 0;
        RemoveDirWithContents(templ);
        err(1, "can't open sorter");
    }

    while (1) {
        datin.Freeze();
        while ((u = datin.Next()))
            sorter.PushWithExtInfo(u);
        sorter.NextPortion();
        if (datin.GetError() || datin.IsEof())
            break;
    }

    if (datin.GetError()) {
        *strrchr(templ, LOCSLASH_C) = 0;
        RemoveDirWithContents(templ);
        err(1, "in data file error %d", datin.GetError());
    }
    if (datin.Close()) {
        *strrchr(templ, LOCSLASH_C) = 0;
        RemoveDirWithContents(templ);
        err(1, "can't close in data file");
    }

    sorter.Sort(memory);

    if (datout.Open(ofile, datin.GetPageSize(), outpages)) {
        *strrchr(templ, LOCSLASH_C) = 0;
        RemoveDirWithContents(templ);
        err(1, "can't write out file");
    }

    while ((u = sorter.Next()))
        datout.PushWithExtInfo(u);

    if (sorter.GetError())
        err(1, "sorter error %d", sorter.GetError());
    if (sorter.Close())
        err(1, "can't close sorter");

    *strrchr(templ, LOCSLASH_C) = 0;
    RemoveDirWithContents(templ);

    if (datout.GetError())
        err(1, "out data file error %d", datout.GetError());
    if (datout.Close())
        err(1, "can't close out data file");
    return 0;
}
