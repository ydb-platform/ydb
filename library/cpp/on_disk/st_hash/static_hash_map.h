#pragma once

#include "static_hash.h"

#include <library/cpp/deprecated/mapped_file/mapped_file.h>

#include <util/system/filemap.h>

template <class SH>
struct sthash_mapped_c {
    typedef SH H;
    typedef typename H::const_iterator const_iterator;
    TMappedFile M;
    H* hsh;
    sthash_mapped_c()
        : M()
        , hsh(nullptr)
    {
    }
    sthash_mapped_c(const char* fname, bool precharge)
        : M()
        , hsh(nullptr)
    {
        Open(fname, precharge);
    }
    void Open(const char* fname, bool precharge) {
        M.init(fname);
        if (precharge)
            M.precharge();
        hsh = (H*)M.getData();
        if (M.getSize() < sizeof(H) || (ssize_t)M.getSize() != hsh->end().Data - (char*)hsh)
            ythrow yexception() << "Could not map hash: " << fname << " is damaged";
    }
    H* operator->() {
        return hsh;
    }
    const H* operator->() const {
        return hsh;
    }
    H* GetSthash() {
        return hsh;
    }
    const H* GetSthash() const {
        return hsh;
    }
};

template <class Key, class T, class Hash>
struct sthash_mapped: public sthash_mapped_c<sthash<Key, T, Hash>> {
    typedef sthash<Key, T, Hash> H;
    sthash_mapped(const char* fname, bool precharge)
        : sthash_mapped_c<H>(fname, precharge)
    {
    }
    sthash_mapped()
        : sthash_mapped_c<H>()
    {
    }
};
