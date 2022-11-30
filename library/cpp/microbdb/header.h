#pragma once

#include <util/system/defaults.h>
#include <util/generic/typetraits.h>
#include <util/generic/string.h>
#include <util/str_stl.h>

#include <stdio.h>

#define METASIZE (1u << 12)
#define METASIG 0x12345678u
#define PAGESIG 0x87654321u

enum EMbdbErrors {
    MBDB_ALREADY_INITIALIZED = 200,
    MBDB_NOT_INITIALIZED = 201,
    MBDB_BAD_DESCRIPTOR = 202,
    MBDB_OPEN_ERROR = 203,
    MBDB_READ_ERROR = 204,
    MBDB_WRITE_ERROR = 205,
    MBDB_CLOSE_ERROR = 206,
    MBDB_EXPECTED_EOF = 207,
    MBDB_UNEXPECTED_EOF = 208,
    MBDB_BAD_FILENAME = 209,
    MBDB_BAD_METAPAGE = 210,
    MBDB_BAD_RECORDSIG = 211,
    MBDB_BAD_FILE_SIZE = 212,
    MBDB_BAD_PAGESIG = 213,
    MBDB_BAD_PAGESIZE = 214,
    MBDB_BAD_PARM = 215,
    MBDB_BAD_SYNC = 216,
    MBDB_PAGE_OVERFLOW = 217,
    MBDB_NO_MEMORY = 218,
    MBDB_MEMORY_LEAK = 219,
    MBDB_NOT_SUPPORTED = 220
};

TString ToString(EMbdbErrors error);
TString ErrorMessage(int error, const TString& text, const TString& path = TString(), ui32 recordSig = 0, ui32 gotRecordSig = 0);

enum EPageFormat {
    MBDB_FORMAT_RAW = 0,
    MBDB_FORMAT_COMPRESSED = 1,
    MBDB_FORMAT_NULL = 255
};

enum ECompressionAlgorithm {
    MBDB_COMPRESSION_ZLIB = 1,
    MBDB_COMPRESSION_FASTLZ = 2,
    MBDB_COMPRESSION_SNAPPY = 3
};

struct TDatMetaPage {
    ui32 MetaSig;
    ui32 RecordSig;
    ui32 PageSize;
};

struct TDatPage {
    ui32 RecNum; //!< number of records on this page
    ui32 PageSig;
    ui32 Format : 2; //!< one of EPageFormat
    ui32 Reserved : 30;
};

/// Additional page header with compression info
struct TCompressedPage {
    ui32 BlockCount;
    ui32 Algorithm : 4;
    ui32 Version : 4;
    ui32 Reserved : 24;
};

namespace NMicroBDB {
    /// Header of compressed block
    struct TCompressedHeader {
        ui32 Compressed;
        ui32 Original; /// original size of block
        ui32 Count;    /// number of records in block
        ui32 Reserved;
    };

    Y_HAS_MEMBER(AssertValid);

    template <typename T, bool TVal>
    struct TAssertValid {
        void operator()(const T*) {
        }
    };

    template <typename T>
    struct TAssertValid<T, true> {
        void operator()(const T* rec) {
            return rec->AssertValid();
        }
    };

    template <typename T>
    void AssertValid(const T* rec) {
        return NMicroBDB::TAssertValid<T, NMicroBDB::THasAssertValid<T>::value>()(rec);
    }

    Y_HAS_MEMBER(SizeOf);

    template <typename T, bool TVal>
    struct TGetSizeOf;

    template <typename T>
    struct TGetSizeOf<T, true> {
        size_t operator()(const T* rec) {
            return rec->SizeOf();
        }
    };

    template <typename T>
    struct TGetSizeOf<T, false> {
        size_t operator()(const T*) {
            return sizeof(T);
        }
    };

    inline char* GetFirstRecord(const TDatPage* page) {
        switch (page->Format) {
            case MBDB_FORMAT_RAW:
                return (char*)page + sizeof(TDatPage);
            case MBDB_FORMAT_COMPRESSED:
                // Первая запись на сжатой странице сохраняется несжатой
                // сразу же после всех заголовков.
                // Алгоритм сохранения смотреть в TOutputRecordIterator::FlushBuffer
                return (char*)page + sizeof(TDatPage) + sizeof(TCompressedPage) + sizeof(NMicroBDB::TCompressedHeader);
        }
        return (char*)nullptr;
    }
}

template <typename T>
size_t SizeOf(const T* rec) {
    return NMicroBDB::TGetSizeOf<T, NMicroBDB::THasSizeOf<T>::value>()(rec);
}

template <typename T>
size_t MaxSizeOf() {
    return sizeof(T);
}

static inline int DatNameToIdx(char iname[/*FILENAME_MAX*/], const char* dname) {
    if (!dname || !*dname)
        return MBDB_BAD_FILENAME;
    const char* ptr;
    if (!(ptr = strrchr(dname, '/')))
        ptr = dname;
    if (!(ptr = strrchr(ptr, '.')))
        ptr = strchr(dname, 0);
    if (ptr - dname > FILENAME_MAX - 5)
        return MBDB_BAD_FILENAME;
    memcpy(iname, dname, ptr - dname);
    strcpy(iname + (ptr - dname), ".idx");
    return 0;
}
