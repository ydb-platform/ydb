#pragma once

#include "loadmode.h"

#include <library/cpp/deprecated/mapped_file/mapped_file.h>

#include <util/generic/vector.h>
#include <util/system/file.h>
#include <util/system/filemap.h>

/** Simple helper that allows a file to be either mapped or read into malloc'ed memory.
    This behaviour is controlled by EDataLoadMode enum defined in loadmode.h.
    Unlike TBlob it provides Precharge() function and simple file size - based integrity check.

    To use this code, inherit your class from TDataFile<TFileHeader>.
    TFileHeader must be a pod-type structure with byte layout of the file header.
    File must start with that header.
    TFileHeader must have FileSize() member function that determines expected file size or
    length of data that need to be read from the beginning of file.
 */

class TDataFileBase {
protected:
    TVector<char> MemData;
    TMappedFile FileData;

    const char* Start;
    size_t Length;

    TDataFileBase()
        : Start(nullptr)
        , Length(0)
    {
    }

    void DoLoad(TFile& f, int loadMode, void* hdrPtr, size_t hdrSize);
    void DoLoad(const char* fname, int loadMode); // just whole file
    void Destroy();
    void swap(TDataFileBase& with) {
        MemData.swap(with.MemData);
        FileData.swap(with.FileData);
        DoSwap(Start, with.Start);
        DoSwap(Length, with.Length);
    }

public:
    void Precharge() const;
};

template <class TFileHeader>
class TDataFile: public TDataFileBase {
protected:
    void Load(const char* fname, EDataLoadMode loadMode) {
        Destroy();
        TFile f(fname, RdOnly | Seq);
        TFileHeader hdr;
        f.Load(&hdr, sizeof(hdr));
        Length = hdr.FileSize();
        DoLoad(f, (int)loadMode, &hdr, sizeof(hdr));
    }
    const TFileHeader& Hdr() const {
        return *(TFileHeader*)Start;
    }
};

// Use: class TFoo: public TDataFileEx<Foo> {...};
// Additional requrement: TFileHeader must have Validate(fname) function that throws exception.
// Class TUser itself must have Init(fname) function
// Adds Load() function to your class (TUser)
template <class TUser, class TFileHeader>
class TDataFileEx: public TDataFile<TFileHeader> {
private:
    using TBase = TDataFile<TFileHeader>;
    TUser& User() const {
        return *(TUser*)this;
    }

public:
    TDataFileEx(const char* fname, EDataLoadMode loadMode = DLM_DEFAULT) {
        if (fname)
            Load(fname, loadMode);
    }
    void Load(const char* fname, EDataLoadMode loadMode = DLM_DEFAULT) {
        TBase::Load(fname, loadMode);
        TBase::Hdr().Validate(fname);
        User().Init(fname);
    }
};
