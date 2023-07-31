#include "datafile.h"

void TDataFileBase::DoLoad(const char* fname, int loadMode) {
    Destroy();
    TFile f(fname, RdOnly);
    DoLoad(f, loadMode, nullptr, 0);
}

void TDataFileBase::DoLoad(TFile& f, int loadMode, void* hdrPtr, size_t hdrSize) {
    if (hdrPtr) {
        if (loadMode & DLM_EXACT_SIZE && f.GetLength() != (i64)Length)
            throw yexception() << f.GetName() << " size does not match its header value";
    } else {
        Length = f.GetLength();
        hdrSize = 0;
    }
    if ((loadMode & DLM_LD_TYPE_MASK) == DLM_READ) {
        MemData = TVector<char>(Length);
        memcpy(MemData.begin(), hdrPtr, hdrSize);
        f.Load(MemData.begin() + hdrSize, Length - hdrSize);
        Start = MemData.begin();
    } else {
        FileData.init(f);
        if (FileData.getSize() < Length)
            throw yexception() << f.GetName() << " is smaller than what its header value says";
        if ((loadMode & DLM_LD_TYPE_MASK) == DLM_MMAP_PRC)
            FileData.precharge();
        Start = (const char*)FileData.getData();
    }
}

void TDataFileBase::Destroy() {
    TVector<char>().swap(MemData);
    FileData.term();
    Start = nullptr;
    Length = 0;
}

void TDataFileBase::Precharge() const {
    if (Length && Start == (char*)FileData.getData())
        FileData.precharge();
}
