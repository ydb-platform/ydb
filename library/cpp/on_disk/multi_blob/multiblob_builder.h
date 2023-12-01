#pragma once

#include <util/system/align.h>
#include <util/stream/output.h>
#include <util/stream/file.h>
#include <util/draft/holder_vector.h>

#include "multiblob.h"

class IBlobSaverBase {
public:
    virtual ~IBlobSaverBase() {
    }
    virtual void Save(IOutputStream& output, ui32 flags = 0) = 0;
    virtual size_t GetLength() = 0;
};

inline void MultiBlobSave(IOutputStream& output, IBlobSaverBase& saver) {
    saver.Save(output);
}

class TBlobSaverMemory: public IBlobSaverBase {
public:
    TBlobSaverMemory(const void* ptr, size_t size);
    TBlobSaverMemory(const TBlob& blob);
    void Save(IOutputStream& output, ui32 flags = 0) override;
    size_t GetLength() override;

private:
    TBlob Blob;
};

class TBlobSaverFile: public IBlobSaverBase {
public:
    TBlobSaverFile(TFile file);
    TBlobSaverFile(const char* filename, EOpenMode oMode = RdOnly);
    void Save(IOutputStream& output, ui32 flags = 0) override;
    size_t GetLength() override;

protected:
    TFile File;
};

class TMultiBlobBuilder: public IBlobSaverBase {
protected:
    // Data will be stored with default alignment DEVTOOLS-4548
    static const size_t ALIGN = 16;

public:
    typedef TVector<IBlobSaverBase*> TSavers;

    TMultiBlobBuilder(bool isOwn = true);
    ~TMultiBlobBuilder() override;
    void Save(IOutputStream& output, ui32 flags = 0) override;
    size_t GetLength() override;
    TSavers& GetBlobs();
    const TSavers& GetBlobs() const;
    void AddBlob(IBlobSaverBase* blob);
    void DeleteSubBlobs();

protected:
    TSavers Blobs;
    bool IsOwner;
};
