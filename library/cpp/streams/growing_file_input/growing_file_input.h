#pragma once

#include <util/stream/input.h>
#include <util/system/file.h>

/**
 * Growing file input stream.
 *
 * File descriptor offsets to the end of the file, when the object is created.
 *
 * Read function waites for reading at least one byte.
 */
class TGrowingFileInput: public IInputStream {
public:
    TGrowingFileInput(const TFile& file);
    TGrowingFileInput(const TString& path);

private:
    size_t DoRead(void* buf, size_t len) override;

private:
    TFile File_;
};
