// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CORE_FPDFAPI_PARSER_CPDF_READ_VALIDATOR_H_
#define CORE_FPDFAPI_PARSER_CPDF_READ_VALIDATOR_H_

#include "core/fpdfapi/parser/cpdf_data_avail.h"
#include "core/fxcrt/fx_memory.h"
#include "core/fxcrt/fx_stream.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/unowned_ptr.h"

class CPDF_ReadValidator : public IFX_SeekableReadStream {
 public:
  class ScopedSession {
   public:
    FX_STACK_ALLOCATED();

    explicit ScopedSession(RetainPtr<CPDF_ReadValidator> validator);
    ScopedSession(const ScopedSession& that) = delete;
    ScopedSession& operator=(const ScopedSession& that) = delete;
    ~ScopedSession();

   private:
    RetainPtr<CPDF_ReadValidator> const validator_;
    const bool saved_read_error_;
    const bool saved_has_unavailable_data_;
  };

  CONSTRUCT_VIA_MAKE_RETAIN;

  void SetDownloadHints(CPDF_DataAvail::DownloadHints* hints) {
    hints_ = hints;
  }
  bool read_error() const { return read_error_; }
  bool has_unavailable_data() const { return has_unavailable_data_; }
  bool has_read_problems() const {
    return read_error() || has_unavailable_data();
  }

  void ResetErrors();
  bool IsWholeFileAvailable();
  bool CheckDataRangeAndRequestIfUnavailable(FX_FILESIZE offset, size_t size);
  bool CheckWholeFileAndRequestIfUnavailable();

  // IFX_SeekableReadStream overrides:
  bool ReadBlockAtOffset(pdfium::span<uint8_t> buffer,
                         FX_FILESIZE offset) override;
  FX_FILESIZE GetSize() override;

 protected:
  CPDF_ReadValidator(RetainPtr<IFX_SeekableReadStream> file_read,
                     CPDF_DataAvail::FileAvail* file_avail);
  ~CPDF_ReadValidator() override;

 private:
  void ScheduleDownload(FX_FILESIZE offset, size_t size);
  bool IsDataRangeAvailable(FX_FILESIZE offset, size_t size) const;

  RetainPtr<IFX_SeekableReadStream> const file_read_;
  UnownedPtr<CPDF_DataAvail::FileAvail> const file_avail_;
  UnownedPtr<CPDF_DataAvail::DownloadHints> hints_;
  bool read_error_ = false;
  bool has_unavailable_data_ = false;
  bool whole_file_already_available_ = false;
  const FX_FILESIZE file_size_;
};

#endif  // CORE_FPDFAPI_PARSER_CPDF_READ_VALIDATOR_H_
