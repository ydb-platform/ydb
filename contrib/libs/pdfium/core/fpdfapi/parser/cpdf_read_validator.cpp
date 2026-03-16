// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "core/fpdfapi/parser/cpdf_read_validator.h"

#include <algorithm>
#include <utility>

#include "core/fpdfapi/parser/cpdf_stream.h"
#include "core/fxcrt/fx_safe_types.h"
#include "core/fxcrt/notreached.h"

namespace {

constexpr FX_FILESIZE kAlignBlockValue = CPDF_Stream::kFileBufSize;

FX_FILESIZE AlignDown(FX_FILESIZE offset) {
  return offset > 0 ? (offset - offset % kAlignBlockValue) : 0;
}

FX_FILESIZE AlignUp(FX_FILESIZE offset) {
  FX_SAFE_FILESIZE safe_result = AlignDown(offset);
  safe_result += kAlignBlockValue;
  return safe_result.ValueOrDefault(offset);
}

}  // namespace

CPDF_ReadValidator::ScopedSession::ScopedSession(
    RetainPtr<CPDF_ReadValidator> validator)
    : validator_(std::move(validator)),
      saved_read_error_(validator_->read_error_),
      saved_has_unavailable_data_(validator_->has_unavailable_data_) {
  validator_->ResetErrors();
}

CPDF_ReadValidator::ScopedSession::~ScopedSession() {
  validator_->read_error_ |= saved_read_error_;
  validator_->has_unavailable_data_ |= saved_has_unavailable_data_;
}

CPDF_ReadValidator::CPDF_ReadValidator(
    RetainPtr<IFX_SeekableReadStream> file_read,
    CPDF_DataAvail::FileAvail* file_avail)
    : file_read_(std::move(file_read)),
      file_avail_(file_avail),
      file_size_(file_read_->GetSize()) {}

CPDF_ReadValidator::~CPDF_ReadValidator() = default;

void CPDF_ReadValidator::ResetErrors() {
  read_error_ = false;
  has_unavailable_data_ = false;
}

bool CPDF_ReadValidator::ReadBlockAtOffset(pdfium::span<uint8_t> buffer,
                                           FX_FILESIZE offset) {
  if (offset < 0) {
    NOTREACHED();
    return false;
  }

  FX_SAFE_FILESIZE end_offset = offset;
  end_offset += buffer.size();
  if (!end_offset.IsValid() || end_offset.ValueOrDie() > file_size_)
    return false;

  if (!IsDataRangeAvailable(offset, buffer.size())) {
    ScheduleDownload(offset, buffer.size());
    return false;
  }

  if (file_read_->ReadBlockAtOffset(buffer, offset))
    return true;

  read_error_ = true;
  ScheduleDownload(offset, buffer.size());
  return false;
}

FX_FILESIZE CPDF_ReadValidator::GetSize() {
  return file_size_;
}

void CPDF_ReadValidator::ScheduleDownload(FX_FILESIZE offset, size_t size) {
  has_unavailable_data_ = true;
  if (!hints_ || size == 0)
    return;

  const FX_FILESIZE start_segment_offset = AlignDown(offset);
  FX_SAFE_FILESIZE end_segment_offset = offset;
  end_segment_offset += size;
  if (!end_segment_offset.IsValid()) {
    NOTREACHED();
    return;
  }
  end_segment_offset =
      std::min(file_size_, AlignUp(end_segment_offset.ValueOrDie()));

  FX_SAFE_SIZE_T segment_size = end_segment_offset;
  segment_size -= start_segment_offset;
  if (!segment_size.IsValid()) {
    NOTREACHED();
    return;
  }
  hints_->AddSegment(start_segment_offset, segment_size.ValueOrDie());
}

bool CPDF_ReadValidator::IsDataRangeAvailable(FX_FILESIZE offset,
                                              size_t size) const {
  return whole_file_already_available_ || !file_avail_ ||
         file_avail_->IsDataAvail(offset, size);
}

bool CPDF_ReadValidator::IsWholeFileAvailable() {
  const FX_SAFE_SIZE_T safe_size = file_size_;
  whole_file_already_available_ =
      whole_file_already_available_ ||
      (safe_size.IsValid() && IsDataRangeAvailable(0, safe_size.ValueOrDie()));

  return whole_file_already_available_;
}

bool CPDF_ReadValidator::CheckDataRangeAndRequestIfUnavailable(
    FX_FILESIZE offset,
    size_t size) {
  if (offset > file_size_)
    return true;

  FX_SAFE_FILESIZE end_segment_offset = offset;
  end_segment_offset += size;
  // Increase checked range to allow CPDF_SyntaxParser read whole buffer.
  end_segment_offset += CPDF_Stream::kFileBufSize;
  if (!end_segment_offset.IsValid()) {
    NOTREACHED();
    return false;
  }
  end_segment_offset = std::min(
      file_size_, static_cast<FX_FILESIZE>(end_segment_offset.ValueOrDie()));
  FX_SAFE_SIZE_T segment_size = end_segment_offset;
  segment_size -= offset;
  if (!segment_size.IsValid()) {
    NOTREACHED();
    return false;
  }

  if (IsDataRangeAvailable(offset, segment_size.ValueOrDie()))
    return true;

  ScheduleDownload(offset, segment_size.ValueOrDie());
  return false;
}

bool CPDF_ReadValidator::CheckWholeFileAndRequestIfUnavailable() {
  if (IsWholeFileAvailable())
    return true;

  const FX_SAFE_SIZE_T safe_size = file_size_;
  if (safe_size.IsValid())
    ScheduleDownload(0, safe_size.ValueOrDie());

  return false;
}
