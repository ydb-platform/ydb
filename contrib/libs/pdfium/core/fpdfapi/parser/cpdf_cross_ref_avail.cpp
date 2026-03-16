// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "core/fpdfapi/parser/cpdf_cross_ref_avail.h"

#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_read_validator.h"
#include "core/fpdfapi/parser/cpdf_reference.h"
#include "core/fpdfapi/parser/cpdf_syntax_parser.h"
#include "core/fpdfapi/parser/fpdf_parser_utility.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/containers/contains.h"
#include "core/fxcrt/numerics/safe_conversions.h"

namespace {

constexpr char kCrossRefKeyword[] = "xref";
constexpr char kTrailerKeyword[] = "trailer";
constexpr char kPrevCrossRefFieldKey[] = "Prev";
constexpr char kTypeFieldKey[] = "Type";
constexpr char kPrevCrossRefStreamOffsetFieldKey[] = "XRefStm";
constexpr char kXRefKeyword[] = "XRef";
constexpr char kEncryptKey[] = "Encrypt";

}  // namespace

CPDF_CrossRefAvail::CPDF_CrossRefAvail(CPDF_SyntaxParser* parser,
                                       FX_FILESIZE last_crossref_offset)
    : parser_(parser), last_crossref_offset_(last_crossref_offset) {
  DCHECK(parser_);
  AddCrossRefForCheck(last_crossref_offset);
}

CPDF_CrossRefAvail::~CPDF_CrossRefAvail() = default;

CPDF_DataAvail::DocAvailStatus CPDF_CrossRefAvail::CheckAvail() {
  if (status_ == CPDF_DataAvail::kDataAvailable)
    return CPDF_DataAvail::kDataAvailable;

  CPDF_ReadValidator::ScopedSession read_session(GetValidator());
  while (true) {
    bool check_result = false;
    switch (state_) {
      case State::kCrossRefCheck:
        check_result = CheckCrossRef();
        break;
      case State::kCrossRefTableItemCheck:
        check_result = CheckCrossRefTableItem();
        break;
      case State::kCrossRefTableTrailerCheck:
        check_result = CheckCrossRefTableTrailer();
        break;
      case State::kDone:
        break;
    }
    if (!check_result)
      break;

    DCHECK(!GetValidator()->has_read_problems());
  }
  return status_;
}

bool CPDF_CrossRefAvail::CheckReadProblems() {
  if (GetValidator()->read_error()) {
    status_ = CPDF_DataAvail::kDataError;
    return true;
  }
  return GetValidator()->has_unavailable_data();
}

bool CPDF_CrossRefAvail::CheckCrossRef() {
  if (cross_refs_for_check_.empty()) {
    // All cross refs were checked.
    state_ = State::kDone;
    status_ = CPDF_DataAvail::kDataAvailable;
    return true;
  }
  parser_->SetPos(cross_refs_for_check_.front());

  const ByteString first_word = parser_->PeekNextWord();
  if (CheckReadProblems())
    return false;

  const bool result = (first_word == kCrossRefKeyword) ? CheckCrossRefTable()
                                                       : CheckCrossRefStream();

  if (result)
    cross_refs_for_check_.pop();

  return result;
}

bool CPDF_CrossRefAvail::CheckCrossRefTable() {
  const ByteString keyword = parser_->GetKeyword();
  if (CheckReadProblems())
    return false;

  if (keyword != kCrossRefKeyword) {
    status_ = CPDF_DataAvail::kDataError;
    return false;
  }

  state_ = State::kCrossRefTableItemCheck;
  offset_ = parser_->GetPos();
  return true;
}

bool CPDF_CrossRefAvail::CheckCrossRefTableItem() {
  parser_->SetPos(offset_);
  const ByteString keyword = parser_->GetKeyword();
  if (CheckReadProblems())
    return false;

  if (keyword.IsEmpty()) {
    status_ = CPDF_DataAvail::kDataError;
    return false;
  }

  if (keyword == kTrailerKeyword)
    state_ = State::kCrossRefTableTrailerCheck;

  // Go to next item.
  offset_ = parser_->GetPos();
  return true;
}

bool CPDF_CrossRefAvail::CheckCrossRefTableTrailer() {
  parser_->SetPos(offset_);

  RetainPtr<CPDF_Dictionary> trailer =
      ToDictionary(parser_->GetObjectBody(nullptr));
  if (CheckReadProblems())
    return false;

  if (!trailer) {
    status_ = CPDF_DataAvail::kDataError;
    return false;
  }

  if (ToReference(trailer->GetObjectFor(kEncryptKey))) {
    status_ = CPDF_DataAvail::kDataError;
    return false;
  }

  const int32_t xrefpos = trailer->GetDirectIntegerFor(kPrevCrossRefFieldKey);
  if (xrefpos > 0 &&
      pdfium::IsValueInRangeForNumericType<FX_FILESIZE>(xrefpos)) {
    AddCrossRefForCheck(static_cast<FX_FILESIZE>(xrefpos));
  }

  const int32_t stream_xref_offset =
      trailer->GetDirectIntegerFor(kPrevCrossRefStreamOffsetFieldKey);
  if (stream_xref_offset > 0 &&
      pdfium::IsValueInRangeForNumericType<FX_FILESIZE>(stream_xref_offset)) {
    AddCrossRefForCheck(static_cast<FX_FILESIZE>(stream_xref_offset));
  }

  // Goto check next crossref
  state_ = State::kCrossRefCheck;
  return true;
}

bool CPDF_CrossRefAvail::CheckCrossRefStream() {
  auto cross_ref =
      parser_->GetIndirectObject(nullptr, CPDF_SyntaxParser::ParseType::kLoose);
  if (CheckReadProblems())
    return false;

  RetainPtr<const CPDF_Dictionary> trailer =
      cross_ref && cross_ref->IsStream() ? cross_ref->GetDict() : nullptr;
  if (!trailer) {
    status_ = CPDF_DataAvail::kDataError;
    return false;
  }

  if (ToReference(trailer->GetObjectFor(kEncryptKey))) {
    status_ = CPDF_DataAvail::kDataError;
    return false;
  }

  if (trailer->GetNameFor(kTypeFieldKey) == kXRefKeyword) {
    const int32_t xrefpos = trailer->GetIntegerFor(kPrevCrossRefFieldKey);
    if (xrefpos > 0 &&
        pdfium::IsValueInRangeForNumericType<FX_FILESIZE>(xrefpos)) {
      AddCrossRefForCheck(static_cast<FX_FILESIZE>(xrefpos));
    }
  }
  // Goto check next crossref
  state_ = State::kCrossRefCheck;
  return true;
}

void CPDF_CrossRefAvail::AddCrossRefForCheck(FX_FILESIZE crossref_offset) {
  if (pdfium::Contains(registered_crossrefs_, crossref_offset))
    return;

  cross_refs_for_check_.push(crossref_offset);
  registered_crossrefs_.insert(crossref_offset);
}

RetainPtr<CPDF_ReadValidator> CPDF_CrossRefAvail::GetValidator() {
  return parser_->GetValidator();
}
