// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CORE_FPDFAPI_PARSER_CPDF_CROSS_REF_AVAIL_H_
#define CORE_FPDFAPI_PARSER_CPDF_CROSS_REF_AVAIL_H_

#include <queue>
#include <set>

#include "core/fpdfapi/parser/cpdf_data_avail.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/unowned_ptr.h"

class CPDF_SyntaxParser;

class CPDF_CrossRefAvail {
 public:
  CPDF_CrossRefAvail(CPDF_SyntaxParser* parser,
                     FX_FILESIZE last_crossref_offset);
  ~CPDF_CrossRefAvail();

  FX_FILESIZE last_crossref_offset() const { return last_crossref_offset_; }

  CPDF_DataAvail::DocAvailStatus CheckAvail();

 private:
  enum class State {
    kCrossRefCheck,
    kCrossRefTableItemCheck,
    kCrossRefTableTrailerCheck,
    kDone,
  };

  bool CheckReadProblems();
  bool CheckCrossRef();
  bool CheckCrossRefTable();
  bool CheckCrossRefTableItem();
  bool CheckCrossRefTableTrailer();
  bool CheckCrossRefStream();

  void AddCrossRefForCheck(FX_FILESIZE crossref_offset);

  RetainPtr<CPDF_ReadValidator> GetValidator();

  UnownedPtr<CPDF_SyntaxParser> const parser_;
  const FX_FILESIZE last_crossref_offset_;
  CPDF_DataAvail::DocAvailStatus status_ = CPDF_DataAvail::kDataNotAvailable;
  State state_ = State::kCrossRefCheck;
  FX_FILESIZE offset_ = 0;
  std::queue<FX_FILESIZE> cross_refs_for_check_;
  std::set<FX_FILESIZE> registered_crossrefs_;
};

#endif  // CORE_FPDFAPI_PARSER_CPDF_CROSS_REF_AVAIL_H_
