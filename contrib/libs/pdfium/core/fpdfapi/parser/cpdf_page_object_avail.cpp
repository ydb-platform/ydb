// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "core/fpdfapi/parser/cpdf_page_object_avail.h"

#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/fpdf_parser_utility.h"

CPDF_PageObjectAvail::~CPDF_PageObjectAvail() = default;

bool CPDF_PageObjectAvail::ExcludeObject(const CPDF_Object* object) const {
  if (CPDF_ObjectAvail::ExcludeObject(object))
    return true;

  // See ISO 32000-1:2008 spec, table 30.
  return ValidateDictType(ToDictionary(object), "Page");
}
