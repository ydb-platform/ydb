// Copyright 2021 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "constants/form_fields.h"

namespace pdfium::form_fields {

// ISO 32000-1:2008 table 220.
// Entries common to all field dictionaries.
const char kFT[] = "FT";
const char kParent[] = "Parent";
const char kKids[] = "Kids";
const char kT[] = "T";
const char kTU[] = "TU";
const char kTM[] = "TM";
const char kFf[] = "Ff";
const char kV[] = "V";
const char kDV[] = "DV";
const char kAA[] = "AA";

// ISO 32000-1:2008 table 220.
// Values for FT keyword.
const char kBtn[] = "Btn";
const char kTx[] = "Tx";
const char kCh[] = "Ch";
const char kSig[] = "Sig";

// ISO 32000-1:2008 table 222.
// Entries common to fields containing variable text.
const char kDA[] = "DA";
const char kQ[] = "Q";
const char kDS[] = "DS";
const char kRV[] = "RV";

}  // namespace pdfium::form_fields
