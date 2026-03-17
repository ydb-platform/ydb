// Copyright 2021 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "constants/annotation_common.h"

namespace pdfium::annotation {

// PDF 1.7 spec, table 8.15.
// Entries common to all annotation dictionaries.
const char kType[] = "Type";
const char kSubtype[] = "Subtype";
const char kRect[] = "Rect";
const char kContents[] = "Contents";
const char kP[] = "P";
const char kNM[] = "NM";
const char kM[] = "M";
const char kF[] = "F";
const char kAP[] = "AP";
const char kAS[] = "AS";
const char kBorder[] = "Border";
const char kC[] = "C";
const char kStructParent[] = "StructParent";
const char kOC[] = "OC";

// Entries for polygon and polyline annotations.
const char kVertices[] = "Vertices";

// Entries for ink annotations
const char kInkList[] = "InkList";

// Entries for line annotations
const char kL[] = "L";

}  // namespace pdfium::annotation
