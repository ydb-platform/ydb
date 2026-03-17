// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxge/freetype/fx_freetype.h"

#include <stdint.h>

#include "core/fxcrt/compiler_specific.h"
#include "core/fxge/cfx_fontmgr.h"
#include "core/fxge/cfx_gemodule.h"

#define DEFINE_PS_TABLES
#include "pstables.h"

namespace {

constexpr uint32_t kVariantBit = 0x80000000;

bool SearchNode(pdfium::span<const uint8_t> glyph_span,
                pdfium::span<char> name_buf,
                int name_offset,
                int table_offset,
                wchar_t unicode) {
  // copy letters
  while (true) {
    name_buf[name_offset] = glyph_span[table_offset] & 0x7f;
    name_offset++;
    table_offset++;
    if (!(glyph_span[table_offset - 1] & 0x80)) {
      break;
    }
  }
  name_buf[name_offset] = 0;

  // get child count
  int count = glyph_span[table_offset] & 0x7f;

  // check if we have value for this node
  if (glyph_span[table_offset] & 0x80) {
    unsigned short thiscode =
        glyph_span[table_offset + 1] * 256 + glyph_span[table_offset + 2];
    if (thiscode == (unsigned short)unicode) {  // found it!
      return true;
    }
    table_offset += 3;
  } else {
    table_offset++;
  }

  // now search in sub-nodes
  for (int i = 0; i < count; i++) {
    int child_offset = glyph_span[table_offset + i * 2] * 256 +
                       glyph_span[table_offset + i * 2 + 1];
    if (SearchNode(glyph_span, name_buf, name_offset, child_offset, unicode)) {
      // found in child
      return true;
    }
  }

  return false;
}

FT_MM_Var* GetVariationDescriptor(FXFT_FaceRec* face) {
  FT_MM_Var* variation_desc = nullptr;
  FT_Get_MM_Var(face, &variation_desc);
  return variation_desc;
}

pdfium::span<const FT_Var_Axis> GetVariationAxis(
    const FT_MM_Var* variation_desc) {
  if (!variation_desc) {
    return {};
  }

  // SAFETY: Required from library.
  return UNSAFE_BUFFERS(
      pdfium::make_span(variation_desc->axis, variation_desc->num_axis));
}

}  // namespace

void FXFTMMVarDeleter::operator()(FT_MM_Var* variation_desc) {
  FT_Done_MM_Var(CFX_GEModule::Get()->GetFontMgr()->GetFTLibrary(),
                 variation_desc);
}

ScopedFXFTMMVar::ScopedFXFTMMVar(FXFT_FaceRec* face)
    : variation_desc_(GetVariationDescriptor(face)),
      axis_(GetVariationAxis(variation_desc_.get())) {}

ScopedFXFTMMVar::~ScopedFXFTMMVar() = default;

FT_Pos ScopedFXFTMMVar::GetAxisDefault(size_t index) const {
  return axis_[index].def;
}

FT_Long ScopedFXFTMMVar::GetAxisMin(size_t index) const {
  return axis_[index].minimum;
}

FT_Long ScopedFXFTMMVar::GetAxisMax(size_t index) const {
  return axis_[index].maximum;
}

int FXFT_unicode_from_adobe_name(const char* glyph_name) {
  /* If the name begins with `uni', then the glyph name may be a */
  /* hard-coded unicode character code.                          */
  UNSAFE_TODO({
    if (glyph_name[0] == 'u' && glyph_name[1] == 'n' && glyph_name[2] == 'i') {
      /* determine whether the next four characters following are */
      /* hexadecimal.                                             */

      /* XXX: Add code to deal with ligatures, i.e. glyph names like */
      /*      `uniXXXXYYYYZZZZ'...                                   */

      FT_Int count;
      FT_UInt32 value = 0;
      const char* p = glyph_name + 3;

      for (count = 4; count > 0; count--, p++) {
        char c = *p;
        unsigned int d = (unsigned char)c - '0';
        if (d >= 10) {
          d = (unsigned char)c - 'A';
          if (d >= 6) {
            d = 16;
          } else {
            d += 10;
          }
        }

        /* Exit if a non-uppercase hexadecimal character was found   */
        /* -- this also catches character codes below `0' since such */
        /* negative numbers cast to `unsigned int' are far too big.  */
        if (d >= 16) {
          break;
        }

        value = (value << 4) + d;
      }

      /* there must be exactly four hex digits */
      if (count == 0) {
        if (*p == '\0') {
          return value;
        }
        if (*p == '.') {
          return (FT_UInt32)(value | kVariantBit);
        }
      }
    }

    /* If the name begins with `u', followed by four to six uppercase */
    /* hexadecimal digits, it is a hard-coded unicode character code. */
    if (glyph_name[0] == 'u') {
      FT_Int count;
      FT_UInt32 value = 0;
      const char* p = glyph_name + 1;

      for (count = 6; count > 0; count--, p++) {
        char c = *p;
        unsigned int d = (unsigned char)c - '0';
        if (d >= 10) {
          d = (unsigned char)c - 'A';
          if (d >= 6) {
            d = 16;
          } else {
            d += 10;
          }
        }

        if (d >= 16) {
          break;
        }

        value = (value << 4) + d;
      }

      if (count <= 2) {
        if (*p == '\0') {
          return value;
        }
        if (*p == '.') {
          return (FT_UInt32)(value | kVariantBit);
        }
      }
    }

    /* Look for a non-initial dot in the glyph name in order to */
    /* find variants like `A.swash', `e.final', etc.            */
    {
      const char* p = glyph_name;
      const char* dot = nullptr;

      for (; *p; p++) {
        if (*p == '.' && p > glyph_name) {
          dot = p;
          break;
        }
      }

      /* now look up the glyph in the Adobe Glyph List */
      if (!dot) {
        return (FT_UInt32)ft_get_adobe_glyph_index(glyph_name, p);
      }

      return (FT_UInt32)(ft_get_adobe_glyph_index(glyph_name, dot) |
                         kVariantBit);
    }
  });
}

void FXFT_adobe_name_from_unicode(pdfium::span<char> name_buf,
                                  wchar_t unicode) {
  pdfium::span<const uint8_t> glyph_span(ft_adobe_glyph_list);

  // start from top level node
  int count = glyph_span[1];
  for (int i = 0; i < count; i++) {
    int child_offset = glyph_span[i * 2 + 2] * 256 + glyph_span[i * 2 + 3];
    if (SearchNode(glyph_span, name_buf, 0, child_offset, unicode)) {
      return;
    }
  }

  // failed, clear the buffer
  name_buf[0] = 0;
}
