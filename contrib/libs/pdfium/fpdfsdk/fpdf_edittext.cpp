// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <map>
#include <memory>
#include <sstream>
#include <utility>
#include <vector>

#include "core/fpdfapi/font/cpdf_cidfont.h"
#include "core/fpdfapi/font/cpdf_font.h"
#include "core/fpdfapi/page/cpdf_docpagedata.h"
#include "core/fpdfapi/page/cpdf_textobject.h"
#include "core/fpdfapi/page/cpdf_textstate.h"
#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_document.h"
#include "core/fpdfapi/parser/cpdf_name.h"
#include "core/fpdfapi/parser/cpdf_number.h"
#include "core/fpdfapi/parser/cpdf_reference.h"
#include "core/fpdfapi/parser/cpdf_stream.h"
#include "core/fpdfapi/parser/cpdf_string.h"
#include "core/fpdfapi/render/charposlist.h"
#include "core/fpdfapi/render/cpdf_pagerendercontext.h"
#include "core/fpdfapi/render/cpdf_rendercontext.h"
#include "core/fpdfapi/render/cpdf_renderstatus.h"
#include "core/fpdfapi/render/cpdf_textrenderer.h"
#include "core/fpdftext/cpdf_textpage.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/check_op.h"
#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/containers/contains.h"
#include "core/fxcrt/fx_extension.h"
#include "core/fxcrt/fx_memcpy_wrappers.h"
#include "core/fxcrt/fx_string_wrappers.h"
#include "core/fxcrt/numerics/safe_conversions.h"
#include "core/fxcrt/span_util.h"
#include "core/fxcrt/stl_util.h"
#include "core/fxcrt/utf16.h"
#include "core/fxge/cfx_defaultrenderdevice.h"
#include "core/fxge/cfx_fontmgr.h"
#include "core/fxge/dib/cfx_dibitmap.h"
#include "core/fxge/fx_font.h"
#include "core/fxge/text_char_pos.h"
#include "fpdfsdk/cpdfsdk_helpers.h"
#include "public/fpdf_edit.h"

// These checks are here because core/ and public/ cannot depend on each other.
static_assert(static_cast<int>(TextRenderingMode::MODE_UNKNOWN) ==
                  FPDF_TEXTRENDERMODE_UNKNOWN,
              "TextRenderingMode::MODE_UNKNOWN value mismatch");
static_assert(static_cast<int>(TextRenderingMode::MODE_FILL) ==
                  FPDF_TEXTRENDERMODE_FILL,
              "TextRenderingMode::MODE_FILL value mismatch");
static_assert(static_cast<int>(TextRenderingMode::MODE_STROKE) ==
                  FPDF_TEXTRENDERMODE_STROKE,
              "TextRenderingMode::MODE_STROKE value mismatch");
static_assert(static_cast<int>(TextRenderingMode::MODE_FILL_STROKE) ==
                  FPDF_TEXTRENDERMODE_FILL_STROKE,
              "TextRenderingMode::MODE_FILL_STROKE value mismatch");
static_assert(static_cast<int>(TextRenderingMode::MODE_INVISIBLE) ==
                  FPDF_TEXTRENDERMODE_INVISIBLE,
              "TextRenderingMode::MODE_INVISIBLE value mismatch");
static_assert(static_cast<int>(TextRenderingMode::MODE_FILL_CLIP) ==
                  FPDF_TEXTRENDERMODE_FILL_CLIP,
              "TextRenderingMode::MODE_FILL_CLIP value mismatch");
static_assert(static_cast<int>(TextRenderingMode::MODE_STROKE_CLIP) ==
                  FPDF_TEXTRENDERMODE_STROKE_CLIP,
              "TextRenderingMode::MODE_STROKE_CLIP value mismatch");
static_assert(static_cast<int>(TextRenderingMode::MODE_FILL_STROKE_CLIP) ==
                  FPDF_TEXTRENDERMODE_FILL_STROKE_CLIP,
              "TextRenderingMode::MODE_FILL_STROKE_CLIP value mismatch");
static_assert(static_cast<int>(TextRenderingMode::MODE_CLIP) ==
                  FPDF_TEXTRENDERMODE_CLIP,
              "TextRenderingMode::MODE_CLIP value mismatch");
static_assert(static_cast<int>(TextRenderingMode::MODE_LAST) ==
                  FPDF_TEXTRENDERMODE_LAST,
              "TextRenderingMode::MODE_LAST value mismatch");

namespace {

ByteString BaseFontNameForType(const CFX_Font* font, int font_type) {
  ByteString name = font_type == FPDF_FONT_TYPE1 ? font->GetPsName()
                                                 : font->GetBaseFontName();
  return name.IsEmpty() ? CFX_Font::kUntitledFontName : name;
}

RetainPtr<CPDF_Dictionary> CreateCompositeFontDict(CPDF_Document* doc,
                                                   const CFX_Font* font,
                                                   int font_type,
                                                   const ByteString& name) {
  auto font_dict = doc->NewIndirect<CPDF_Dictionary>();
  font_dict->SetNewFor<CPDF_Name>("Type", "Font");
  font_dict->SetNewFor<CPDF_Name>("Subtype", "Type0");
  // TODO(npm): Get the correct encoding, if it's not identity.
  ByteString encoding = "Identity-H";
  font_dict->SetNewFor<CPDF_Name>("Encoding", encoding);
  font_dict->SetNewFor<CPDF_Name>(
      "BaseFont", font_type == FPDF_FONT_TYPE1 ? name + "-" + encoding : name);
  return font_dict;
}

RetainPtr<CPDF_Dictionary> CreateCidFontDict(CPDF_Document* doc,
                                             int font_type,
                                             const ByteString& name) {
  auto cid_font_dict = doc->NewIndirect<CPDF_Dictionary>();
  cid_font_dict->SetNewFor<CPDF_Name>("Type", "Font");
  cid_font_dict->SetNewFor<CPDF_Name>("Subtype", font_type == FPDF_FONT_TYPE1
                                                     ? "CIDFontType0"
                                                     : "CIDFontType2");
  cid_font_dict->SetNewFor<CPDF_Name>("BaseFont", name);

  // TODO(npm): Maybe use FT_Get_CID_Registry_Ordering_Supplement to get the
  // CIDSystemInfo
  auto cid_system_info_dict = doc->NewIndirect<CPDF_Dictionary>();
  cid_system_info_dict->SetNewFor<CPDF_String>("Registry", "Adobe");
  cid_system_info_dict->SetNewFor<CPDF_String>("Ordering", "Identity");
  cid_system_info_dict->SetNewFor<CPDF_Number>("Supplement", 0);
  cid_font_dict->SetNewFor<CPDF_Reference>("CIDSystemInfo", doc,
                                           cid_system_info_dict->GetObjNum());
  return cid_font_dict;
}

RetainPtr<CPDF_Dictionary> LoadFontDesc(CPDF_Document* doc,
                                        const ByteString& font_name,
                                        CFX_Font* font,
                                        pdfium::span<const uint8_t> font_data,
                                        int font_type) {
  auto font_descriptor_dict = doc->NewIndirect<CPDF_Dictionary>();
  font_descriptor_dict->SetNewFor<CPDF_Name>("Type", "FontDescriptor");
  font_descriptor_dict->SetNewFor<CPDF_Name>("FontName", font_name);
  int flags = 0;
  if (font->GetFace()->IsFixedWidth()) {
    flags |= FXFONT_FIXED_PITCH;
  }
  if (font_name.Contains("Serif"))
    flags |= FXFONT_SERIF;
  if (font->GetFace()->IsItalic()) {
    flags |= FXFONT_ITALIC;
  }
  if (font->GetFace()->IsBold()) {
    flags |= FXFONT_FORCE_BOLD;
  }

  // TODO(npm): How do I know if a font is symbolic, script, allcap, smallcap?
  flags |= FXFONT_NONSYMBOLIC;

  font_descriptor_dict->SetNewFor<CPDF_Number>("Flags", flags);
  FX_RECT bbox = font->GetBBox().value_or(FX_RECT());
  font_descriptor_dict->SetRectFor("FontBBox", CFX_FloatRect(bbox));

  // TODO(npm): calculate italic angle correctly
  font_descriptor_dict->SetNewFor<CPDF_Number>("ItalicAngle",
                                               font->IsItalic() ? -12 : 0);

  font_descriptor_dict->SetNewFor<CPDF_Number>("Ascent", font->GetAscent());
  font_descriptor_dict->SetNewFor<CPDF_Number>("Descent", font->GetDescent());

  // TODO(npm): calculate the capheight, stemV correctly
  font_descriptor_dict->SetNewFor<CPDF_Number>("CapHeight", font->GetAscent());
  font_descriptor_dict->SetNewFor<CPDF_Number>("StemV",
                                               font->IsBold() ? 120 : 70);

  auto stream = doc->NewIndirect<CPDF_Stream>(font_data);
  // TODO(npm): Lengths for Type1 fonts.
  if (font_type == FPDF_FONT_TRUETYPE) {
    stream->GetMutableDict()->SetNewFor<CPDF_Number>(
        "Length1", pdfium::checked_cast<int>(font_data.size()));
  }
  ByteString font_file_key =
      font_type == FPDF_FONT_TYPE1 ? "FontFile" : "FontFile2";
  font_descriptor_dict->SetNewFor<CPDF_Reference>(font_file_key, doc,
                                                  stream->GetObjNum());
  return font_descriptor_dict;
}

RetainPtr<CPDF_Array> CreateWidthsArray(
    CPDF_Document* doc,
    const std::map<uint32_t, uint32_t>& widths) {
  auto widths_array = doc->NewIndirect<CPDF_Array>();
  for (auto it = widths.begin(); it != widths.end(); ++it) {
    int ch = it->first;
    int w = it->second;
    if (std::next(it) == widths.end()) {
      // Only one char left, use format c [w]
      auto single_w_array = pdfium::MakeRetain<CPDF_Array>();
      single_w_array->AppendNew<CPDF_Number>(w);
      widths_array->AppendNew<CPDF_Number>(ch);
      widths_array->Append(std::move(single_w_array));
      break;
    }
    ++it;
    int next_ch = it->first;
    int next_w = it->second;
    if (next_ch == ch + 1 && next_w == w) {
      // The array can have a group c_first c_last w: all CIDs in the range from
      // c_first to c_last will have width w
      widths_array->AppendNew<CPDF_Number>(ch);
      ch = next_ch;
      while (true) {
        auto next_it = std::next(it);
        if (next_it == widths.end() || next_it->first != it->first + 1 ||
            next_it->second != it->second) {
          break;
        }
        ++it;
        ch = it->first;
      }
      widths_array->AppendNew<CPDF_Number>(ch);
      widths_array->AppendNew<CPDF_Number>(w);
      continue;
    }
    // Otherwise we can have a group of the form c [w1 w2 ...]: c has width
    // w1, c+1 has width w2, etc.
    widths_array->AppendNew<CPDF_Number>(ch);
    auto current_width_array = pdfium::MakeRetain<CPDF_Array>();
    current_width_array->AppendNew<CPDF_Number>(w);
    current_width_array->AppendNew<CPDF_Number>(next_w);
    while (true) {
      auto next_it = std::next(it);
      if (next_it == widths.end() || next_it->first != it->first + 1) {
        break;
      }
      ++it;
      current_width_array->AppendNew<CPDF_Number>(static_cast<int>(it->second));
    }
    widths_array->Append(std::move(current_width_array));
  }
  return widths_array;
}

const char kToUnicodeStart[] =
    "/CIDInit /ProcSet findresource begin\n"
    "12 dict begin\n"
    "begincmap\n"
    "/CIDSystemInfo\n"
    "<</Registry (Adobe)\n"
    "/Ordering (Identity)\n"
    "/Supplement 0\n"
    ">> def\n"
    "/CMapName /Adobe-Identity-H def\n"
    "/CMapType 2 def\n"
    "1 begincodespacerange\n"
    "<0000> <FFFF>\n"
    "endcodespacerange\n";

const char kToUnicodeEnd[] =
    "endcmap\n"
    "CMapName currentdict /CMap defineresource pop\n"
    "end\n"
    "end\n";

void AddCharcode(fxcrt::ostringstream& buffer, uint32_t number) {
  CHECK_LE(number, 0xFFFF);
  buffer << "<";
  char ans[4];
  FXSYS_IntToFourHexChars(number, ans);
  for (char c : ans) {
    buffer << c;
  }
  buffer << ">";
}

// PDF spec 1.7 Section 5.9.2: "Unicode character sequences as expressed in
// UTF-16BE encoding." See https://en.wikipedia.org/wiki/UTF-16#Description
void AddUnicode(fxcrt::ostringstream& buffer, uint32_t unicode) {
  if (pdfium::IsHighSurrogate(unicode) || pdfium::IsLowSurrogate(unicode)) {
    unicode = 0;
  }

  char ans[8];
  size_t char_count = FXSYS_ToUTF16BE(unicode, ans);
  buffer << "<";
  CHECK_LE(char_count, std::size(ans));
  auto ans_span = pdfium::make_span(ans).first(char_count);
  for (char c : ans_span) {
    buffer << c;
  }
  buffer << ">";
}

// Loads the charcode to unicode mapping into a stream
RetainPtr<CPDF_Stream> LoadUnicode(
    CPDF_Document* doc,
    const std::multimap<uint32_t, uint32_t>& to_unicode) {
  // A map charcode->unicode
  std::map<uint32_t, uint32_t> char_to_uni;
  // A map <char_start, char_end> to vector v of unicode characters of size (end
  // - start + 1). This abbreviates: start->v[0], start+1->v[1], etc. PDF spec
  // 1.7 Section 5.9.2 says that only the last byte of the unicode may change.
  std::map<std::pair<uint32_t, uint32_t>, std::vector<uint32_t>>
      map_range_vector;
  // A map <start, end> -> unicode
  // This abbreviates: start->unicode, start+1->unicode+1, etc.
  // PDF spec 1.7 Section 5.9.2 says that only the last byte of the unicode may
  // change.
  std::map<std::pair<uint32_t, uint32_t>, uint32_t> map_range;

  // Calculate the maps
  for (auto it = to_unicode.begin(); it != to_unicode.end(); ++it) {
    uint32_t first_charcode = it->first;
    uint32_t first_unicode = it->second;
    {
      auto next_it = std::next(it);
      if (next_it == to_unicode.end() || first_charcode + 1 != next_it->first) {
        char_to_uni[first_charcode] = first_unicode;
        continue;
      }
    }
    ++it;
    uint32_t current_charcode = it->first;
    uint32_t current_unicode = it->second;
    if (current_charcode % 256 == 0) {
      char_to_uni[first_charcode] = first_unicode;
      char_to_uni[current_charcode] = current_unicode;
      continue;
    }
    const size_t max_extra = 255 - (current_charcode % 256);
    auto next_it = std::next(it);
    if (first_unicode + 1 != current_unicode) {
      // Consecutive charcodes mapping to non-consecutive unicodes
      std::vector<uint32_t> unicodes = {first_unicode, current_unicode};
      for (size_t i = 0; i < max_extra; ++i) {
        if (next_it == to_unicode.end() ||
            current_charcode + 1 != next_it->first) {
          break;
        }
        ++it;
        ++current_charcode;
        unicodes.push_back(it->second);
        next_it = std::next(it);
      }
      CHECK_EQ(it->first - first_charcode + 1, unicodes.size());
      map_range_vector[std::make_pair(first_charcode, it->first)] = unicodes;
      continue;
    }
    // Consecutive charcodes mapping to consecutive unicodes
    for (size_t i = 0; i < max_extra; ++i) {
      if (next_it == to_unicode.end() ||
          current_charcode + 1 != next_it->first ||
          current_unicode + 1 != next_it->second) {
        break;
      }
      ++it;
      ++current_charcode;
      ++current_unicode;
      next_it = std::next(it);
    }
    map_range[std::make_pair(first_charcode, current_charcode)] = first_unicode;
  }

  fxcrt::ostringstream buffer;
  buffer << kToUnicodeStart;
  // Add maps to buffer
  buffer << static_cast<uint32_t>(char_to_uni.size()) << " beginbfchar\n";
  for (const auto& it : char_to_uni) {
    AddCharcode(buffer, it.first);
    buffer << " ";
    AddUnicode(buffer, it.second);
    buffer << "\n";
  }
  buffer << "endbfchar\n"
         << static_cast<uint32_t>(map_range_vector.size() + map_range.size())
         << " beginbfrange\n";
  for (const auto& it : map_range_vector) {
    const std::pair<uint32_t, uint32_t>& charcode_range = it.first;
    AddCharcode(buffer, charcode_range.first);
    buffer << " ";
    AddCharcode(buffer, charcode_range.second);
    buffer << " [";
    const std::vector<uint32_t>& unicodes = it.second;
    for (size_t i = 0; i < unicodes.size(); ++i) {
      AddUnicode(buffer, unicodes[i]);
      if (i != unicodes.size() - 1)
        buffer << " ";
    }
    buffer << "]\n";
  }
  for (const auto& it : map_range) {
    const std::pair<uint32_t, uint32_t>& charcode_range = it.first;
    AddCharcode(buffer, charcode_range.first);
    buffer << " ";
    AddCharcode(buffer, charcode_range.second);
    buffer << " ";
    AddUnicode(buffer, it.second);
    buffer << "\n";
  }
  buffer << "endbfrange\n";
  buffer << kToUnicodeEnd;
  auto stream = doc->NewIndirect<CPDF_Stream>(&buffer);
  return stream;
}

void CreateDescendantFontsArray(CPDF_Document* doc,
                                CPDF_Dictionary* font_dict,
                                uint32_t cid_font_dict_obj_num) {
  auto descendant_fonts_dict =
      font_dict->SetNewFor<CPDF_Array>("DescendantFonts");
  descendant_fonts_dict->AppendNew<CPDF_Reference>(doc, cid_font_dict_obj_num);
}

RetainPtr<CPDF_Font> LoadSimpleFont(CPDF_Document* doc,
                                    std::unique_ptr<CFX_Font> font,
                                    pdfium::span<const uint8_t> font_data,
                                    int font_type) {
  // If it doesn't have a single char, just fail.
  RetainPtr<CFX_Face> face = font->GetFace();
  if (face->GetGlyphCount() <= 0) {
    return nullptr;
  }

  // Simple fonts have 1-byte charcodes only.
  static constexpr uint32_t kMaxSimpleFontChar = 0xFF;
  auto char_codes_and_indices =
      face->GetCharCodesAndIndices(kMaxSimpleFontChar);
  if (char_codes_and_indices.empty()) {
    return nullptr;
  }

  auto font_dict = doc->NewIndirect<CPDF_Dictionary>();
  font_dict->SetNewFor<CPDF_Name>("Type", "Font");
  font_dict->SetNewFor<CPDF_Name>(
      "Subtype", font_type == FPDF_FONT_TYPE1 ? "Type1" : "TrueType");
  const ByteString name = BaseFontNameForType(font.get(), font_type);
  font_dict->SetNewFor<CPDF_Name>("BaseFont", name);

  font_dict->SetNewFor<CPDF_Number>(
      "FirstChar", static_cast<int>(char_codes_and_indices[0].char_code));
  auto widths_array = doc->NewIndirect<CPDF_Array>();
  for (size_t i = 0; i < char_codes_and_indices.size(); ++i) {
    widths_array->AppendNew<CPDF_Number>(
        font->GetGlyphWidth(char_codes_and_indices[i].glyph_index));
    if (i > 0 && i < char_codes_and_indices.size() - 1) {
      for (uint32_t j = char_codes_and_indices[i - 1].char_code + 1;
           j < char_codes_and_indices[i].char_code; ++j) {
        widths_array->AppendNew<CPDF_Number>(0);
      }
    }
  }
  font_dict->SetNewFor<CPDF_Number>(
      "LastChar", static_cast<int>(char_codes_and_indices.back().char_code));
  font_dict->SetNewFor<CPDF_Reference>("Widths", doc,
                                       widths_array->GetObjNum());
  RetainPtr<CPDF_Dictionary> font_descriptor_dict =
      LoadFontDesc(doc, name, font.get(), font_data, font_type);

  font_dict->SetNewFor<CPDF_Reference>("FontDescriptor", doc,
                                       font_descriptor_dict->GetObjNum());
  return CPDF_DocPageData::FromDocument(doc)->GetFont(std::move(font_dict));
}

RetainPtr<CPDF_Font> LoadCompositeFont(CPDF_Document* doc,
                                       std::unique_ptr<CFX_Font> font,
                                       pdfium::span<const uint8_t> font_data,
                                       int font_type) {
  // If it doesn't have a single char, just fail.
  RetainPtr<CFX_Face> face = font->GetFace();
  if (face->GetGlyphCount() <= 0) {
    return nullptr;
  }

  auto char_codes_and_indices =
      face->GetCharCodesAndIndices(pdfium::kMaximumSupplementaryCodePoint);
  if (char_codes_and_indices.empty()) {
    return nullptr;
  }

  const ByteString name = BaseFontNameForType(font.get(), font_type);
  RetainPtr<CPDF_Dictionary> font_dict =
      CreateCompositeFontDict(doc, font.get(), font_type, name);

  RetainPtr<CPDF_Dictionary> cid_font_dict =
      CreateCidFontDict(doc, font_type, name);

  RetainPtr<CPDF_Dictionary> font_descriptor_dict =
      LoadFontDesc(doc, name, font.get(), font_data, font_type);
  cid_font_dict->SetNewFor<CPDF_Reference>("FontDescriptor", doc,
                                           font_descriptor_dict->GetObjNum());

  std::multimap<uint32_t, uint32_t> to_unicode;
  std::map<uint32_t, uint32_t> widths;
  for (const auto& item : char_codes_and_indices) {
    if (!pdfium::Contains(widths, item.glyph_index)) {
      widths[item.glyph_index] = font->GetGlyphWidth(item.glyph_index);
    }
    to_unicode.emplace(item.glyph_index, item.char_code);
  }
  RetainPtr<CPDF_Array> widths_array = CreateWidthsArray(doc, widths);
  cid_font_dict->SetNewFor<CPDF_Reference>("W", doc, widths_array->GetObjNum());

  // TODO(npm): Support vertical writing

  CreateDescendantFontsArray(doc, font_dict.Get(), cid_font_dict->GetObjNum());

  RetainPtr<CPDF_Stream> to_unicode_stream = LoadUnicode(doc, to_unicode);
  font_dict->SetNewFor<CPDF_Reference>("ToUnicode", doc,
                                       to_unicode_stream->GetObjNum());
  return CPDF_DocPageData::FromDocument(doc)->GetFont(font_dict);
}

RetainPtr<CPDF_Font> LoadCustomCompositeFont(
    CPDF_Document* doc,
    std::unique_ptr<CFX_Font> font,
    pdfium::span<const uint8_t> font_span,
    const char* to_unicode_cmap,
    pdfium::span<const uint8_t> cid_to_gid_map_span) {
  // If it doesn't have a single char, just fail.
  RetainPtr<CFX_Face> face = font->GetFace();
  if (face->GetGlyphCount() <= 0) {
    return nullptr;
  }

  auto char_codes_and_indices =
      face->GetCharCodesAndIndices(pdfium::kMaximumSupplementaryCodePoint);
  if (char_codes_and_indices.empty()) {
    return nullptr;
  }

  const ByteString name = BaseFontNameForType(font.get(), FPDF_FONT_TRUETYPE);
  RetainPtr<CPDF_Dictionary> font_dict =
      CreateCompositeFontDict(doc, font.get(), FPDF_FONT_TRUETYPE, name);

  RetainPtr<CPDF_Dictionary> cid_font_dict =
      CreateCidFontDict(doc, FPDF_FONT_TRUETYPE, name);

  RetainPtr<CPDF_Dictionary> font_descriptor =
      LoadFontDesc(doc, name, font.get(), font_span, FPDF_FONT_TRUETYPE);
  cid_font_dict->SetNewFor<CPDF_Reference>("FontDescriptor", doc,
                                           font_descriptor->GetObjNum());

  std::map<uint32_t, uint32_t> widths;
  for (const auto& item : char_codes_and_indices) {
    if (!pdfium::Contains(widths, item.glyph_index)) {
      widths[item.glyph_index] = font->GetGlyphWidth(item.glyph_index);
    }
  }
  RetainPtr<CPDF_Array> widths_array = CreateWidthsArray(doc, widths);
  cid_font_dict->SetNewFor<CPDF_Reference>("W", doc, widths_array->GetObjNum());

  auto cid_to_gid_map = doc->NewIndirect<CPDF_Stream>(cid_to_gid_map_span);
  cid_font_dict->SetNewFor<CPDF_Reference>("CIDToGIDMap", doc,
                                           cid_to_gid_map->GetObjNum());

  CreateDescendantFontsArray(doc, font_dict, cid_font_dict->GetObjNum());

  auto to_unicode_stream = doc->NewIndirect<CPDF_Stream>(
      ByteStringView(to_unicode_cmap).unsigned_span());
  font_dict->SetNewFor<CPDF_Reference>("ToUnicode", doc,
                                       to_unicode_stream->GetObjNum());
  return CPDF_DocPageData::FromDocument(doc)->GetFont(font_dict);
}

CPDF_TextObject* CPDFTextObjectFromFPDFPageObject(FPDF_PAGEOBJECT page_object) {
  auto* obj = CPDFPageObjectFromFPDFPageObject(page_object);
  return obj ? obj->AsText() : nullptr;
}

FPDF_GLYPHPATH FPDFGlyphPathFromCFXPath(const CFX_Path* path) {
  return reinterpret_cast<FPDF_GLYPHPATH>(path);
}
const CFX_Path* CFXPathFromFPDFGlyphPath(FPDF_GLYPHPATH path) {
  return reinterpret_cast<const CFX_Path*>(path);
}

}  // namespace

FPDF_EXPORT FPDF_PAGEOBJECT FPDF_CALLCONV
FPDFPageObj_NewTextObj(FPDF_DOCUMENT document,
                       FPDF_BYTESTRING font,
                       float font_size) {
  CPDF_Document* pDoc = CPDFDocumentFromFPDFDocument(document);
  if (!pDoc)
    return nullptr;

  RetainPtr<CPDF_Font> pFont =
      CPDF_Font::GetStockFont(pDoc, ByteStringView(font));
  if (!pFont)
    return nullptr;

  auto pTextObj = std::make_unique<CPDF_TextObject>();
  pTextObj->mutable_text_state().SetFont(std::move(pFont));
  pTextObj->mutable_text_state().SetFontSize(font_size);
  pTextObj->SetDefaultStates();

  // Caller takes ownership.
  return FPDFPageObjectFromCPDFPageObject(pTextObj.release());
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFText_SetText(FPDF_PAGEOBJECT text_object, FPDF_WIDESTRING text) {
  CPDF_TextObject* pTextObj = CPDFTextObjectFromFPDFPageObject(text_object);
  if (!pTextObj) {
    return false;
  }
  // SAFETY: required from caller.
  WideString encodedText = UNSAFE_BUFFERS(WideStringFromFPDFWideString(text));
  ByteString byteText;
  for (wchar_t wc : encodedText) {
    pTextObj->GetFont()->AppendChar(
        &byteText, pTextObj->GetFont()->CharCodeFromUnicode(wc));
  }
  pTextObj->SetText(byteText);
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFText_SetCharcodes(FPDF_PAGEOBJECT text_object,
                      const uint32_t* charcodes,
                      size_t count) {
  CPDF_TextObject* pTextObj = CPDFTextObjectFromFPDFPageObject(text_object);
  if (!pTextObj)
    return false;

  if (!charcodes && count)
    return false;

  ByteString byte_text;
  if (charcodes) {
    for (size_t i = 0; i < count; ++i) {
      pTextObj->GetFont()->AppendChar(&byte_text, UNSAFE_TODO(charcodes[i]));
    }
  }
  pTextObj->SetText(byte_text);
  return true;
}

FPDF_EXPORT FPDF_FONT FPDF_CALLCONV FPDFText_LoadFont(FPDF_DOCUMENT document,
                                                      const uint8_t* data,
                                                      uint32_t size,
                                                      int font_type,
                                                      FPDF_BOOL cid) {
  CPDF_Document* pDoc = CPDFDocumentFromFPDFDocument(document);
  if (!pDoc || !data || size == 0 ||
      (font_type != FPDF_FONT_TYPE1 && font_type != FPDF_FONT_TRUETYPE)) {
    return nullptr;
  }
  // SAFETY: required from caller.
  auto span = UNSAFE_BUFFERS(pdfium::make_span(data, size));
  auto pFont = std::make_unique<CFX_Font>();

  // TODO(npm): Maybe use FT_Get_X11_Font_Format to check format? Otherwise, we
  // are allowing giving any font that can be loaded on freetype and setting it
  // as any font type.
  if (!pFont->LoadEmbedded(span, /*force_vertical=*/false, /*object_tag=*/0))
    return nullptr;

  // Caller takes ownership.
  return FPDFFontFromCPDFFont(
      cid ? LoadCompositeFont(pDoc, std::move(pFont), span, font_type).Leak()
          : LoadSimpleFont(pDoc, std::move(pFont), span, font_type).Leak());
}

FPDF_EXPORT FPDF_FONT FPDF_CALLCONV
FPDFText_LoadStandardFont(FPDF_DOCUMENT document, FPDF_BYTESTRING font) {
  CPDF_Document* pDoc = CPDFDocumentFromFPDFDocument(document);
  if (!pDoc)
    return nullptr;

  // Caller takes ownership.
  return FPDFFontFromCPDFFont(
      CPDF_Font::GetStockFont(pDoc, ByteStringView(font)).Leak());
}

FPDF_EXPORT FPDF_FONT FPDF_CALLCONV
FPDFText_LoadCidType2Font(FPDF_DOCUMENT document,
                          const uint8_t* font_data,
                          uint32_t font_data_size,
                          FPDF_BYTESTRING to_unicode_cmap,
                          const uint8_t* cid_to_gid_map_data,
                          uint32_t cid_to_gid_map_data_size) {
  CPDF_Document* doc = CPDFDocumentFromFPDFDocument(document);
  if (!doc || !font_data || font_data_size == 0 || !to_unicode_cmap ||
      strlen(to_unicode_cmap) == 0 || !cid_to_gid_map_data ||
      cid_to_gid_map_data_size == 0) {
    return nullptr;
  }
  // SAFETY: required from caller.
  auto font_span = UNSAFE_BUFFERS(pdfium::make_span(font_data, font_data_size));
  auto font = std::make_unique<CFX_Font>();

  // TODO(thestig): Consider checking the font format. See similar comment in
  // FPDFText_LoadFont() above.
  if (!font->LoadEmbedded(font_span, /*force_vertical=*/false,
                          /*object_tag=*/0)) {
    return nullptr;
  }

  // Caller takes ownership of result.
  // SAFETY: caller ensures `cid_to_gid_map_data` points to at least
  // `cid_to_gid_map_data_size` entries.
  return FPDFFontFromCPDFFont(
      LoadCustomCompositeFont(
          doc, std::move(font), font_span, to_unicode_cmap,
          UNSAFE_BUFFERS(
              pdfium::make_span(cid_to_gid_map_data, cid_to_gid_map_data_size)))
          .Leak());
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFTextObj_GetFontSize(FPDF_PAGEOBJECT text, float* size) {
  if (!size)
    return false;

  CPDF_TextObject* pTextObj = CPDFTextObjectFromFPDFPageObject(text);
  if (!pTextObj)
    return false;

  *size = pTextObj->GetFontSize();
  return true;
}

FPDF_EXPORT unsigned long FPDF_CALLCONV
FPDFTextObj_GetText(FPDF_PAGEOBJECT text_object,
                    FPDF_TEXTPAGE text_page,
                    FPDF_WCHAR* buffer,
                    unsigned long length) {
  CPDF_TextObject* pTextObj = CPDFTextObjectFromFPDFPageObject(text_object);
  if (!pTextObj)
    return 0;

  CPDF_TextPage* pTextPage = CPDFTextPageFromFPDFTextPage(text_page);
  if (!pTextPage)
    return 0;

  // SAFETY: required from caller.
  return Utf16EncodeMaybeCopyAndReturnLength(
      pTextPage->GetTextByObject(pTextObj),
      UNSAFE_BUFFERS(SpanFromFPDFApiArgs(buffer, length)));
}

FPDF_EXPORT FPDF_BITMAP FPDF_CALLCONV
FPDFTextObj_GetRenderedBitmap(FPDF_DOCUMENT document,
                              FPDF_PAGE page,
                              FPDF_PAGEOBJECT text_object,
                              float scale) {
  CPDF_Document* doc = CPDFDocumentFromFPDFDocument(document);
  if (!doc)
    return nullptr;

  CPDF_Page* optional_page = CPDFPageFromFPDFPage(page);
  if (optional_page && optional_page->GetDocument() != doc)
    return nullptr;

  CPDF_TextObject* text = CPDFTextObjectFromFPDFPageObject(text_object);
  if (!text)
    return nullptr;

  if (scale <= 0)
    return nullptr;

  const CFX_Matrix scale_matrix(scale, 0, 0, scale, 0, 0);
  const CFX_FloatRect& text_rect = text->GetRect();
  const CFX_FloatRect scaled_text_rect = scale_matrix.TransformRect(text_rect);

  // `rect` has to use integer values. Round up as needed.
  const FX_RECT rect = scaled_text_rect.GetOuterRect();
  if (rect.IsEmpty())
    return nullptr;

  // TODO(crbug.com/42271020): Consider adding support for
  // `FXDIB_Format::kBgraPremul`
  auto result_bitmap = pdfium::MakeRetain<CFX_DIBitmap>();
  if (!result_bitmap->Create(rect.Width(), rect.Height(),
                             FXDIB_Format::kBgra)) {
    return nullptr;
  }

  auto render_context = std::make_unique<CPDF_PageRenderContext>();
  CPDF_PageRenderContext* render_context_ptr = render_context.get();
  CPDF_Page::RenderContextClearer clearer(optional_page);
  if (optional_page)
    optional_page->SetRenderContext(std::move(render_context));

  RetainPtr<CPDF_Dictionary> page_resources =
      optional_page ? optional_page->GetMutablePageResources() : nullptr;

  auto device = std::make_unique<CFX_DefaultRenderDevice>();
  CFX_DefaultRenderDevice* device_ptr = device.get();
  render_context_ptr->m_pDevice = std::move(device);
  render_context_ptr->m_pContext = std::make_unique<CPDF_RenderContext>(
      doc, std::move(page_resources), /*pPageCache=*/nullptr);

  device_ptr->Attach(result_bitmap);

  CFX_Matrix device_matrix(rect.Width(), 0, 0, rect.Height(), 0, 0);
  CPDF_RenderStatus status(render_context_ptr->m_pContext.get(), device_ptr);
  status.SetDeviceMatrix(device_matrix);
  status.Initialize(nullptr, nullptr);

  // Need to flip the rendering and also move it to fit within `result_bitmap`.
  CFX_Matrix render_matrix(1, 0, 0, -1, -text_rect.left, text_rect.top);
  render_matrix *= scale_matrix;
  status.RenderSingleObject(text, render_matrix);

  CHECK(!result_bitmap->IsPremultiplied());

  // Caller takes ownership.
  return FPDFBitmapFromCFXDIBitmap(result_bitmap.Leak());
}

FPDF_EXPORT void FPDF_CALLCONV FPDFFont_Close(FPDF_FONT font) {
  // Take back ownership from caller and release.
  RetainPtr<CPDF_Font>().Unleak(CPDFFontFromFPDFFont(font));
}

FPDF_EXPORT FPDF_PAGEOBJECT FPDF_CALLCONV
FPDFPageObj_CreateTextObj(FPDF_DOCUMENT document,
                          FPDF_FONT font,
                          float font_size) {
  CPDF_Document* pDoc = CPDFDocumentFromFPDFDocument(document);
  CPDF_Font* pFont = CPDFFontFromFPDFFont(font);
  if (!pDoc || !pFont)
    return nullptr;

  auto pTextObj = std::make_unique<CPDF_TextObject>();
  pTextObj->mutable_text_state().SetFont(
      CPDF_DocPageData::FromDocument(pDoc)->GetFont(
          pFont->GetMutableFontDict()));
  pTextObj->mutable_text_state().SetFontSize(font_size);
  pTextObj->SetDefaultStates();
  return FPDFPageObjectFromCPDFPageObject(pTextObj.release());
}

FPDF_EXPORT FPDF_TEXT_RENDERMODE FPDF_CALLCONV
FPDFTextObj_GetTextRenderMode(FPDF_PAGEOBJECT text) {
  CPDF_TextObject* pTextObj = CPDFTextObjectFromFPDFPageObject(text);
  if (!pTextObj)
    return FPDF_TEXTRENDERMODE_UNKNOWN;
  return static_cast<FPDF_TEXT_RENDERMODE>(pTextObj->GetTextRenderMode());
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFTextObj_SetTextRenderMode(FPDF_PAGEOBJECT text,
                              FPDF_TEXT_RENDERMODE render_mode) {
  if (render_mode <= FPDF_TEXTRENDERMODE_UNKNOWN ||
      render_mode > FPDF_TEXTRENDERMODE_LAST) {
    return false;
  }

  CPDF_TextObject* pTextObj = CPDFTextObjectFromFPDFPageObject(text);
  if (!pTextObj)
    return false;

  pTextObj->SetTextRenderMode(static_cast<TextRenderingMode>(render_mode));
  return true;
}

FPDF_EXPORT FPDF_FONT FPDF_CALLCONV FPDFTextObj_GetFont(FPDF_PAGEOBJECT text) {
  CPDF_TextObject* pTextObj = CPDFTextObjectFromFPDFPageObject(text);
  if (!pTextObj)
    return nullptr;

  // Unretained reference in public API. NOLINTNEXTLINE
  return FPDFFontFromCPDFFont(pTextObj->GetFont());
}

FPDF_EXPORT size_t FPDF_CALLCONV FPDFFont_GetBaseFontName(FPDF_FONT font,
                                                          char* buffer,
                                                          size_t length) {
  auto* cfont = CPDFFontFromFPDFFont(font);
  if (!cfont) {
    return 0;
  }

  // SAFETY: required from caller.
  auto result_span = UNSAFE_BUFFERS(SpanFromFPDFApiArgs(buffer, length));
  ByteString name = cfont->GetBaseFontName();
  pdfium::span<const char> name_span = name.span_with_terminator();
  fxcrt::try_spancpy(result_span, name_span);
  return name_span.size();
}

FPDF_EXPORT size_t FPDF_CALLCONV FPDFFont_GetFamilyName(FPDF_FONT font,
                                                        char* buffer,
                                                        size_t length) {
  auto* cfont = CPDFFontFromFPDFFont(font);
  if (!cfont) {
    return 0;
  }

  // SAFETY: required from caller.
  auto result_span = UNSAFE_BUFFERS(SpanFromFPDFApiArgs(buffer, length));
  ByteString name = cfont->GetFont()->GetFamilyName();
  pdfium::span<const char> name_span = name.span_with_terminator();
  fxcrt::try_spancpy(result_span, name_span);
  return name_span.size();
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV FPDFFont_GetFontData(FPDF_FONT font,
                                                         uint8_t* buffer,
                                                         size_t buflen,
                                                         size_t* out_buflen) {
  auto* cfont = CPDFFontFromFPDFFont(font);
  if (!cfont || !out_buflen)
    return false;

  // SAFETY: required from caller.
  auto result_span = UNSAFE_BUFFERS(SpanFromFPDFApiArgs(buffer, buflen));
  pdfium::span<const uint8_t> data = cfont->GetFont()->GetFontSpan();
  fxcrt::try_spancpy(result_span, data);
  *out_buflen = data.size();
  return true;
}

FPDF_EXPORT int FPDF_CALLCONV FPDFFont_GetIsEmbedded(FPDF_FONT font) {
  auto* cfont = CPDFFontFromFPDFFont(font);
  if (!cfont)
    return -1;
  return cfont->IsEmbedded() ? 1 : 0;
}

FPDF_EXPORT int FPDF_CALLCONV FPDFFont_GetFlags(FPDF_FONT font) {
  auto* pFont = CPDFFontFromFPDFFont(font);
  if (!pFont)
    return -1;

  // Return only flags from ISO 32000-1:2008, table 123.
  return pFont->GetFontFlags() & 0x7ffff;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV FPDFFont_GetWeight(FPDF_FONT font) {
  auto* pFont = CPDFFontFromFPDFFont(font);
  return pFont ? pFont->GetFontWeight() : -1;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV FPDFFont_GetItalicAngle(FPDF_FONT font,
                                                            int* angle) {
  auto* pFont = CPDFFontFromFPDFFont(font);
  if (!pFont || !angle)
    return false;

  *angle = pFont->GetItalicAngle();
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV FPDFFont_GetAscent(FPDF_FONT font,
                                                       float font_size,
                                                       float* ascent) {
  auto* pFont = CPDFFontFromFPDFFont(font);
  if (!pFont || !ascent)
    return false;

  *ascent = pFont->GetTypeAscent() * font_size / 1000.f;
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV FPDFFont_GetDescent(FPDF_FONT font,
                                                        float font_size,
                                                        float* descent) {
  auto* pFont = CPDFFontFromFPDFFont(font);
  if (!pFont || !descent)
    return false;

  *descent = pFont->GetTypeDescent() * font_size / 1000.f;
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV FPDFFont_GetGlyphWidth(FPDF_FONT font,
                                                           uint32_t glyph,
                                                           float font_size,
                                                           float* width) {
  auto* pFont = CPDFFontFromFPDFFont(font);
  if (!pFont || !width)
    return false;

  uint32_t charcode = pFont->CharCodeFromUnicode(static_cast<wchar_t>(glyph));

  CPDF_CIDFont* pCIDFont = pFont->AsCIDFont();
  if (pCIDFont && pCIDFont->IsVertWriting()) {
    uint16_t cid = pCIDFont->CIDFromCharCode(charcode);
    *width = pCIDFont->GetVertWidth(cid) * font_size / 1000.f;
  } else {
    *width = pFont->GetCharWidthF(charcode) * font_size / 1000.f;
  }

  return true;
}

FPDF_EXPORT FPDF_GLYPHPATH FPDF_CALLCONV
FPDFFont_GetGlyphPath(FPDF_FONT font, uint32_t glyph, float font_size) {
  auto* pFont = CPDFFontFromFPDFFont(font);
  if (!pFont)
    return nullptr;

  if (!pdfium::IsValueInRangeForNumericType<wchar_t>(glyph)) {
    return nullptr;
  }

  uint32_t charcode = pFont->CharCodeFromUnicode(static_cast<wchar_t>(glyph));
  std::vector<TextCharPos> pos =
      GetCharPosList(pdfium::span_from_ref(charcode),
                     pdfium::span<const float>(), pFont, font_size);
  if (pos.empty())
    return nullptr;

  CFX_Font* pCfxFont;
  if (pos[0].m_FallbackFontPosition == -1) {
    pCfxFont = pFont->GetFont();
    DCHECK(pCfxFont);  // Never null.
  } else {
    pCfxFont = pFont->GetFontFallback(pos[0].m_FallbackFontPosition);
    if (!pCfxFont)
      return nullptr;
  }

  const CFX_Path* pPath =
      pCfxFont->LoadGlyphPath(pos[0].m_GlyphIndex, pos[0].m_FontCharWidth);

  return FPDFGlyphPathFromCFXPath(pPath);
}

FPDF_EXPORT int FPDF_CALLCONV
FPDFGlyphPath_CountGlyphSegments(FPDF_GLYPHPATH glyphpath) {
  auto* pPath = CFXPathFromFPDFGlyphPath(glyphpath);
  if (!pPath)
    return -1;

  return fxcrt::CollectionSize<int>(pPath->GetPoints());
}

FPDF_EXPORT FPDF_PATHSEGMENT FPDF_CALLCONV
FPDFGlyphPath_GetGlyphPathSegment(FPDF_GLYPHPATH glyphpath, int index) {
  auto* pPath = CFXPathFromFPDFGlyphPath(glyphpath);
  if (!pPath)
    return nullptr;

  pdfium::span<const CFX_Path::Point> points = pPath->GetPoints();
  if (!fxcrt::IndexInBounds(points, index))
    return nullptr;

  return FPDFPathSegmentFromFXPathPoint(&points[index]);
}
