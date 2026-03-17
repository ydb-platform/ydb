// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/page/cpdf_streamcontentparser.h"

#include <algorithm>
#include <array>
#include <map>
#include <memory>
#include <utility>
#include <vector>

#include "core/fpdfapi/font/cpdf_font.h"
#include "core/fpdfapi/font/cpdf_type3font.h"
#include "core/fpdfapi/page/cpdf_allstates.h"
#include "core/fpdfapi/page/cpdf_docpagedata.h"
#include "core/fpdfapi/page/cpdf_form.h"
#include "core/fpdfapi/page/cpdf_formobject.h"
#include "core/fpdfapi/page/cpdf_image.h"
#include "core/fpdfapi/page/cpdf_imageobject.h"
#include "core/fpdfapi/page/cpdf_meshstream.h"
#include "core/fpdfapi/page/cpdf_pageobject.h"
#include "core/fpdfapi/page/cpdf_pathobject.h"
#include "core/fpdfapi/page/cpdf_shadingobject.h"
#include "core/fpdfapi/page/cpdf_shadingpattern.h"
#include "core/fpdfapi/page/cpdf_streamparser.h"
#include "core/fpdfapi/page/cpdf_textobject.h"
#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_document.h"
#include "core/fpdfapi/parser/cpdf_name.h"
#include "core/fpdfapi/parser/cpdf_number.h"
#include "core/fpdfapi/parser/cpdf_reference.h"
#include "core/fpdfapi/parser/cpdf_stream.h"
#include "core/fpdfapi/parser/fpdf_parser_utility.h"
#include "core/fxcrt/autonuller.h"
#include "core/fxcrt/bytestring.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/containers/contains.h"
#include "core/fxcrt/fx_safe_types.h"
#include "core/fxcrt/notreached.h"
#include "core/fxcrt/scoped_set_insertion.h"
#include "core/fxcrt/span.h"
#include "core/fxcrt/stl_util.h"
#include "core/fxge/cfx_graphstate.h"
#include "core/fxge/cfx_graphstatedata.h"

namespace {

constexpr int kMaxFormLevel = 40;

constexpr int kSingleCoordinatePair = 1;
constexpr int kTensorCoordinatePairs = 16;
constexpr int kCoonsCoordinatePairs = 12;
constexpr int kSingleColorPerPatch = 1;
constexpr int kQuadColorsPerPatch = 4;

const char kPathOperatorSubpath = 'm';
const char kPathOperatorLine = 'l';
const char kPathOperatorCubicBezier1 = 'c';
const char kPathOperatorCubicBezier2 = 'v';
const char kPathOperatorCubicBezier3 = 'y';
const char kPathOperatorClosePath = 'h';
const char kPathOperatorRectangle[] = "re";

using OpCodes = std::map<uint32_t, void (CPDF_StreamContentParser::*)()>;
OpCodes* g_opcodes = nullptr;

CFX_FloatRect GetShadingBBox(CPDF_ShadingPattern* pShading,
                             const CFX_Matrix& matrix) {
  ShadingType type = pShading->GetShadingType();
  RetainPtr<const CPDF_Stream> pStream = ToStream(pShading->GetShadingObject());
  RetainPtr<CPDF_ColorSpace> pCS = pShading->GetCS();
  if (!pStream || !pCS)
    return CFX_FloatRect();

  CPDF_MeshStream stream(type, pShading->GetFuncs(), std::move(pStream),
                         std::move(pCS));
  if (!stream.Load())
    return CFX_FloatRect();

  CFX_FloatRect rect;
  bool update_rect = false;
  bool bGouraud = type == kFreeFormGouraudTriangleMeshShading ||
                  type == kLatticeFormGouraudTriangleMeshShading;

  int point_count;
  if (type == kTensorProductPatchMeshShading)
    point_count = kTensorCoordinatePairs;
  else if (type == kCoonsPatchMeshShading)
    point_count = kCoonsCoordinatePairs;
  else
    point_count = kSingleCoordinatePair;

  int color_count;
  if (type == kCoonsPatchMeshShading || type == kTensorProductPatchMeshShading)
    color_count = kQuadColorsPerPatch;
  else
    color_count = kSingleColorPerPatch;

  while (!stream.IsEOF()) {
    uint32_t flag = 0;
    if (type != kLatticeFormGouraudTriangleMeshShading) {
      if (!stream.CanReadFlag())
        break;
      flag = stream.ReadFlag();
    }

    if (!bGouraud && flag) {
      point_count -= 4;
      color_count -= 2;
    }

    for (int i = 0; i < point_count; ++i) {
      if (!stream.CanReadCoords())
        break;

      CFX_PointF origin = stream.ReadCoords();
      if (update_rect) {
        rect.UpdateRect(origin);
      } else {
        rect = CFX_FloatRect(origin);
        update_rect = true;
      }
    }
    FX_SAFE_UINT32 nBits = stream.Components();
    nBits *= stream.ComponentBits();
    nBits *= color_count;
    if (!nBits.IsValid())
      break;

    stream.SkipBits(nBits.ValueOrDie());
    if (bGouraud)
      stream.ByteAlign();
  }
  return matrix.TransformRect(rect);
}

struct AbbrPair {
  const char* abbr;
  const char* full_name;
};

const AbbrPair kInlineKeyAbbr[] = {
    {"BPC", "BitsPerComponent"}, {"CS", "ColorSpace"}, {"D", "Decode"},
    {"DP", "DecodeParms"},       {"F", "Filter"},      {"H", "Height"},
    {"IM", "ImageMask"},         {"I", "Interpolate"}, {"W", "Width"},
};

const AbbrPair kInlineValueAbbr[] = {
    {"G", "DeviceGray"},       {"RGB", "DeviceRGB"},
    {"CMYK", "DeviceCMYK"},    {"I", "Indexed"},
    {"AHx", "ASCIIHexDecode"}, {"A85", "ASCII85Decode"},
    {"LZW", "LZWDecode"},      {"Fl", "FlateDecode"},
    {"RL", "RunLengthDecode"}, {"CCF", "CCITTFaxDecode"},
    {"DCT", "DCTDecode"},
};

struct AbbrReplacementOp {
  bool is_replace_key;
  ByteString key;
  ByteStringView replacement;
};

ByteStringView FindFullName(pdfium::span<const AbbrPair> table,
                            ByteStringView abbr) {
  for (const auto& pair : table) {
    if (pair.abbr == abbr)
      return ByteStringView(pair.full_name);
  }
  return ByteStringView();
}

void ReplaceAbbr(RetainPtr<CPDF_Object> pObj);

void ReplaceAbbrInDictionary(CPDF_Dictionary* pDict) {
  std::vector<AbbrReplacementOp> replacements;
  {
    CPDF_DictionaryLocker locker(pDict);
    for (const auto& it : locker) {
      ByteString key = it.first;
      ByteStringView fullname =
          FindFullName(kInlineKeyAbbr, key.AsStringView());
      if (!fullname.IsEmpty()) {
        AbbrReplacementOp op;
        op.is_replace_key = true;
        op.key = std::move(key);
        op.replacement = fullname;
        replacements.push_back(op);
        key = fullname;
      }
      RetainPtr<CPDF_Object> value = it.second;
      if (value->IsName()) {
        ByteString name = value->GetString();
        fullname = FindFullName(kInlineValueAbbr, name.AsStringView());
        if (!fullname.IsEmpty()) {
          AbbrReplacementOp op;
          op.is_replace_key = false;
          op.key = key;
          op.replacement = fullname;
          replacements.push_back(op);
        }
      } else {
        ReplaceAbbr(std::move(value));
      }
    }
  }
  for (const auto& op : replacements) {
    if (op.is_replace_key)
      pDict->ReplaceKey(op.key, ByteString(op.replacement));
    else
      pDict->SetNewFor<CPDF_Name>(op.key, ByteString(op.replacement));
  }
}

void ReplaceAbbrInArray(CPDF_Array* pArray) {
  for (size_t i = 0; i < pArray->size(); ++i) {
    RetainPtr<CPDF_Object> pElement = pArray->GetMutableObjectAt(i);
    if (pElement->IsName()) {
      ByteString name = pElement->GetString();
      ByteStringView fullname =
          FindFullName(kInlineValueAbbr, name.AsStringView());
      if (!fullname.IsEmpty())
        pArray->SetNewAt<CPDF_Name>(i, ByteString(fullname));
    } else {
      ReplaceAbbr(std::move(pElement));
    }
  }
}

void ReplaceAbbr(RetainPtr<CPDF_Object> pObj) {
  CPDF_Dictionary* pDict = pObj->AsMutableDictionary();
  if (pDict) {
    ReplaceAbbrInDictionary(pDict);
    return;
  }

  CPDF_Array* pArray = pObj->AsMutableArray();
  if (pArray)
    ReplaceAbbrInArray(pArray);
}

}  // namespace

// static
void CPDF_StreamContentParser::InitializeGlobals() {
  CHECK(!g_opcodes);
  g_opcodes = new OpCodes({
      {FXBSTR_ID('"', 0, 0, 0),
       &CPDF_StreamContentParser::Handle_NextLineShowText_Space},
      {FXBSTR_ID('\'', 0, 0, 0),
       &CPDF_StreamContentParser::Handle_NextLineShowText},
      {FXBSTR_ID('B', 0, 0, 0),
       &CPDF_StreamContentParser::Handle_FillStrokePath},
      {FXBSTR_ID('B', '*', 0, 0),
       &CPDF_StreamContentParser::Handle_EOFillStrokePath},
      {FXBSTR_ID('B', 'D', 'C', 0),
       &CPDF_StreamContentParser::Handle_BeginMarkedContent_Dictionary},
      {FXBSTR_ID('B', 'I', 0, 0), &CPDF_StreamContentParser::Handle_BeginImage},
      {FXBSTR_ID('B', 'M', 'C', 0),
       &CPDF_StreamContentParser::Handle_BeginMarkedContent},
      {FXBSTR_ID('B', 'T', 0, 0), &CPDF_StreamContentParser::Handle_BeginText},
      {FXBSTR_ID('C', 'S', 0, 0),
       &CPDF_StreamContentParser::Handle_SetColorSpace_Stroke},
      {FXBSTR_ID('D', 'P', 0, 0),
       &CPDF_StreamContentParser::Handle_MarkPlace_Dictionary},
      {FXBSTR_ID('D', 'o', 0, 0),
       &CPDF_StreamContentParser::Handle_ExecuteXObject},
      {FXBSTR_ID('E', 'I', 0, 0), &CPDF_StreamContentParser::Handle_EndImage},
      {FXBSTR_ID('E', 'M', 'C', 0),
       &CPDF_StreamContentParser::Handle_EndMarkedContent},
      {FXBSTR_ID('E', 'T', 0, 0), &CPDF_StreamContentParser::Handle_EndText},
      {FXBSTR_ID('F', 0, 0, 0), &CPDF_StreamContentParser::Handle_FillPathOld},
      {FXBSTR_ID('G', 0, 0, 0),
       &CPDF_StreamContentParser::Handle_SetGray_Stroke},
      {FXBSTR_ID('I', 'D', 0, 0),
       &CPDF_StreamContentParser::Handle_BeginImageData},
      {FXBSTR_ID('J', 0, 0, 0), &CPDF_StreamContentParser::Handle_SetLineCap},
      {FXBSTR_ID('K', 0, 0, 0),
       &CPDF_StreamContentParser::Handle_SetCMYKColor_Stroke},
      {FXBSTR_ID('M', 0, 0, 0),
       &CPDF_StreamContentParser::Handle_SetMiterLimit},
      {FXBSTR_ID('M', 'P', 0, 0), &CPDF_StreamContentParser::Handle_MarkPlace},
      {FXBSTR_ID('Q', 0, 0, 0),
       &CPDF_StreamContentParser::Handle_RestoreGraphState},
      {FXBSTR_ID('R', 'G', 0, 0),
       &CPDF_StreamContentParser::Handle_SetRGBColor_Stroke},
      {FXBSTR_ID('S', 0, 0, 0), &CPDF_StreamContentParser::Handle_StrokePath},
      {FXBSTR_ID('S', 'C', 0, 0),
       &CPDF_StreamContentParser::Handle_SetColor_Stroke},
      {FXBSTR_ID('S', 'C', 'N', 0),
       &CPDF_StreamContentParser::Handle_SetColorPS_Stroke},
      {FXBSTR_ID('T', '*', 0, 0),
       &CPDF_StreamContentParser::Handle_MoveToNextLine},
      {FXBSTR_ID('T', 'D', 0, 0),
       &CPDF_StreamContentParser::Handle_MoveTextPoint_SetLeading},
      {FXBSTR_ID('T', 'J', 0, 0),
       &CPDF_StreamContentParser::Handle_ShowText_Positioning},
      {FXBSTR_ID('T', 'L', 0, 0),
       &CPDF_StreamContentParser::Handle_SetTextLeading},
      {FXBSTR_ID('T', 'c', 0, 0),
       &CPDF_StreamContentParser::Handle_SetCharSpace},
      {FXBSTR_ID('T', 'd', 0, 0),
       &CPDF_StreamContentParser::Handle_MoveTextPoint},
      {FXBSTR_ID('T', 'f', 0, 0), &CPDF_StreamContentParser::Handle_SetFont},
      {FXBSTR_ID('T', 'j', 0, 0), &CPDF_StreamContentParser::Handle_ShowText},
      {FXBSTR_ID('T', 'm', 0, 0),
       &CPDF_StreamContentParser::Handle_SetTextMatrix},
      {FXBSTR_ID('T', 'r', 0, 0),
       &CPDF_StreamContentParser::Handle_SetTextRenderMode},
      {FXBSTR_ID('T', 's', 0, 0),
       &CPDF_StreamContentParser::Handle_SetTextRise},
      {FXBSTR_ID('T', 'w', 0, 0),
       &CPDF_StreamContentParser::Handle_SetWordSpace},
      {FXBSTR_ID('T', 'z', 0, 0),
       &CPDF_StreamContentParser::Handle_SetHorzScale},
      {FXBSTR_ID('W', 0, 0, 0), &CPDF_StreamContentParser::Handle_Clip},
      {FXBSTR_ID('W', '*', 0, 0), &CPDF_StreamContentParser::Handle_EOClip},
      {FXBSTR_ID('b', 0, 0, 0),
       &CPDF_StreamContentParser::Handle_CloseFillStrokePath},
      {FXBSTR_ID('b', '*', 0, 0),
       &CPDF_StreamContentParser::Handle_CloseEOFillStrokePath},
      {FXBSTR_ID('c', 0, 0, 0), &CPDF_StreamContentParser::Handle_CurveTo_123},
      {FXBSTR_ID('c', 'm', 0, 0),
       &CPDF_StreamContentParser::Handle_ConcatMatrix},
      {FXBSTR_ID('c', 's', 0, 0),
       &CPDF_StreamContentParser::Handle_SetColorSpace_Fill},
      {FXBSTR_ID('d', 0, 0, 0), &CPDF_StreamContentParser::Handle_SetDash},
      {FXBSTR_ID('d', '0', 0, 0),
       &CPDF_StreamContentParser::Handle_SetCharWidth},
      {FXBSTR_ID('d', '1', 0, 0),
       &CPDF_StreamContentParser::Handle_SetCachedDevice},
      {FXBSTR_ID('f', 0, 0, 0), &CPDF_StreamContentParser::Handle_FillPath},
      {FXBSTR_ID('f', '*', 0, 0), &CPDF_StreamContentParser::Handle_EOFillPath},
      {FXBSTR_ID('g', 0, 0, 0), &CPDF_StreamContentParser::Handle_SetGray_Fill},
      {FXBSTR_ID('g', 's', 0, 0),
       &CPDF_StreamContentParser::Handle_SetExtendGraphState},
      {FXBSTR_ID('h', 0, 0, 0), &CPDF_StreamContentParser::Handle_ClosePath},
      {FXBSTR_ID('i', 0, 0, 0), &CPDF_StreamContentParser::Handle_SetFlat},
      {FXBSTR_ID('j', 0, 0, 0), &CPDF_StreamContentParser::Handle_SetLineJoin},
      {FXBSTR_ID('k', 0, 0, 0),
       &CPDF_StreamContentParser::Handle_SetCMYKColor_Fill},
      {FXBSTR_ID('l', 0, 0, 0), &CPDF_StreamContentParser::Handle_LineTo},
      {FXBSTR_ID('m', 0, 0, 0), &CPDF_StreamContentParser::Handle_MoveTo},
      {FXBSTR_ID('n', 0, 0, 0), &CPDF_StreamContentParser::Handle_EndPath},
      {FXBSTR_ID('q', 0, 0, 0),
       &CPDF_StreamContentParser::Handle_SaveGraphState},
      {FXBSTR_ID('r', 'e', 0, 0), &CPDF_StreamContentParser::Handle_Rectangle},
      {FXBSTR_ID('r', 'g', 0, 0),
       &CPDF_StreamContentParser::Handle_SetRGBColor_Fill},
      {FXBSTR_ID('r', 'i', 0, 0),
       &CPDF_StreamContentParser::Handle_SetRenderIntent},
      {FXBSTR_ID('s', 0, 0, 0),
       &CPDF_StreamContentParser::Handle_CloseStrokePath},
      {FXBSTR_ID('s', 'c', 0, 0),
       &CPDF_StreamContentParser::Handle_SetColor_Fill},
      {FXBSTR_ID('s', 'c', 'n', 0),
       &CPDF_StreamContentParser::Handle_SetColorPS_Fill},
      {FXBSTR_ID('s', 'h', 0, 0), &CPDF_StreamContentParser::Handle_ShadeFill},
      {FXBSTR_ID('v', 0, 0, 0), &CPDF_StreamContentParser::Handle_CurveTo_23},
      {FXBSTR_ID('w', 0, 0, 0), &CPDF_StreamContentParser::Handle_SetLineWidth},
      {FXBSTR_ID('y', 0, 0, 0), &CPDF_StreamContentParser::Handle_CurveTo_13},
  });
}

// static
void CPDF_StreamContentParser::DestroyGlobals() {
  delete g_opcodes;
  g_opcodes = nullptr;
}

CPDF_StreamContentParser::CPDF_StreamContentParser(
    CPDF_Document* pDocument,
    RetainPtr<CPDF_Dictionary> pPageResources,
    RetainPtr<CPDF_Dictionary> pParentResources,
    const CFX_Matrix* pmtContentToUser,
    CPDF_PageObjectHolder* pObjHolder,
    RetainPtr<CPDF_Dictionary> pResources,
    const CFX_FloatRect& rcBBox,
    const CPDF_AllStates* pStates,
    CPDF_Form::RecursionState* recursion_state)
    : m_pDocument(pDocument),
      m_pPageResources(pPageResources),
      m_pParentResources(pParentResources),
      m_pResources(CPDF_Form::ChooseResourcesDict(pResources.Get(),
                                                  pParentResources.Get(),
                                                  pPageResources.Get())),
      m_pObjectHolder(pObjHolder),
      m_RecursionState(recursion_state),
      m_BBox(rcBBox),
      m_pCurStates(std::make_unique<CPDF_AllStates>()) {
  if (pmtContentToUser)
    m_mtContentToUser = *pmtContentToUser;
  if (pStates) {
    *m_pCurStates = *pStates;
  } else {
    m_pCurStates->mutable_general_state().Emplace();
    m_pCurStates->mutable_graph_state().Emplace();
    m_pCurStates->mutable_text_state().Emplace();
    m_pCurStates->mutable_color_state().Emplace();
  }

  // Add the sentinel.
  m_ContentMarksStack.push(std::make_unique<CPDF_ContentMarks>());

  // Initialize `m_AllCTMs`, as there is a CTM, even if the stream contains no
  // cm operators.
  m_AllCTMs[0] = m_pCurStates->current_transformation_matrix();
}

CPDF_StreamContentParser::~CPDF_StreamContentParser() {
  ClearAllParams();
}

int CPDF_StreamContentParser::GetNextParamPos() {
  if (m_ParamCount == kParamBufSize) {
    m_ParamStartPos++;
    if (m_ParamStartPos == kParamBufSize) {
      m_ParamStartPos = 0;
    }
    if (m_ParamBuf[m_ParamStartPos].m_Type == ContentParam::Type::kObject)
      m_ParamBuf[m_ParamStartPos].m_pObject.Reset();

    return m_ParamStartPos;
  }
  int index = m_ParamStartPos + m_ParamCount;
  if (index >= kParamBufSize) {
    index -= kParamBufSize;
  }
  m_ParamCount++;
  return index;
}

void CPDF_StreamContentParser::AddNameParam(ByteStringView bsName) {
  ContentParam& param = m_ParamBuf[GetNextParamPos()];
  param.m_Type = ContentParam::Type::kName;
  param.m_Name = PDF_NameDecode(bsName);
}

void CPDF_StreamContentParser::AddNumberParam(ByteStringView str) {
  ContentParam& param = m_ParamBuf[GetNextParamPos()];
  param.m_Type = ContentParam::Type::kNumber;
  param.m_Number = FX_Number(str);
}

void CPDF_StreamContentParser::AddObjectParam(RetainPtr<CPDF_Object> pObj) {
  ContentParam& param = m_ParamBuf[GetNextParamPos()];
  param.m_Type = ContentParam::Type::kObject;
  param.m_pObject = std::move(pObj);
}

void CPDF_StreamContentParser::ClearAllParams() {
  uint32_t index = m_ParamStartPos;
  for (uint32_t i = 0; i < m_ParamCount; i++) {
    if (m_ParamBuf[index].m_Type == ContentParam::Type::kObject)
      m_ParamBuf[index].m_pObject.Reset();
    index++;
    if (index == kParamBufSize)
      index = 0;
  }
  m_ParamStartPos = 0;
  m_ParamCount = 0;
}

RetainPtr<CPDF_Object> CPDF_StreamContentParser::GetObject(uint32_t index) {
  if (index >= m_ParamCount) {
    return nullptr;
  }
  int real_index = m_ParamStartPos + m_ParamCount - index - 1;
  if (real_index >= kParamBufSize) {
    real_index -= kParamBufSize;
  }
  ContentParam& param = m_ParamBuf[real_index];
  if (param.m_Type == ContentParam::Type::kNumber) {
    param.m_Type = ContentParam::Type::kObject;
    param.m_pObject =
        param.m_Number.IsInteger()
            ? pdfium::MakeRetain<CPDF_Number>(param.m_Number.GetSigned())
            : pdfium::MakeRetain<CPDF_Number>(param.m_Number.GetFloat());
    return param.m_pObject;
  }
  if (param.m_Type == ContentParam::Type::kName) {
    param.m_Type = ContentParam::Type::kObject;
    param.m_pObject = m_pDocument->New<CPDF_Name>(param.m_Name);
    return param.m_pObject;
  }
  if (param.m_Type == ContentParam::Type::kObject)
    return param.m_pObject;

  NOTREACHED_NORETURN();
}

ByteString CPDF_StreamContentParser::GetString(uint32_t index) const {
  if (index >= m_ParamCount)
    return ByteString();

  int real_index = m_ParamStartPos + m_ParamCount - index - 1;
  if (real_index >= kParamBufSize)
    real_index -= kParamBufSize;

  const ContentParam& param = m_ParamBuf[real_index];
  if (param.m_Type == ContentParam::Type::kName)
    return param.m_Name;

  if (param.m_Type == ContentParam::Type::kObject && param.m_pObject)
    return param.m_pObject->GetString();

  return ByteString();
}

float CPDF_StreamContentParser::GetNumber(uint32_t index) const {
  if (index >= m_ParamCount)
    return 0;

  int real_index = m_ParamStartPos + m_ParamCount - index - 1;
  if (real_index >= kParamBufSize)
    real_index -= kParamBufSize;

  const ContentParam& param = m_ParamBuf[real_index];
  if (param.m_Type == ContentParam::Type::kNumber)
    return param.m_Number.GetFloat();

  if (param.m_Type == ContentParam::Type::kObject && param.m_pObject)
    return param.m_pObject->GetNumber();

  return 0;
}

std::vector<float> CPDF_StreamContentParser::GetNumbers(size_t count) const {
  std::vector<float> values(count);
  for (size_t i = 0; i < count; ++i)
    values[i] = GetNumber(count - i - 1);
  return values;
}

CFX_PointF CPDF_StreamContentParser::GetPoint(uint32_t index) const {
  return CFX_PointF(GetNumber(index + 1), GetNumber(index));
}

CFX_Matrix CPDF_StreamContentParser::GetMatrix() const {
  return CFX_Matrix(GetNumber(5), GetNumber(4), GetNumber(3), GetNumber(2),
                    GetNumber(1), GetNumber(0));
}

void CPDF_StreamContentParser::SetGraphicStates(CPDF_PageObject* pObj,
                                                bool bColor,
                                                bool bText,
                                                bool bGraph) {
  pObj->mutable_general_state() = m_pCurStates->general_state();
  pObj->mutable_clip_path() = m_pCurStates->clip_path();
  pObj->SetContentMarks(*m_ContentMarksStack.top());
  if (bColor) {
    pObj->mutable_color_state() = m_pCurStates->color_state();
  }
  if (bGraph) {
    pObj->mutable_graph_state() = m_pCurStates->graph_state();
  }
  if (bText) {
    pObj->mutable_text_state() = m_pCurStates->text_state();
  }
}

void CPDF_StreamContentParser::OnOperator(ByteStringView op) {
  auto it = g_opcodes->find(op.GetID());
  if (it != g_opcodes->end()) {
    (this->*it->second)();
  }
}

void CPDF_StreamContentParser::Handle_CloseFillStrokePath() {
  Handle_ClosePath();
  AddPathObject(CFX_FillRenderOptions::FillType::kWinding, RenderType::kStroke);
}

void CPDF_StreamContentParser::Handle_FillStrokePath() {
  AddPathObject(CFX_FillRenderOptions::FillType::kWinding, RenderType::kStroke);
}

void CPDF_StreamContentParser::Handle_CloseEOFillStrokePath() {
  AddPathPointAndClose(m_PathStart, CFX_Path::Point::Type::kLine);
  AddPathObject(CFX_FillRenderOptions::FillType::kEvenOdd, RenderType::kStroke);
}

void CPDF_StreamContentParser::Handle_EOFillStrokePath() {
  AddPathObject(CFX_FillRenderOptions::FillType::kEvenOdd, RenderType::kStroke);
}

void CPDF_StreamContentParser::Handle_BeginMarkedContent_Dictionary() {
  RetainPtr<CPDF_Object> pProperty = GetObject(0);
  if (!pProperty)
    return;

  ByteString tag = GetString(1);
  std::unique_ptr<CPDF_ContentMarks> new_marks =
      m_ContentMarksStack.top()->Clone();

  if (pProperty->IsName()) {
    ByteString property_name = pProperty->GetString();
    RetainPtr<CPDF_Dictionary> pHolder = FindResourceHolder("Properties");
    if (!pHolder || !pHolder->GetDictFor(property_name))
      return;
    new_marks->AddMarkWithPropertiesHolder(tag, std::move(pHolder),
                                           property_name);
  } else if (pProperty->IsDictionary()) {
    new_marks->AddMarkWithDirectDict(tag, ToDictionary(pProperty));
  } else {
    return;
  }
  m_ContentMarksStack.push(std::move(new_marks));
}

void CPDF_StreamContentParser::Handle_BeginImage() {
  FX_FILESIZE savePos = m_pSyntax->GetPos();
  auto pDict = m_pDocument->New<CPDF_Dictionary>();
  while (true) {
    CPDF_StreamParser::ElementType type = m_pSyntax->ParseNextElement();
    if (type == CPDF_StreamParser::ElementType::kKeyword) {
      if (m_pSyntax->GetWord() != "ID") {
        m_pSyntax->SetPos(savePos);
        return;
      }
    }
    if (type != CPDF_StreamParser::ElementType::kName) {
      break;
    }
    auto word = m_pSyntax->GetWord();
    ByteString key(word.Last(word.GetLength() - 1));
    auto pObj = m_pSyntax->ReadNextObject(false, false, 0);
    if (pObj && !pObj->IsInline()) {
      pDict->SetNewFor<CPDF_Reference>(key, m_pDocument, pObj->GetObjNum());
    } else {
      pDict->SetFor(key, std::move(pObj));
    }
  }
  ReplaceAbbr(pDict);
  RetainPtr<const CPDF_Object> pCSObj;
  if (pDict->KeyExist("ColorSpace")) {
    pCSObj = pDict->GetDirectObjectFor("ColorSpace");
    if (pCSObj->IsName()) {
      ByteString name = pCSObj->GetString();
      if (name != "DeviceRGB" && name != "DeviceGray" && name != "DeviceCMYK") {
        pCSObj = FindResourceObj("ColorSpace", name);
        if (pCSObj && pCSObj->IsInline())
          pDict->SetFor("ColorSpace", pCSObj->Clone());
      }
    }
  }
  pDict->SetNewFor<CPDF_Name>("Subtype", "Image");
  RetainPtr<CPDF_Stream> pStream =
      m_pSyntax->ReadInlineStream(m_pDocument, std::move(pDict), pCSObj.Get());
  while (true) {
    CPDF_StreamParser::ElementType type = m_pSyntax->ParseNextElement();
    if (type == CPDF_StreamParser::ElementType::kEndOfData)
      break;

    if (type != CPDF_StreamParser::ElementType::kKeyword)
      continue;

    if (m_pSyntax->GetWord() == "EI")
      break;
  }
  CPDF_ImageObject* pObj = AddImageFromStream(std::move(pStream), /*name=*/"");
  // Record the bounding box of this image, so rendering code can draw it
  // properly.
  if (pObj && pObj->GetImage()->IsMask())
    m_pObjectHolder->AddImageMaskBoundingBox(pObj->GetRect());
}

void CPDF_StreamContentParser::Handle_BeginMarkedContent() {
  std::unique_ptr<CPDF_ContentMarks> new_marks =
      m_ContentMarksStack.top()->Clone();
  new_marks->AddMark(GetString(0));
  m_ContentMarksStack.push(std::move(new_marks));
}

void CPDF_StreamContentParser::Handle_BeginText() {
  m_pCurStates->set_text_matrix(CFX_Matrix());
  OnChangeTextMatrix();
  m_pCurStates->ResetTextPosition();
}

void CPDF_StreamContentParser::Handle_CurveTo_123() {
  AddPathPoint(GetPoint(4), CFX_Path::Point::Type::kBezier);
  AddPathPoint(GetPoint(2), CFX_Path::Point::Type::kBezier);
  AddPathPoint(GetPoint(0), CFX_Path::Point::Type::kBezier);
}

void CPDF_StreamContentParser::Handle_ConcatMatrix() {
  m_pCurStates->prepend_to_current_transformation_matrix(GetMatrix());
  m_AllCTMs[GetCurrentStreamIndex()] =
      m_pCurStates->current_transformation_matrix();
  OnChangeTextMatrix();
}

void CPDF_StreamContentParser::Handle_SetColorSpace_Fill() {
  RetainPtr<CPDF_ColorSpace> pCS = FindColorSpace(GetString(0));
  if (!pCS)
    return;

  m_pCurStates->mutable_color_state().GetMutableFillColor()->SetColorSpace(
      std::move(pCS));
}

void CPDF_StreamContentParser::Handle_SetColorSpace_Stroke() {
  RetainPtr<CPDF_ColorSpace> pCS = FindColorSpace(GetString(0));
  if (!pCS)
    return;

  m_pCurStates->mutable_color_state().GetMutableStrokeColor()->SetColorSpace(
      std::move(pCS));
}

void CPDF_StreamContentParser::Handle_SetDash() {
  RetainPtr<CPDF_Array> pArray = ToArray(GetObject(1));
  if (!pArray)
    return;

  m_pCurStates->SetLineDash(pArray.Get(), GetNumber(0), 1.0f);
}

void CPDF_StreamContentParser::Handle_SetCharWidth() {
  m_Type3Data[0] = GetNumber(1);
  m_Type3Data[1] = GetNumber(0);
  m_bColored = true;
}

void CPDF_StreamContentParser::Handle_SetCachedDevice() {
  for (int i = 0; i < 6; i++) {
    m_Type3Data[i] = GetNumber(5 - i);
  }
  m_bColored = false;
}

void CPDF_StreamContentParser::Handle_ExecuteXObject() {
  ByteString name = GetString(0);
  if (name == m_LastImageName && m_pLastImage && m_pLastImage->GetStream() &&
      m_pLastImage->GetStream()->GetObjNum()) {
    CPDF_ImageObject* pObj = AddLastImage();
    // Record the bounding box of this image, so rendering code can draw it
    // properly.
    if (pObj && pObj->GetImage()->IsMask())
      m_pObjectHolder->AddImageMaskBoundingBox(pObj->GetRect());
    return;
  }

  RetainPtr<CPDF_Stream> pXObject(ToStream(FindResourceObj("XObject", name)));
  if (!pXObject)
    return;

  const ByteString type = pXObject->GetDict()->GetByteStringFor("Subtype");
  if (type == "Form") {
    AddForm(std::move(pXObject), name);
    return;
  }

  if (type == "Image") {
    CPDF_ImageObject* pObj =
        pXObject->IsInline()
            ? AddImageFromStream(ToStream(pXObject->Clone()), name)
            : AddImageFromStreamObjNum(pXObject->GetObjNum(), name);

    m_LastImageName = std::move(name);
    if (pObj) {
      m_pLastImage = pObj->GetImage();
      if (m_pLastImage->IsMask())
        m_pObjectHolder->AddImageMaskBoundingBox(pObj->GetRect());
    }
  }
}

void CPDF_StreamContentParser::AddForm(RetainPtr<CPDF_Stream> pStream,
                                       const ByteString& name) {
  CPDF_AllStates status;
  status.mutable_general_state() = m_pCurStates->general_state();
  status.mutable_graph_state() = m_pCurStates->graph_state();
  status.mutable_color_state() = m_pCurStates->color_state();
  status.mutable_text_state() = m_pCurStates->text_state();
  auto form = std::make_unique<CPDF_Form>(
      m_pDocument, m_pPageResources, std::move(pStream), m_pResources.Get());
  form->ParseContent(&status, nullptr, m_RecursionState);

  CFX_Matrix matrix =
      m_pCurStates->current_transformation_matrix() * m_mtContentToUser;
  auto pFormObj = std::make_unique<CPDF_FormObject>(GetCurrentStreamIndex(),
                                                    std::move(form), matrix);
  pFormObj->SetResourceName(name);
  if (!m_pObjectHolder->BackgroundAlphaNeeded() &&
      pFormObj->form()->BackgroundAlphaNeeded()) {
    m_pObjectHolder->SetBackgroundAlphaNeeded(true);
  }
  pFormObj->CalcBoundingBox();
  SetGraphicStates(pFormObj.get(), true, true, true);
  m_pObjectHolder->AppendPageObject(std::move(pFormObj));
}

CPDF_ImageObject* CPDF_StreamContentParser::AddImageFromStream(
    RetainPtr<CPDF_Stream> pStream,
    const ByteString& name) {
  if (!pStream)
    return nullptr;

  auto pImageObj = std::make_unique<CPDF_ImageObject>(GetCurrentStreamIndex());
  pImageObj->SetResourceName(name);
  pImageObj->SetImage(
      pdfium::MakeRetain<CPDF_Image>(m_pDocument, std::move(pStream)));

  return AddImageObject(std::move(pImageObj));
}

CPDF_ImageObject* CPDF_StreamContentParser::AddImageFromStreamObjNum(
    uint32_t stream_obj_num,
    const ByteString& name) {
  auto pImageObj = std::make_unique<CPDF_ImageObject>(GetCurrentStreamIndex());
  pImageObj->SetResourceName(name);
  pImageObj->SetImage(
      CPDF_DocPageData::FromDocument(m_pDocument)->GetImage(stream_obj_num));

  return AddImageObject(std::move(pImageObj));
}

CPDF_ImageObject* CPDF_StreamContentParser::AddLastImage() {
  DCHECK(m_pLastImage);

  auto pImageObj = std::make_unique<CPDF_ImageObject>(GetCurrentStreamIndex());
  pImageObj->SetResourceName(m_LastImageName);
  pImageObj->SetImage(CPDF_DocPageData::FromDocument(m_pDocument)
                          ->GetImage(m_pLastImage->GetStream()->GetObjNum()));

  return AddImageObject(std::move(pImageObj));
}

CPDF_ImageObject* CPDF_StreamContentParser::AddImageObject(
    std::unique_ptr<CPDF_ImageObject> pImageObj) {
  SetGraphicStates(pImageObj.get(), pImageObj->GetImage()->IsMask(), false,
                   false);

  CFX_Matrix ImageMatrix =
      m_pCurStates->current_transformation_matrix() * m_mtContentToUser;
  pImageObj->SetImageMatrix(ImageMatrix);

  CPDF_ImageObject* pRet = pImageObj.get();
  m_pObjectHolder->AppendPageObject(std::move(pImageObj));
  return pRet;
}

std::vector<float> CPDF_StreamContentParser::GetColors() const {
  DCHECK(m_ParamCount > 0);
  return GetNumbers(m_ParamCount);
}

std::vector<float> CPDF_StreamContentParser::GetNamedColors() const {
  DCHECK(m_ParamCount > 0);
  const uint32_t nvalues = m_ParamCount - 1;
  std::vector<float> values(nvalues);
  for (size_t i = 0; i < nvalues; ++i)
    values[i] = GetNumber(m_ParamCount - i - 1);
  return values;
}

void CPDF_StreamContentParser::Handle_MarkPlace_Dictionary() {}

void CPDF_StreamContentParser::Handle_EndImage() {}

void CPDF_StreamContentParser::Handle_EndMarkedContent() {
  // First element is a sentinel, so do not pop it, ever. This may come up if
  // the EMCs are mismatched with the BMC/BDCs.
  if (m_ContentMarksStack.size() > 1)
    m_ContentMarksStack.pop();
}

void CPDF_StreamContentParser::Handle_EndText() {
  if (m_ClipTextList.empty())
    return;

  if (TextRenderingModeIsClipMode(m_pCurStates->text_state().GetTextMode())) {
    m_pCurStates->mutable_clip_path().AppendTexts(&m_ClipTextList);
  }

  m_ClipTextList.clear();
}

void CPDF_StreamContentParser::Handle_FillPath() {
  AddPathObject(CFX_FillRenderOptions::FillType::kWinding, RenderType::kFill);
}

void CPDF_StreamContentParser::Handle_FillPathOld() {
  AddPathObject(CFX_FillRenderOptions::FillType::kWinding, RenderType::kFill);
}

void CPDF_StreamContentParser::Handle_EOFillPath() {
  AddPathObject(CFX_FillRenderOptions::FillType::kEvenOdd, RenderType::kFill);
}

void CPDF_StreamContentParser::Handle_SetGray_Fill() {
  m_pCurStates->mutable_color_state().SetFillColor(
      CPDF_ColorSpace::GetStockCS(CPDF_ColorSpace::Family::kDeviceGray),
      GetNumbers(1));
}

void CPDF_StreamContentParser::Handle_SetGray_Stroke() {
  m_pCurStates->mutable_color_state().SetStrokeColor(
      CPDF_ColorSpace::GetStockCS(CPDF_ColorSpace::Family::kDeviceGray),
      GetNumbers(1));
}

void CPDF_StreamContentParser::Handle_SetExtendGraphState() {
  ByteString name = GetString(0);
  RetainPtr<CPDF_Dictionary> pGS =
      ToDictionary(FindResourceObj("ExtGState", name));
  if (!pGS)
    return;

  CHECK(!name.IsEmpty());
  m_pCurStates->mutable_general_state().AppendGraphicsResourceName(
      std::move(name));
  m_pCurStates->ProcessExtGS(pGS.Get(), this);
}

void CPDF_StreamContentParser::Handle_ClosePath() {
  if (m_PathPoints.empty())
    return;

  if (m_PathStart.x != m_PathCurrent.x || m_PathStart.y != m_PathCurrent.y) {
    AddPathPointAndClose(m_PathStart, CFX_Path::Point::Type::kLine);
  } else {
    m_PathPoints.back().m_CloseFigure = true;
  }
}

void CPDF_StreamContentParser::Handle_SetFlat() {
  m_pCurStates->mutable_general_state().SetFlatness(GetNumber(0));
}

void CPDF_StreamContentParser::Handle_BeginImageData() {}

void CPDF_StreamContentParser::Handle_SetLineJoin() {
  m_pCurStates->mutable_graph_state().SetLineJoin(
      static_cast<CFX_GraphStateData::LineJoin>(GetInteger(0)));
}

void CPDF_StreamContentParser::Handle_SetLineCap() {
  m_pCurStates->mutable_graph_state().SetLineCap(
      static_cast<CFX_GraphStateData::LineCap>(GetInteger(0)));
}

void CPDF_StreamContentParser::Handle_SetCMYKColor_Fill() {
  if (m_ParamCount != 4)
    return;

  m_pCurStates->mutable_color_state().SetFillColor(
      CPDF_ColorSpace::GetStockCS(CPDF_ColorSpace::Family::kDeviceCMYK),
      GetNumbers(4));
}

void CPDF_StreamContentParser::Handle_SetCMYKColor_Stroke() {
  if (m_ParamCount != 4)
    return;

  m_pCurStates->mutable_color_state().SetStrokeColor(
      CPDF_ColorSpace::GetStockCS(CPDF_ColorSpace::Family::kDeviceCMYK),
      GetNumbers(4));
}

void CPDF_StreamContentParser::Handle_LineTo() {
  if (m_ParamCount != 2)
    return;

  AddPathPoint(GetPoint(0), CFX_Path::Point::Type::kLine);
}

void CPDF_StreamContentParser::Handle_MoveTo() {
  if (m_ParamCount != 2)
    return;

  AddPathPoint(GetPoint(0), CFX_Path::Point::Type::kMove);
  ParsePathObject();
}

void CPDF_StreamContentParser::Handle_SetMiterLimit() {
  m_pCurStates->mutable_graph_state().SetMiterLimit(GetNumber(0));
}

void CPDF_StreamContentParser::Handle_MarkPlace() {}

void CPDF_StreamContentParser::Handle_EndPath() {
  AddPathObject(CFX_FillRenderOptions::FillType::kNoFill, RenderType::kFill);
}

void CPDF_StreamContentParser::Handle_SaveGraphState() {
  m_StateStack.push_back(std::make_unique<CPDF_AllStates>(*m_pCurStates));
}

void CPDF_StreamContentParser::Handle_RestoreGraphState() {
  if (m_StateStack.empty()) {
    return;
  }

  *m_pCurStates = *m_StateStack.back();
  m_StateStack.pop_back();
  m_AllCTMs[GetCurrentStreamIndex()] =
      m_pCurStates->current_transformation_matrix();
}

void CPDF_StreamContentParser::Handle_Rectangle() {
  float x = GetNumber(3);
  float y = GetNumber(2);
  float w = GetNumber(1);
  float h = GetNumber(0);
  AddPathRect(x, y, w, h);
}

void CPDF_StreamContentParser::AddPathRect(float x, float y, float w, float h) {
  AddPathPoint({x, y}, CFX_Path::Point::Type::kMove);
  AddPathPoint({x + w, y}, CFX_Path::Point::Type::kLine);
  AddPathPoint({x + w, y + h}, CFX_Path::Point::Type::kLine);
  AddPathPoint({x, y + h}, CFX_Path::Point::Type::kLine);
  AddPathPointAndClose({x, y}, CFX_Path::Point::Type::kLine);
}

void CPDF_StreamContentParser::Handle_SetRGBColor_Fill() {
  if (m_ParamCount != 3)
    return;

  m_pCurStates->mutable_color_state().SetFillColor(
      CPDF_ColorSpace::GetStockCS(CPDF_ColorSpace::Family::kDeviceRGB),
      GetNumbers(3));
}

void CPDF_StreamContentParser::Handle_SetRGBColor_Stroke() {
  if (m_ParamCount != 3)
    return;

  m_pCurStates->mutable_color_state().SetStrokeColor(
      CPDF_ColorSpace::GetStockCS(CPDF_ColorSpace::Family::kDeviceRGB),
      GetNumbers(3));
}

void CPDF_StreamContentParser::Handle_SetRenderIntent() {}

void CPDF_StreamContentParser::Handle_CloseStrokePath() {
  Handle_ClosePath();
  AddPathObject(CFX_FillRenderOptions::FillType::kNoFill, RenderType::kStroke);
}

void CPDF_StreamContentParser::Handle_StrokePath() {
  AddPathObject(CFX_FillRenderOptions::FillType::kNoFill, RenderType::kStroke);
}

void CPDF_StreamContentParser::Handle_SetColor_Fill() {
  int nargs = std::min(m_ParamCount, 4U);
  m_pCurStates->mutable_color_state().SetFillColor(nullptr, GetNumbers(nargs));
}

void CPDF_StreamContentParser::Handle_SetColor_Stroke() {
  int nargs = std::min(m_ParamCount, 4U);
  m_pCurStates->mutable_color_state().SetStrokeColor(nullptr,
                                                     GetNumbers(nargs));
}

void CPDF_StreamContentParser::Handle_SetColorPS_Fill() {
  RetainPtr<CPDF_Object> pLastParam = GetObject(0);
  if (!pLastParam)
    return;

  if (!pLastParam->IsName()) {
    m_pCurStates->mutable_color_state().SetFillColor(nullptr, GetColors());
    return;
  }

  // A valid |pLastParam| implies |m_ParamCount| > 0, so GetNamedColors() call
  // below is safe.
  RetainPtr<CPDF_Pattern> pPattern = FindPattern(GetString(0));
  if (!pPattern)
    return;

  std::vector<float> values = GetNamedColors();
  m_pCurStates->mutable_color_state().SetFillPattern(std::move(pPattern),
                                                     values);
}

void CPDF_StreamContentParser::Handle_SetColorPS_Stroke() {
  RetainPtr<CPDF_Object> pLastParam = GetObject(0);
  if (!pLastParam)
    return;

  if (!pLastParam->IsName()) {
    m_pCurStates->mutable_color_state().SetStrokeColor(nullptr, GetColors());
    return;
  }

  // A valid |pLastParam| implies |m_ParamCount| > 0, so GetNamedColors() call
  // below is safe.
  RetainPtr<CPDF_Pattern> pPattern = FindPattern(GetString(0));
  if (!pPattern)
    return;

  std::vector<float> values = GetNamedColors();
  m_pCurStates->mutable_color_state().SetStrokePattern(std::move(pPattern),
                                                       values);
}

void CPDF_StreamContentParser::Handle_ShadeFill() {
  RetainPtr<CPDF_ShadingPattern> pShading = FindShading(GetString(0));
  if (!pShading)
    return;

  if (!pShading->IsShadingObject() || !pShading->Load())
    return;

  CFX_Matrix matrix =
      m_pCurStates->current_transformation_matrix() * m_mtContentToUser;
  auto pObj = std::make_unique<CPDF_ShadingObject>(GetCurrentStreamIndex(),
                                                   pShading, matrix);
  SetGraphicStates(pObj.get(), false, false, false);
  CFX_FloatRect bbox =
      pObj->clip_path().HasRef() ? pObj->clip_path().GetClipBox() : m_BBox;
  if (pShading->IsMeshShading())
    bbox.Intersect(GetShadingBBox(pShading.Get(), pObj->matrix()));
  pObj->SetRect(bbox);
  m_pObjectHolder->AppendPageObject(std::move(pObj));
}

void CPDF_StreamContentParser::Handle_SetCharSpace() {
  m_pCurStates->mutable_text_state().SetCharSpace(GetNumber(0));
}

void CPDF_StreamContentParser::Handle_MoveTextPoint() {
  m_pCurStates->MoveTextPoint(GetPoint(0));
}

void CPDF_StreamContentParser::Handle_MoveTextPoint_SetLeading() {
  Handle_MoveTextPoint();
  m_pCurStates->set_text_leading(-GetNumber(0));
}

void CPDF_StreamContentParser::Handle_SetFont() {
  m_pCurStates->mutable_text_state().SetFontSize(GetNumber(0));
  RetainPtr<CPDF_Font> pFont = FindFont(GetString(1));
  if (pFont)
    m_pCurStates->mutable_text_state().SetFont(std::move(pFont));
}

RetainPtr<CPDF_Dictionary> CPDF_StreamContentParser::FindResourceHolder(
    const ByteString& type) {
  if (!m_pResources)
    return nullptr;

  RetainPtr<CPDF_Dictionary> pDict = m_pResources->GetMutableDictFor(type);
  if (pDict)
    return pDict;

  if (m_pResources == m_pPageResources || !m_pPageResources)
    return nullptr;

  return m_pPageResources->GetMutableDictFor(type);
}

RetainPtr<CPDF_Object> CPDF_StreamContentParser::FindResourceObj(
    const ByteString& type,
    const ByteString& name) {
  RetainPtr<CPDF_Dictionary> pHolder = FindResourceHolder(type);
  return pHolder ? pHolder->GetMutableDirectObjectFor(name) : nullptr;
}

RetainPtr<CPDF_Font> CPDF_StreamContentParser::FindFont(
    const ByteString& name) {
  RetainPtr<CPDF_Dictionary> pFontDict(
      ToDictionary(FindResourceObj("Font", name)));
  if (!pFontDict) {
    return CPDF_Font::GetStockFont(m_pDocument, CFX_Font::kDefaultAnsiFontName);
  }
  RetainPtr<CPDF_Font> pFont = CPDF_DocPageData::FromDocument(m_pDocument)
                                   ->GetFont(std::move(pFontDict));
  if (pFont) {
    // Save `name` for later retrieval by the CPDF_TextObject that uses the
    // font.
    pFont->SetResourceName(name);
    if (pFont->IsType3Font()) {
      pFont->AsType3Font()->SetPageResources(m_pResources.Get());
      pFont->AsType3Font()->CheckType3FontMetrics();
    }
  }
  return pFont;
}

CPDF_PageObjectHolder::CTMMap CPDF_StreamContentParser::TakeAllCTMs() {
  CPDF_PageObjectHolder::CTMMap all_ctms;
  all_ctms.swap(m_AllCTMs);
  return all_ctms;
}

RetainPtr<CPDF_ColorSpace> CPDF_StreamContentParser::FindColorSpace(
    const ByteString& name) {
  if (name == "Pattern")
    return CPDF_ColorSpace::GetStockCS(CPDF_ColorSpace::Family::kPattern);

  if (name == "DeviceGray" || name == "DeviceCMYK" || name == "DeviceRGB") {
    ByteString defname = "Default";
    defname += name.Last(name.GetLength() - 7);
    RetainPtr<const CPDF_Object> pDefObj =
        FindResourceObj("ColorSpace", defname);
    if (!pDefObj) {
      if (name == "DeviceGray") {
        return CPDF_ColorSpace::GetStockCS(
            CPDF_ColorSpace::Family::kDeviceGray);
      }
      if (name == "DeviceRGB")
        return CPDF_ColorSpace::GetStockCS(CPDF_ColorSpace::Family::kDeviceRGB);

      return CPDF_ColorSpace::GetStockCS(CPDF_ColorSpace::Family::kDeviceCMYK);
    }
    return CPDF_DocPageData::FromDocument(m_pDocument)
        ->GetColorSpace(pDefObj.Get(), nullptr);
  }
  RetainPtr<const CPDF_Object> pCSObj = FindResourceObj("ColorSpace", name);
  if (!pCSObj)
    return nullptr;
  return CPDF_DocPageData::FromDocument(m_pDocument)
      ->GetColorSpace(pCSObj.Get(), nullptr);
}

RetainPtr<CPDF_Pattern> CPDF_StreamContentParser::FindPattern(
    const ByteString& name) {
  RetainPtr<CPDF_Object> pPattern = FindResourceObj("Pattern", name);
  if (!pPattern || (!pPattern->IsDictionary() && !pPattern->IsStream()))
    return nullptr;
  return CPDF_DocPageData::FromDocument(m_pDocument)
      ->GetPattern(std::move(pPattern), m_pCurStates->parent_matrix());
}

RetainPtr<CPDF_ShadingPattern> CPDF_StreamContentParser::FindShading(
    const ByteString& name) {
  RetainPtr<CPDF_Object> pPattern = FindResourceObj("Shading", name);
  if (!pPattern || (!pPattern->IsDictionary() && !pPattern->IsStream()))
    return nullptr;
  return CPDF_DocPageData::FromDocument(m_pDocument)
      ->GetShading(std::move(pPattern), m_pCurStates->parent_matrix());
}

void CPDF_StreamContentParser::AddTextObject(
    pdfium::span<const ByteString> strings,
    pdfium::span<const float> kernings,
    float initial_kerning) {
  RetainPtr<CPDF_Font> pFont = m_pCurStates->text_state().GetFont();
  if (!pFont) {
    return;
  }
  if (initial_kerning != 0) {
    if (pFont->IsVertWriting()) {
      m_pCurStates->IncrementTextPositionY(
          -GetVerticalTextSize(initial_kerning));
    } else {
      m_pCurStates->IncrementTextPositionX(
          -GetHorizontalTextSize(initial_kerning));
    }
  }
  if (strings.empty()) {
    return;
  }
  const TextRenderingMode text_mode =
      pFont->IsType3Font() ? TextRenderingMode::MODE_FILL
                           : m_pCurStates->text_state().GetTextMode();
  {
    auto pText = std::make_unique<CPDF_TextObject>(GetCurrentStreamIndex());
    pText->SetResourceName(pFont->GetResourceName());
    SetGraphicStates(pText.get(), true, true, true);
    if (TextRenderingModeIsStrokeMode(text_mode)) {
      const CFX_Matrix& ctm = m_pCurStates->current_transformation_matrix();
      pdfium::span<float> text_ctm =
          pText->mutable_text_state().GetMutableCTM();
      text_ctm[0] = ctm.a;
      text_ctm[1] = ctm.c;
      text_ctm[2] = ctm.b;
      text_ctm[3] = ctm.d;
    }
    pText->SetSegments(strings, kernings);
    pText->SetPosition(m_mtContentToUser.Transform(
        m_pCurStates->GetTransformedTextPosition()));

    const CFX_PointF position =
        pText->CalcPositionData(m_pCurStates->text_horz_scale());
    m_pCurStates->IncrementTextPositionX(position.x);
    m_pCurStates->IncrementTextPositionY(position.y);
    if (TextRenderingModeIsClipMode(text_mode))
      m_ClipTextList.push_back(pText->Clone());
    m_pObjectHolder->AppendPageObject(std::move(pText));
  }
  if (!kernings.empty() && kernings.back() != 0) {
    if (pFont->IsVertWriting())
      m_pCurStates->IncrementTextPositionY(
          -GetVerticalTextSize(kernings.back()));
    else
      m_pCurStates->IncrementTextPositionX(
          -GetHorizontalTextSize(kernings.back()));
  }
}

float CPDF_StreamContentParser::GetHorizontalTextSize(float fKerning) const {
  return GetVerticalTextSize(fKerning) * m_pCurStates->text_horz_scale();
}

float CPDF_StreamContentParser::GetVerticalTextSize(float fKerning) const {
  return fKerning * m_pCurStates->text_state().GetFontSize() / 1000;
}

int32_t CPDF_StreamContentParser::GetCurrentStreamIndex() {
  auto it =
      std::upper_bound(m_StreamStartOffsets.begin(), m_StreamStartOffsets.end(),
                       m_pSyntax->GetPos() + m_StartParseOffset);
  return (it - m_StreamStartOffsets.begin()) - 1;
}

void CPDF_StreamContentParser::Handle_ShowText() {
  ByteString str = GetString(0);
  if (!str.IsEmpty()) {
    AddTextObject(pdfium::span_from_ref(str), pdfium::span<float>(), 0.0f);
  }
}

void CPDF_StreamContentParser::Handle_ShowText_Positioning() {
  RetainPtr<CPDF_Array> pArray = ToArray(GetObject(0));
  if (!pArray)
    return;

  size_t n = pArray->size();
  size_t nsegs = 0;
  for (size_t i = 0; i < n; i++) {
    RetainPtr<const CPDF_Object> pDirectObject = pArray->GetDirectObjectAt(i);
    if (pDirectObject && pDirectObject->IsString())
      nsegs++;
  }
  if (nsegs == 0) {
    for (size_t i = 0; i < n; i++) {
      float fKerning = pArray->GetFloatAt(i);
      if (fKerning != 0)
        m_pCurStates->IncrementTextPositionX(-GetHorizontalTextSize(fKerning));
    }
    return;
  }
  std::vector<ByteString> strs(nsegs);
  std::vector<float> kernings(nsegs);
  size_t iSegment = 0;
  float fInitKerning = 0;
  for (size_t i = 0; i < n; i++) {
    RetainPtr<const CPDF_Object> pObj = pArray->GetDirectObjectAt(i);
    if (!pObj)
      continue;

    if (pObj->IsString()) {
      ByteString str = pObj->GetString();
      if (str.IsEmpty())
        continue;
      strs[iSegment] = std::move(str);
      kernings[iSegment++] = 0;
    } else {
      float num = pObj->GetNumber();
      if (iSegment == 0)
        fInitKerning += num;
      else
        kernings[iSegment - 1] += num;
    }
  }
  AddTextObject(pdfium::make_span(strs).first(iSegment), kernings,
                fInitKerning);
}

void CPDF_StreamContentParser::Handle_SetTextLeading() {
  m_pCurStates->set_text_leading(GetNumber(0));
}

void CPDF_StreamContentParser::Handle_SetTextMatrix() {
  m_pCurStates->set_text_matrix(GetMatrix());
  OnChangeTextMatrix();
  m_pCurStates->ResetTextPosition();
}

void CPDF_StreamContentParser::OnChangeTextMatrix() {
  CFX_Matrix text_matrix(m_pCurStates->text_horz_scale(), 0.0f, 0.0f, 1.0f,
                         0.0f, 0.0f);
  text_matrix.Concat(m_pCurStates->text_matrix());
  text_matrix.Concat(m_pCurStates->current_transformation_matrix());
  text_matrix.Concat(m_mtContentToUser);
  pdfium::span<float> pTextMatrix =
      m_pCurStates->mutable_text_state().GetMutableMatrix();
  pTextMatrix[0] = text_matrix.a;
  pTextMatrix[1] = text_matrix.c;
  pTextMatrix[2] = text_matrix.b;
  pTextMatrix[3] = text_matrix.d;
}

void CPDF_StreamContentParser::Handle_SetTextRenderMode() {
  TextRenderingMode mode;
  if (SetTextRenderingModeFromInt(GetInteger(0), &mode))
    m_pCurStates->mutable_text_state().SetTextMode(mode);
}

void CPDF_StreamContentParser::Handle_SetTextRise() {
  m_pCurStates->set_text_rise(GetNumber(0));
}

void CPDF_StreamContentParser::Handle_SetWordSpace() {
  m_pCurStates->mutable_text_state().SetWordSpace(GetNumber(0));
}

void CPDF_StreamContentParser::Handle_SetHorzScale() {
  if (m_ParamCount != 1) {
    return;
  }
  m_pCurStates->set_text_horz_scale(GetNumber(0) / 100);
  OnChangeTextMatrix();
}

void CPDF_StreamContentParser::Handle_MoveToNextLine() {
  m_pCurStates->MoveTextToNextLine();
}

void CPDF_StreamContentParser::Handle_CurveTo_23() {
  AddPathPoint(m_PathCurrent, CFX_Path::Point::Type::kBezier);
  AddPathPoint(GetPoint(2), CFX_Path::Point::Type::kBezier);
  AddPathPoint(GetPoint(0), CFX_Path::Point::Type::kBezier);
}

void CPDF_StreamContentParser::Handle_SetLineWidth() {
  m_pCurStates->mutable_graph_state().SetLineWidth(GetNumber(0));
}

void CPDF_StreamContentParser::Handle_Clip() {
  m_PathClipType = CFX_FillRenderOptions::FillType::kWinding;
}

void CPDF_StreamContentParser::Handle_EOClip() {
  m_PathClipType = CFX_FillRenderOptions::FillType::kEvenOdd;
}

void CPDF_StreamContentParser::Handle_CurveTo_13() {
  AddPathPoint(GetPoint(2), CFX_Path::Point::Type::kBezier);
  AddPathPoint(GetPoint(0), CFX_Path::Point::Type::kBezier);
  AddPathPoint(GetPoint(0), CFX_Path::Point::Type::kBezier);
}

void CPDF_StreamContentParser::Handle_NextLineShowText() {
  Handle_MoveToNextLine();
  Handle_ShowText();
}

void CPDF_StreamContentParser::Handle_NextLineShowText_Space() {
  m_pCurStates->mutable_text_state().SetWordSpace(GetNumber(2));
  m_pCurStates->mutable_text_state().SetCharSpace(GetNumber(1));
  Handle_NextLineShowText();
}

void CPDF_StreamContentParser::Handle_Invalid() {}

void CPDF_StreamContentParser::AddPathPoint(const CFX_PointF& point,
                                            CFX_Path::Point::Type type) {
  // If the path point is the same move as the previous one and neither of them
  // closes the path, then just skip it.
  if (type == CFX_Path::Point::Type::kMove && !m_PathPoints.empty() &&
      !m_PathPoints.back().m_CloseFigure &&
      m_PathPoints.back().m_Type == type && m_PathCurrent == point) {
    return;
  }

  m_PathCurrent = point;
  if (type == CFX_Path::Point::Type::kMove) {
    m_PathStart = point;
    if (!m_PathPoints.empty() &&
        m_PathPoints.back().IsTypeAndOpen(CFX_Path::Point::Type::kMove)) {
      m_PathPoints.back().m_Point = point;
      return;
    }
  } else if (m_PathPoints.empty()) {
    return;
  }
  m_PathPoints.emplace_back(point, type, /*close=*/false);
}

void CPDF_StreamContentParser::AddPathPointAndClose(
    const CFX_PointF& point,
    CFX_Path::Point::Type type) {
  m_PathCurrent = point;
  if (m_PathPoints.empty())
    return;

  m_PathPoints.emplace_back(point, type, /*close=*/true);
}

void CPDF_StreamContentParser::AddPathObject(
    CFX_FillRenderOptions::FillType fill_type,
    RenderType render_type) {
  std::vector<CFX_Path::Point> path_points;
  path_points.swap(m_PathPoints);
  CFX_FillRenderOptions::FillType path_clip_type = m_PathClipType;
  m_PathClipType = CFX_FillRenderOptions::FillType::kNoFill;

  if (path_points.empty())
    return;

  if (path_points.size() == 1) {
    if (path_clip_type != CFX_FillRenderOptions::FillType::kNoFill) {
      CPDF_Path path;
      path.AppendRect(0, 0, 0, 0);
      m_pCurStates->mutable_clip_path().AppendPathWithAutoMerge(
          path, CFX_FillRenderOptions::FillType::kWinding);
      return;
    }

    CFX_Path::Point& point = path_points.front();
    if (point.m_Type != CFX_Path::Point::Type::kMove || !point.m_CloseFigure ||
        m_pCurStates->graph_state().GetLineCap() !=
            CFX_GraphStateData::LineCap::kRound) {
      return;
    }

    // For round line cap only: When a path moves to a point and immediately
    // gets closed, we can treat it as drawing a path from this point to itself
    // and closing the path. This should not apply to butt line cap or
    // projecting square line cap since they should not be rendered.
    point.m_CloseFigure = false;
    path_points.emplace_back(point.m_Point, CFX_Path::Point::Type::kLine,
                             /*close=*/true);
  }

  if (path_points.back().IsTypeAndOpen(CFX_Path::Point::Type::kMove))
    path_points.pop_back();

  CPDF_Path path;
  for (const auto& point : path_points) {
    if (point.m_CloseFigure)
      path.AppendPointAndClose(point.m_Point, point.m_Type);
    else
      path.AppendPoint(point.m_Point, point.m_Type);
  }

  CFX_Matrix matrix =
      m_pCurStates->current_transformation_matrix() * m_mtContentToUser;
  bool bStroke = render_type == RenderType::kStroke;
  if (bStroke || fill_type != CFX_FillRenderOptions::FillType::kNoFill) {
    auto pPathObj = std::make_unique<CPDF_PathObject>(GetCurrentStreamIndex());
    pPathObj->set_stroke(bStroke);
    pPathObj->set_filltype(fill_type);
    pPathObj->path() = path;
    SetGraphicStates(pPathObj.get(), true, false, true);
    pPathObj->SetPathMatrix(matrix);
    m_pObjectHolder->AppendPageObject(std::move(pPathObj));
  }
  if (path_clip_type != CFX_FillRenderOptions::FillType::kNoFill) {
    if (!matrix.IsIdentity())
      path.Transform(matrix);
    m_pCurStates->mutable_clip_path().AppendPathWithAutoMerge(path,
                                                              path_clip_type);
  }
}

uint32_t CPDF_StreamContentParser::Parse(
    pdfium::span<const uint8_t> pData,
    uint32_t start_offset,
    uint32_t max_cost,
    const std::vector<uint32_t>& stream_start_offsets) {
  DCHECK(start_offset < pData.size());

  // Parsing will be done from within |pDataStart|.
  pdfium::span<const uint8_t> pDataStart = pData.subspan(start_offset);
  m_StartParseOffset = start_offset;
  if (m_RecursionState->parsed_set.size() > kMaxFormLevel ||
      pdfium::Contains(m_RecursionState->parsed_set, pDataStart.data())) {
    return fxcrt::CollectionSize<uint32_t>(pDataStart);
  }

  m_StreamStartOffsets = stream_start_offsets;

  ScopedSetInsertion<const uint8_t*> scoped_insert(
      &m_RecursionState->parsed_set, pDataStart.data());

  uint32_t init_obj_count = m_pObjectHolder->GetPageObjectCount();
  AutoNuller<std::unique_ptr<CPDF_StreamParser>> auto_clearer(&m_pSyntax);
  m_pSyntax = std::make_unique<CPDF_StreamParser>(
      pDataStart, m_pDocument->GetByteStringPool());

  while (true) {
    uint32_t cost = m_pObjectHolder->GetPageObjectCount() - init_obj_count;
    if (max_cost && cost >= max_cost) {
      break;
    }
    switch (m_pSyntax->ParseNextElement()) {
      case CPDF_StreamParser::ElementType::kEndOfData:
        return m_pSyntax->GetPos();
      case CPDF_StreamParser::ElementType::kKeyword:
        OnOperator(m_pSyntax->GetWord());
        ClearAllParams();
        break;
      case CPDF_StreamParser::ElementType::kNumber:
        AddNumberParam(m_pSyntax->GetWord());
        break;
      case CPDF_StreamParser::ElementType::kName: {
        auto word = m_pSyntax->GetWord();
        AddNameParam(word.Last(word.GetLength() - 1));
        break;
      }
      default:
        AddObjectParam(m_pSyntax->GetObject());
    }
  }
  return m_pSyntax->GetPos();
}

void CPDF_StreamContentParser::ParsePathObject() {
  std::array<float, 6> params = {};
  int nParams = 0;
  int last_pos = m_pSyntax->GetPos();
  while (true) {
    CPDF_StreamParser::ElementType type = m_pSyntax->ParseNextElement();
    bool bProcessed = true;
    switch (type) {
      case CPDF_StreamParser::ElementType::kEndOfData:
        return;
      case CPDF_StreamParser::ElementType::kKeyword: {
        ByteStringView strc = m_pSyntax->GetWord();
        int len = strc.GetLength();
        if (len == 1) {
          switch (strc[0]) {
            case kPathOperatorSubpath:
              AddPathPoint({params[0], params[1]},
                           CFX_Path::Point::Type::kMove);
              nParams = 0;
              break;
            case kPathOperatorLine:
              AddPathPoint({params[0], params[1]},
                           CFX_Path::Point::Type::kLine);
              nParams = 0;
              break;
            case kPathOperatorCubicBezier1:
              AddPathPoint({params[0], params[1]},
                           CFX_Path::Point::Type::kBezier);
              AddPathPoint({params[2], params[3]},
                           CFX_Path::Point::Type::kBezier);
              AddPathPoint({params[4], params[5]},
                           CFX_Path::Point::Type::kBezier);
              nParams = 0;
              break;
            case kPathOperatorCubicBezier2:
              AddPathPoint(m_PathCurrent, CFX_Path::Point::Type::kBezier);
              AddPathPoint({params[0], params[1]},
                           CFX_Path::Point::Type::kBezier);
              AddPathPoint({params[2], params[3]},
                           CFX_Path::Point::Type::kBezier);
              nParams = 0;
              break;
            case kPathOperatorCubicBezier3:
              AddPathPoint({params[0], params[1]},
                           CFX_Path::Point::Type::kBezier);
              AddPathPoint({params[2], params[3]},
                           CFX_Path::Point::Type::kBezier);
              AddPathPoint({params[2], params[3]},
                           CFX_Path::Point::Type::kBezier);
              nParams = 0;
              break;
            case kPathOperatorClosePath:
              Handle_ClosePath();
              nParams = 0;
              break;
            default:
              bProcessed = false;
              break;
          }
        } else if (len == 2) {
          if (strc[0] == kPathOperatorRectangle[0] &&
              strc[1] == kPathOperatorRectangle[1]) {
            AddPathRect(params[0], params[1], params[2], params[3]);
            nParams = 0;
          } else {
            bProcessed = false;
          }
        } else {
          bProcessed = false;
        }
        if (bProcessed) {
          last_pos = m_pSyntax->GetPos();
        }
        break;
      }
      case CPDF_StreamParser::ElementType::kNumber: {
        if (nParams == 6)
          break;

        FX_Number number(m_pSyntax->GetWord());
        params[nParams++] = number.GetFloat();
        break;
      }
      default:
        bProcessed = false;
    }
    if (!bProcessed) {
      m_pSyntax->SetPos(last_pos);
      return;
    }
  }
}

// static
ByteStringView CPDF_StreamContentParser::FindKeyAbbreviationForTesting(
    ByteStringView abbr) {
  return FindFullName(kInlineKeyAbbr, abbr);
}

// static
ByteStringView CPDF_StreamContentParser::FindValueAbbreviationForTesting(
    ByteStringView abbr) {
  return FindFullName(kInlineValueAbbr, abbr);
}

CPDF_StreamContentParser::ContentParam::ContentParam() = default;

CPDF_StreamContentParser::ContentParam::~ContentParam() = default;
