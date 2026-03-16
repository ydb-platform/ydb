// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFTEXT_CPDF_TEXTPAGE_H_
#define CORE_FPDFTEXT_CPDF_TEXTPAGE_H_

#include <stdint.h>

#include <deque>
#include <functional>
#include <optional>
#include <vector>

#include "core/fpdfapi/page/cpdf_pageobjectholder.h"
#include "core/fxcrt/data_vector.h"
#include "core/fxcrt/fx_coordinates.h"
#include "core/fxcrt/fx_memory_wrappers.h"
#include "core/fxcrt/unowned_ptr.h"
#include "core/fxcrt/widestring.h"
#include "core/fxcrt/widetext_buffer.h"

class CPDF_FormObject;
class CPDF_Page;
class CPDF_TextObject;

struct TextPageCharSegment {
  int index;
  int count;
};

FX_DATA_PARTITION_EXCEPTION(TextPageCharSegment);

class CPDF_TextPage {
 public:
  enum class CharType : uint8_t {
    kNormal,
    kGenerated,
    kNotUnicode,
    kHyphen,
    kPiece,
  };

  class CharInfo {
   public:
    CharInfo();
    CharInfo(const CharInfo&);
    ~CharInfo();

    int m_Index = 0;
    uint32_t m_CharCode = 0;
    wchar_t m_Unicode = 0;
    CharType m_CharType = CharType::kNormal;
    CFX_PointF m_Origin;
    CFX_FloatRect m_CharBox;
    UnownedPtr<CPDF_TextObject> m_pTextObj;
    CFX_Matrix m_Matrix;
  };

  CPDF_TextPage(const CPDF_Page* pPage, bool rtl);
  ~CPDF_TextPage();

  int CharIndexFromTextIndex(int text_index) const;
  int TextIndexFromCharIndex(int char_index) const;
  size_t size() const { return m_CharList.size(); }
  int CountChars() const;

  // These methods CHECK() to make sure |index| is within bounds.
  const CharInfo& GetCharInfo(size_t index) const;
  CharInfo& GetCharInfo(size_t index);
  float GetCharFontSize(size_t index) const;
  CFX_FloatRect GetCharLooseBounds(size_t index) const;

  std::vector<CFX_FloatRect> GetRectArray(int start, int count) const;
  int GetIndexAtPos(const CFX_PointF& point, const CFX_SizeF& tolerance) const;
  WideString GetTextByRect(const CFX_FloatRect& rect) const;
  WideString GetTextByObject(const CPDF_TextObject* pTextObj) const;

  // Returns string with the text from |m_TextBuf| that are covered by the input
  // range. |start| and |count| are in terms of the |m_CharIndices|, so the
  // range will be converted into appropriate indices.
  WideString GetPageText(int start, int count) const;
  WideString GetAllPageText() const { return GetPageText(0, CountChars()); }

  int CountRects(int start, int nCount);
  bool GetRect(int rectIndex, CFX_FloatRect* pRect) const;

 private:
  enum class TextOrientation {
    kUnknown,
    kHorizontal,
    kVertical,
  };

  enum class GenerateCharacter {
    kNone,
    kSpace,
    kLineBreak,
    kHyphen,
  };

  enum class MarkedContentState { kPass = 0, kDone, kDelay };

  struct TransformedTextObject {
    TransformedTextObject();
    TransformedTextObject(const TransformedTextObject& that);
    ~TransformedTextObject();

    UnownedPtr<CPDF_TextObject> m_pTextObj;
    CFX_Matrix m_formMatrix;
  };

  void Init();
  bool IsHyphen(wchar_t curChar) const;
  void ProcessObject();
  void ProcessFormObject(CPDF_FormObject* pFormObj,
                         const CFX_Matrix& formMatrix);
  void ProcessTextObject(const TransformedTextObject& obj);
  void ProcessTextObject(CPDF_TextObject* pTextObj,
                         const CFX_Matrix& formMatrix,
                         const CPDF_PageObjectHolder* pObjList,
                         CPDF_PageObjectHolder::const_iterator ObjPos);
  GenerateCharacter ProcessInsertObject(const CPDF_TextObject* pObj,
                                        const CFX_Matrix& formMatrix);
  const CharInfo* GetPrevCharInfo() const;
  std::optional<CharInfo> GenerateCharInfo(wchar_t unicode);
  bool IsSameAsPreTextObject(CPDF_TextObject* pTextObj,
                             const CPDF_PageObjectHolder* pObjList,
                             CPDF_PageObjectHolder::const_iterator iter) const;
  bool IsSameTextObject(CPDF_TextObject* pTextObj1,
                        CPDF_TextObject* pTextObj2) const;
  void CloseTempLine();
  MarkedContentState PreMarkedContent(const CPDF_TextObject* pTextObj);
  void ProcessMarkedContent(const TransformedTextObject& obj);
  void FindPreviousTextObject();
  void AddCharInfoByLRDirection(wchar_t wChar, const CharInfo& info);
  void AddCharInfoByRLDirection(wchar_t wChar, const CharInfo& info);
  TextOrientation GetTextObjectWritingMode(
      const CPDF_TextObject* pTextObj) const;
  TextOrientation FindTextlineFlowOrientation() const;
  void AppendGeneratedCharacter(wchar_t unicode, const CFX_Matrix& formMatrix);
  void SwapTempTextBuf(size_t iCharListStartAppend, size_t iBufStartAppend);
  WideString GetTextByPredicate(
      const std::function<bool(const CharInfo&)>& predicate) const;

  UnownedPtr<const CPDF_Page> const m_pPage;
  DataVector<TextPageCharSegment> m_CharIndices;
  std::deque<CharInfo> m_CharList;
  std::deque<CharInfo> m_TempCharList;
  WideTextBuffer m_TextBuf;
  WideTextBuffer m_TempTextBuf;
  UnownedPtr<const CPDF_TextObject> m_pPrevTextObj;
  CFX_Matrix m_PrevMatrix;
  const bool m_rtl;
  const CFX_Matrix m_DisplayMatrix;
  std::vector<CFX_FloatRect> m_SelRects;
  std::vector<TransformedTextObject> mTextObjects;
  TextOrientation m_TextlineDir = TextOrientation::kUnknown;
  CFX_FloatRect m_CurlineRect;
};

#endif  // CORE_FPDFTEXT_CPDF_TEXTPAGE_H_
