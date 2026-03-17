// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "fpdfsdk/pwl/cpwl_edit_impl.h"

#include <algorithm>
#include <memory>
#include <utility>

#include "core/fpdfapi/font/cpdf_font.h"
#include "core/fpdfapi/render/cpdf_renderoptions.h"
#include "core/fpdfapi/render/cpdf_textrenderer.h"
#include "core/fpdfdoc/cpvt_word.h"
#include "core/fpdfdoc/ipvt_fontmap.h"
#include "core/fxcrt/autorestorer.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/check_op.h"
#include "core/fxcrt/fx_codepage.h"
#include "core/fxge/cfx_fillrenderoptions.h"
#include "core/fxge/cfx_graphstatedata.h"
#include "core/fxge/cfx_path.h"
#include "core/fxge/cfx_renderdevice.h"
#include "fpdfsdk/pwl/cpwl_edit.h"
#include "fpdfsdk/pwl/cpwl_scroll_bar.h"
#include "fpdfsdk/pwl/ipwl_fillernotify.h"

namespace {

const int kEditUndoMaxItems = 10000;

void DrawTextString(CFX_RenderDevice* pDevice,
                    const CFX_PointF& pt,
                    CPDF_Font* pFont,
                    float fFontSize,
                    const CFX_Matrix& mtUser2Device,
                    const ByteString& str,
                    FX_ARGB crTextFill) {
  if (!pFont)
    return;

  CFX_PointF pos = mtUser2Device.Transform(pt);
  CPDF_RenderOptions ro;
  DCHECK(ro.GetOptions().bClearType);
  ro.SetColorMode(CPDF_RenderOptions::kNormal);
  CPDF_TextRenderer::DrawTextString(pDevice, pos.x, pos.y, pFont, fFontSize,
                                    mtUser2Device, str, crTextFill, ro);
}

}  // namespace

CPWL_EditImpl::Iterator::Iterator(CPWL_EditImpl* pEdit,
                                  CPVT_VariableText::Iterator* pVTIterator)
    : m_pEdit(pEdit), m_pVTIterator(pVTIterator) {}

CPWL_EditImpl::Iterator::~Iterator() = default;

bool CPWL_EditImpl::Iterator::NextWord() {
  return m_pVTIterator->NextWord();
}

bool CPWL_EditImpl::Iterator::GetWord(CPVT_Word& word) const {
  DCHECK(m_pEdit);

  if (m_pVTIterator->GetWord(word)) {
    word.ptWord = m_pEdit->VTToEdit(word.ptWord);
    return true;
  }
  return false;
}

bool CPWL_EditImpl::Iterator::GetLine(CPVT_Line& line) const {
  DCHECK(m_pEdit);

  if (m_pVTIterator->GetLine(line)) {
    line.ptLine = m_pEdit->VTToEdit(line.ptLine);
    return true;
  }
  return false;
}

void CPWL_EditImpl::Iterator::SetAt(int32_t nWordIndex) {
  m_pVTIterator->SetAt(nWordIndex);
}

void CPWL_EditImpl::Iterator::SetAt(const CPVT_WordPlace& place) {
  m_pVTIterator->SetAt(place);
}

const CPVT_WordPlace& CPWL_EditImpl::Iterator::GetAt() const {
  return m_pVTIterator->GetWordPlace();
}

class CPWL_EditImpl::Provider final : public CPVT_VariableText::Provider {
 public:
  explicit Provider(IPVT_FontMap* pFontMap);
  ~Provider() override;

  // CPVT_VariableText::Provider:
  int GetCharWidth(int32_t nFontIndex, uint16_t word) override;
  int32_t GetWordFontIndex(uint16_t word,
                           FX_Charset charset,
                           int32_t nFontIndex) override;
};

CPWL_EditImpl::Provider::Provider(IPVT_FontMap* pFontMap)
    : CPVT_VariableText::Provider(pFontMap) {}

CPWL_EditImpl::Provider::~Provider() = default;

int CPWL_EditImpl::Provider::GetCharWidth(int32_t nFontIndex, uint16_t word) {
  RetainPtr<CPDF_Font> pPDFFont = GetFontMap()->GetPDFFont(nFontIndex);
  if (!pPDFFont)
    return 0;

  uint32_t charcode = pPDFFont->IsUnicodeCompatible()
                          ? pPDFFont->CharCodeFromUnicode(word)
                          : GetFontMap()->CharCodeFromUnicode(nFontIndex, word);
  if (charcode == CPDF_Font::kInvalidCharCode)
    return 0;

  return pPDFFont->GetCharWidthF(charcode);
}

int32_t CPWL_EditImpl::Provider::GetWordFontIndex(uint16_t word,
                                                  FX_Charset charset,
                                                  int32_t nFontIndex) {
  return GetFontMap()->GetWordFontIndex(word, charset, nFontIndex);
}

CPWL_EditImpl::RefreshState::RefreshState() = default;

CPWL_EditImpl::RefreshState::~RefreshState() = default;

void CPWL_EditImpl::RefreshState::BeginRefresh() {
  m_OldLineRects = std::move(m_NewLineRects);
  m_NewLineRects.clear();
  m_RefreshRects.clear();
}

void CPWL_EditImpl::RefreshState::Push(const CPVT_WordRange& linerange,
                                       const CFX_FloatRect& rect) {
  m_NewLineRects.emplace_back(linerange, rect);
}

void CPWL_EditImpl::RefreshState::NoAnalyse() {
  for (const auto& lineRect : m_OldLineRects)
    Add(lineRect.m_rcLine);

  for (const auto& lineRect : m_NewLineRects)
    Add(lineRect.m_rcLine);
}

std::vector<CFX_FloatRect>* CPWL_EditImpl::RefreshState::GetRefreshRects() {
  return &m_RefreshRects;
}

void CPWL_EditImpl::RefreshState::EndRefresh() {
  m_RefreshRects.clear();
}

void CPWL_EditImpl::RefreshState::Add(const CFX_FloatRect& new_rect) {
  // Check for overlapped area.
  for (const auto& rect : m_RefreshRects) {
    if (rect.Contains(new_rect))
      return;
  }
  m_RefreshRects.push_back(new_rect);
}

CPWL_EditImpl::UndoStack::UndoStack() = default;

CPWL_EditImpl::UndoStack::~UndoStack() = default;

bool CPWL_EditImpl::UndoStack::CanUndo() const {
  return m_nCurUndoPos > 0;
}

void CPWL_EditImpl::UndoStack::Undo() {
  DCHECK(!m_bWorking);
  m_bWorking = true;
  int undo_remaining = 1;
  while (CanUndo() && undo_remaining > 0) {
    undo_remaining += m_UndoItemStack[m_nCurUndoPos - 1]->Undo();
    m_nCurUndoPos--;
    undo_remaining--;
  }
  DCHECK_EQ(undo_remaining, 0);
  DCHECK(m_bWorking);
  m_bWorking = false;
}

bool CPWL_EditImpl::UndoStack::CanRedo() const {
  return m_nCurUndoPos < m_UndoItemStack.size();
}

CPWL_EditImpl::UndoItemIface* CPWL_EditImpl::UndoStack::GetLastAddItem() {
  CHECK(!m_UndoItemStack.empty());
  return m_UndoItemStack.back().get();
}

void CPWL_EditImpl::UndoStack::Redo() {
  DCHECK(!m_bWorking);
  m_bWorking = true;
  int nRedoRemain = 1;
  while (CanRedo() && nRedoRemain > 0) {
    nRedoRemain += m_UndoItemStack[m_nCurUndoPos]->Redo();
    m_nCurUndoPos++;
    nRedoRemain--;
  }
  DCHECK_EQ(nRedoRemain, 0);
  DCHECK(m_bWorking);
  m_bWorking = false;
}

void CPWL_EditImpl::UndoStack::AddItem(std::unique_ptr<UndoItemIface> pItem) {
  DCHECK(!m_bWorking);
  DCHECK(pItem);
  if (CanRedo())
    RemoveTails();

  if (m_UndoItemStack.size() >= kEditUndoMaxItems)
    RemoveHeads();

  m_UndoItemStack.push_back(std::move(pItem));
  m_nCurUndoPos = m_UndoItemStack.size();
}

void CPWL_EditImpl::UndoStack::RemoveHeads() {
  DCHECK(m_UndoItemStack.size() > 1);
  m_UndoItemStack.pop_front();
}

void CPWL_EditImpl::UndoStack::RemoveTails() {
  while (CanRedo())
    m_UndoItemStack.pop_back();
}

class CPWL_EditImpl::UndoInsertWord final
    : public CPWL_EditImpl::UndoItemIface {
 public:
  UndoInsertWord(CPWL_EditImpl* pEdit,
                 const CPVT_WordPlace& wpOldPlace,
                 const CPVT_WordPlace& wpNewPlace,
                 uint16_t word,
                 FX_Charset charset);
  ~UndoInsertWord() override;

  // UndoItemIface:
  int Redo() override;
  int Undo() override;

 private:
  UnownedPtr<CPWL_EditImpl> m_pEdit;

  CPVT_WordPlace m_wpOld;
  CPVT_WordPlace m_wpNew;
  uint16_t m_Word;
  FX_Charset m_nCharset;
};

CPWL_EditImpl::UndoInsertWord::UndoInsertWord(CPWL_EditImpl* pEdit,
                                              const CPVT_WordPlace& wpOldPlace,
                                              const CPVT_WordPlace& wpNewPlace,
                                              uint16_t word,
                                              FX_Charset charset)
    : m_pEdit(pEdit),
      m_wpOld(wpOldPlace),
      m_wpNew(wpNewPlace),
      m_Word(word),
      m_nCharset(charset) {
  DCHECK(m_pEdit);
}

CPWL_EditImpl::UndoInsertWord::~UndoInsertWord() = default;

int CPWL_EditImpl::UndoInsertWord::Redo() {
  m_pEdit->SelectNone();
  m_pEdit->SetCaret(m_wpOld);
  m_pEdit->InsertWord(m_Word, m_nCharset, false);
  return 0;
}

int CPWL_EditImpl::UndoInsertWord::Undo() {
  m_pEdit->SelectNone();
  m_pEdit->SetCaret(m_wpNew);
  m_pEdit->Backspace(false);
  return 0;
}

class CPWL_EditImpl::UndoInsertReturn final
    : public CPWL_EditImpl::UndoItemIface {
 public:
  UndoInsertReturn(CPWL_EditImpl* pEdit,
                   const CPVT_WordPlace& wpOldPlace,
                   const CPVT_WordPlace& wpNewPlace);
  ~UndoInsertReturn() override;

  // UndoItemIface:
  int Redo() override;
  int Undo() override;

 private:
  UnownedPtr<CPWL_EditImpl> m_pEdit;

  CPVT_WordPlace m_wpOld;
  CPVT_WordPlace m_wpNew;
};

CPWL_EditImpl::UndoInsertReturn::UndoInsertReturn(
    CPWL_EditImpl* pEdit,
    const CPVT_WordPlace& wpOldPlace,
    const CPVT_WordPlace& wpNewPlace)
    : m_pEdit(pEdit), m_wpOld(wpOldPlace), m_wpNew(wpNewPlace) {
  DCHECK(m_pEdit);
}

CPWL_EditImpl::UndoInsertReturn::~UndoInsertReturn() = default;

int CPWL_EditImpl::UndoInsertReturn::Redo() {
  m_pEdit->SelectNone();
  m_pEdit->SetCaret(m_wpOld);
  m_pEdit->InsertReturn(false);
  return 0;
}

int CPWL_EditImpl::UndoInsertReturn::Undo() {
  m_pEdit->SelectNone();
  m_pEdit->SetCaret(m_wpNew);
  m_pEdit->Backspace(false);
  return 0;
}

class CPWL_EditImpl::UndoReplaceSelection final
    : public CPWL_EditImpl::UndoItemIface {
 public:
  UndoReplaceSelection(CPWL_EditImpl* pEdit, bool bIsEnd);
  ~UndoReplaceSelection() override;

  // UndoItemIface:
  int Redo() override;
  int Undo() override;

 private:
  bool IsEnd() const { return m_bEnd; }

  UnownedPtr<CPWL_EditImpl> m_pEdit;
  const bool m_bEnd;  // indicate whether this is the end of replace action
};

CPWL_EditImpl::UndoReplaceSelection::UndoReplaceSelection(CPWL_EditImpl* pEdit,
                                                          bool bIsEnd)
    : m_pEdit(pEdit), m_bEnd(bIsEnd) {
  DCHECK(m_pEdit);
  // Redo ClearSelection, InsertText and ReplaceSelection's end marker
  // Undo InsertText, ClearSelection and ReplaceSelection's beginning
  // marker
  set_undo_remaining(3);
}

CPWL_EditImpl::UndoReplaceSelection::~UndoReplaceSelection() = default;

int CPWL_EditImpl::UndoReplaceSelection::Redo() {
  m_pEdit->SelectNone();
  if (IsEnd()) {
    return 0;
  }
  // Redo ClearSelection, InsertText and ReplaceSelection's end
  // marker. (ClearSelection may not exist)
  return undo_remaining();
}

int CPWL_EditImpl::UndoReplaceSelection::Undo() {
  m_pEdit->SelectNone();
  if (!IsEnd()) {
    return 0;
  }
  // Undo InsertText, ClearSelection and ReplaceSelection's beginning
  // marker. (ClearSelection may not exist)
  return undo_remaining();
}

class CPWL_EditImpl::UndoBackspace final : public CPWL_EditImpl::UndoItemIface {
 public:
  UndoBackspace(CPWL_EditImpl* pEdit,
                const CPVT_WordPlace& wpOldPlace,
                const CPVT_WordPlace& wpNewPlace,
                uint16_t word,
                FX_Charset charset);
  ~UndoBackspace() override;

  // UndoItemIface:
  int Redo() override;
  int Undo() override;

 private:
  UnownedPtr<CPWL_EditImpl> m_pEdit;

  CPVT_WordPlace m_wpOld;
  CPVT_WordPlace m_wpNew;
  uint16_t m_Word;
  FX_Charset m_nCharset;
};

CPWL_EditImpl::UndoBackspace::UndoBackspace(CPWL_EditImpl* pEdit,
                                            const CPVT_WordPlace& wpOldPlace,
                                            const CPVT_WordPlace& wpNewPlace,
                                            uint16_t word,
                                            FX_Charset charset)
    : m_pEdit(pEdit),
      m_wpOld(wpOldPlace),
      m_wpNew(wpNewPlace),
      m_Word(word),
      m_nCharset(charset) {
  DCHECK(m_pEdit);
}

CPWL_EditImpl::UndoBackspace::~UndoBackspace() = default;

int CPWL_EditImpl::UndoBackspace::Redo() {
  m_pEdit->SelectNone();
  m_pEdit->SetCaret(m_wpOld);
  m_pEdit->Backspace(false);
  return 0;
}

int CPWL_EditImpl::UndoBackspace::Undo() {
  m_pEdit->SelectNone();
  m_pEdit->SetCaret(m_wpNew);
  if (m_wpNew.nSecIndex != m_wpOld.nSecIndex)
    m_pEdit->InsertReturn(false);
  else
    m_pEdit->InsertWord(m_Word, m_nCharset, false);
  return 0;
}

class CPWL_EditImpl::UndoDelete final : public CPWL_EditImpl::UndoItemIface {
 public:
  UndoDelete(CPWL_EditImpl* pEdit,
             const CPVT_WordPlace& wpOldPlace,
             const CPVT_WordPlace& wpNewPlace,
             uint16_t word,
             FX_Charset charset,
             bool bSecEnd);
  ~UndoDelete() override;

  // UndoItemIface:
  int Redo() override;
  int Undo() override;

 private:
  UnownedPtr<CPWL_EditImpl> m_pEdit;

  CPVT_WordPlace m_wpOld;
  CPVT_WordPlace m_wpNew;
  uint16_t m_Word;
  FX_Charset m_nCharset;
  bool m_bSecEnd;
};

CPWL_EditImpl::UndoDelete::UndoDelete(CPWL_EditImpl* pEdit,
                                      const CPVT_WordPlace& wpOldPlace,
                                      const CPVT_WordPlace& wpNewPlace,
                                      uint16_t word,
                                      FX_Charset charset,
                                      bool bSecEnd)
    : m_pEdit(pEdit),
      m_wpOld(wpOldPlace),
      m_wpNew(wpNewPlace),
      m_Word(word),
      m_nCharset(charset),
      m_bSecEnd(bSecEnd) {
  DCHECK(m_pEdit);
}

CPWL_EditImpl::UndoDelete::~UndoDelete() = default;

int CPWL_EditImpl::UndoDelete::Redo() {
  m_pEdit->SelectNone();
  m_pEdit->SetCaret(m_wpOld);
  m_pEdit->Delete(false);
  return 0;
}

int CPWL_EditImpl::UndoDelete::Undo() {
  m_pEdit->SelectNone();
  m_pEdit->SetCaret(m_wpNew);
  if (m_bSecEnd)
    m_pEdit->InsertReturn(false);
  else
    m_pEdit->InsertWord(m_Word, m_nCharset, false);
  return 0;
}

class CPWL_EditImpl::UndoClear final : public CPWL_EditImpl::UndoItemIface {
 public:
  UndoClear(CPWL_EditImpl* pEdit,
            const CPVT_WordRange& wrSel,
            const WideString& swText);
  ~UndoClear() override;

  // UndoItemIface:
  int Redo() override;
  int Undo() override;

 private:
  UnownedPtr<CPWL_EditImpl> m_pEdit;

  CPVT_WordRange m_wrSel;
  WideString m_swText;
};

CPWL_EditImpl::UndoClear::UndoClear(CPWL_EditImpl* pEdit,
                                    const CPVT_WordRange& wrSel,
                                    const WideString& swText)
    : m_pEdit(pEdit), m_wrSel(wrSel), m_swText(swText) {
  DCHECK(m_pEdit);
}

CPWL_EditImpl::UndoClear::~UndoClear() = default;

int CPWL_EditImpl::UndoClear::Redo() {
  m_pEdit->SelectNone();
  m_pEdit->SetSelection(m_wrSel.BeginPos, m_wrSel.EndPos);
  m_pEdit->Clear(false);
  return 0;
}

int CPWL_EditImpl::UndoClear::Undo() {
  m_pEdit->SelectNone();
  m_pEdit->SetCaret(m_wrSel.BeginPos);
  m_pEdit->InsertText(m_swText, FX_Charset::kDefault, false);
  m_pEdit->SetSelection(m_wrSel.BeginPos, m_wrSel.EndPos);
  return 0;
}

class CPWL_EditImpl::UndoInsertText final
    : public CPWL_EditImpl::UndoItemIface {
 public:
  UndoInsertText(CPWL_EditImpl* pEdit,
                 const CPVT_WordPlace& wpOldPlace,
                 const CPVT_WordPlace& wpNewPlace,
                 const WideString& swText,
                 FX_Charset charset);
  ~UndoInsertText() override;

  // UndoItemIface:
  int Redo() override;
  int Undo() override;

 private:
  UnownedPtr<CPWL_EditImpl> m_pEdit;

  CPVT_WordPlace m_wpOld;
  CPVT_WordPlace m_wpNew;
  WideString m_swText;
  FX_Charset m_nCharset;
};

CPWL_EditImpl::UndoInsertText::UndoInsertText(CPWL_EditImpl* pEdit,
                                              const CPVT_WordPlace& wpOldPlace,
                                              const CPVT_WordPlace& wpNewPlace,
                                              const WideString& swText,
                                              FX_Charset charset)
    : m_pEdit(pEdit),
      m_wpOld(wpOldPlace),
      m_wpNew(wpNewPlace),
      m_swText(swText),
      m_nCharset(charset) {
  DCHECK(m_pEdit);
}

CPWL_EditImpl::UndoInsertText::~UndoInsertText() = default;

int CPWL_EditImpl::UndoInsertText::Redo() {
  m_pEdit->SelectNone();
  m_pEdit->SetCaret(m_wpOld);
  m_pEdit->InsertText(m_swText, m_nCharset, false);
  return 0;
}

int CPWL_EditImpl::UndoInsertText::Undo() {
  m_pEdit->SelectNone();
  m_pEdit->SetSelection(m_wpOld, m_wpNew);
  m_pEdit->Clear(false);
  return 0;
}

void CPWL_EditImpl::DrawEdit(CFX_RenderDevice* pDevice,
                             const CFX_Matrix& mtUser2Device,
                             FX_COLORREF crTextFill,
                             const CFX_FloatRect& rcClip,
                             const CFX_PointF& ptOffset,
                             const CPVT_WordRange* pRange,
                             IPWL_FillerNotify* pFillerNotify,
                             IPWL_FillerNotify::PerWindowData* pSystemData) {
  const bool bContinuous = GetCharArray() == 0;
  uint16_t SubWord = GetPasswordChar();
  float fFontSize = GetFontSize();
  CPVT_WordRange wrSelect = GetSelectWordRange();
  FX_COLORREF crCurFill = crTextFill;
  FX_COLORREF crOldFill = crCurFill;
  bool bSelect = false;
  const FX_COLORREF crWhite = ArgbEncode(255, 255, 255, 255);
  const FX_COLORREF crSelBK = ArgbEncode(255, 0, 51, 113);

  int32_t nFontIndex = -1;
  CFX_PointF ptBT;
  CFX_RenderDevice::StateRestorer restorer(pDevice);
  if (!rcClip.IsEmpty())
    pDevice->SetClip_Rect(mtUser2Device.TransformRect(rcClip).ToFxRect());

  Iterator* pIterator = GetIterator();
  IPVT_FontMap* pFontMap = GetFontMap();
  if (!pFontMap)
    return;

  if (pRange)
    pIterator->SetAt(pRange->BeginPos);
  else
    pIterator->SetAt(0);

  ByteString sTextBuf;
  CPVT_WordPlace oldplace;
  while (pIterator->NextWord()) {
    CPVT_WordPlace place = pIterator->GetAt();
    if (pRange && place > pRange->EndPos)
      break;

    if (!wrSelect.IsEmpty()) {
      bSelect = place > wrSelect.BeginPos && place <= wrSelect.EndPos;
      crCurFill = bSelect ? crWhite : crTextFill;
    }
    if (pFillerNotify->IsSelectionImplemented()) {
      crCurFill = crTextFill;
      crOldFill = crCurFill;
    }
    CPVT_Word word;
    if (pIterator->GetWord(word)) {
      if (bSelect) {
        CPVT_Line line;
        pIterator->GetLine(line);
        if (pFillerNotify->IsSelectionImplemented()) {
          CFX_FloatRect rc(word.ptWord.x, line.ptLine.y + line.fLineDescent,
                           word.ptWord.x + word.fWidth,
                           line.ptLine.y + line.fLineAscent);
          rc.Intersect(rcClip);
          pFillerNotify->OutputSelectedRect(pSystemData, rc);
        } else {
          CFX_Path pathSelBK;
          pathSelBK.AppendRect(word.ptWord.x, line.ptLine.y + line.fLineDescent,
                               word.ptWord.x + word.fWidth,
                               line.ptLine.y + line.fLineAscent);

          pDevice->DrawPath(pathSelBK, &mtUser2Device, nullptr, crSelBK, 0,
                            CFX_FillRenderOptions::WindingOptions());
        }
      }
      if (bContinuous) {
        if (place.LineCmp(oldplace) != 0 || word.nFontIndex != nFontIndex ||
            crOldFill != crCurFill) {
          if (!sTextBuf.IsEmpty()) {
            DrawTextString(pDevice,
                           CFX_PointF(ptBT.x + ptOffset.x, ptBT.y + ptOffset.y),
                           pFontMap->GetPDFFont(nFontIndex).Get(), fFontSize,
                           mtUser2Device, sTextBuf, crOldFill);
            sTextBuf.clear();
          }
          nFontIndex = word.nFontIndex;
          ptBT = word.ptWord;
          crOldFill = crCurFill;
        }
        sTextBuf += GetPDFWordString(word.nFontIndex, word.Word, SubWord);
      } else {
        DrawTextString(
            pDevice,
            CFX_PointF(word.ptWord.x + ptOffset.x, word.ptWord.y + ptOffset.y),
            pFontMap->GetPDFFont(word.nFontIndex).Get(), fFontSize,
            mtUser2Device,
            GetPDFWordString(word.nFontIndex, word.Word, SubWord), crCurFill);
      }
      oldplace = place;
    }
  }
  if (!sTextBuf.IsEmpty()) {
    DrawTextString(pDevice,
                   CFX_PointF(ptBT.x + ptOffset.x, ptBT.y + ptOffset.y),
                   pFontMap->GetPDFFont(nFontIndex).Get(), fFontSize,
                   mtUser2Device, sTextBuf, crOldFill);
  }
}

CPWL_EditImpl::CPWL_EditImpl()
    : m_pVT(std::make_unique<CPVT_VariableText>(nullptr)) {}

CPWL_EditImpl::~CPWL_EditImpl() = default;

void CPWL_EditImpl::Initialize() {
  m_pVT->Initialize();
  SetCaret(m_pVT->GetBeginWordPlace());
  SetCaretOrigin();
}

void CPWL_EditImpl::SetFontMap(IPVT_FontMap* pFontMap) {
  m_pVTProvider = std::make_unique<Provider>(pFontMap);
  m_pVT->SetProvider(m_pVTProvider.get());
}

void CPWL_EditImpl::SetNotify(CPWL_Edit* pNotify) {
  m_pNotify = pNotify;
}

CPWL_EditImpl::Iterator* CPWL_EditImpl::GetIterator() {
  if (!m_pIterator)
    m_pIterator = std::make_unique<Iterator>(this, m_pVT->GetIterator());
  return m_pIterator.get();
}

IPVT_FontMap* CPWL_EditImpl::GetFontMap() {
  return m_pVTProvider ? m_pVTProvider->GetFontMap() : nullptr;
}

void CPWL_EditImpl::SetPlateRect(const CFX_FloatRect& rect) {
  m_pVT->SetPlateRect(rect);
  m_ptScrollPos = CFX_PointF(rect.left, rect.top);
}

void CPWL_EditImpl::SetAlignmentH(int32_t nFormat) {
  m_pVT->SetAlignment(nFormat);
}

void CPWL_EditImpl::SetAlignmentV(int32_t nFormat) {
  m_nAlignment = nFormat;
}

void CPWL_EditImpl::SetPasswordChar(uint16_t wSubWord) {
  m_pVT->SetPasswordChar(wSubWord);
}

void CPWL_EditImpl::SetLimitChar(int32_t nLimitChar) {
  m_pVT->SetLimitChar(nLimitChar);
}

void CPWL_EditImpl::SetCharArray(int32_t nCharArray) {
  m_pVT->SetCharArray(nCharArray);
}

void CPWL_EditImpl::SetMultiLine(bool bMultiLine) {
  m_pVT->SetMultiLine(bMultiLine);
}

void CPWL_EditImpl::SetAutoReturn(bool bAuto) {
  m_pVT->SetAutoReturn(bAuto);
}

void CPWL_EditImpl::SetAutoFontSize(bool bAuto) {
  m_pVT->SetAutoFontSize(bAuto);
}

void CPWL_EditImpl::SetFontSize(float fFontSize) {
  m_pVT->SetFontSize(fFontSize);
}

void CPWL_EditImpl::SetAutoScroll(bool bAuto) {
  m_bEnableScroll = bAuto;
}

void CPWL_EditImpl::SetTextOverflow(bool bAllowed) {
  m_bEnableOverflow = bAllowed;
}

void CPWL_EditImpl::SetSelection(int32_t nStartChar, int32_t nEndChar) {
  if (m_pVT->IsValid()) {
    if (nStartChar == 0 && nEndChar < 0) {
      SelectAll();
    } else if (nStartChar < 0) {
      SelectNone();
    } else {
      if (nStartChar < nEndChar) {
        SetSelection(m_pVT->WordIndexToWordPlace(nStartChar),
                     m_pVT->WordIndexToWordPlace(nEndChar));
      } else {
        SetSelection(m_pVT->WordIndexToWordPlace(nEndChar),
                     m_pVT->WordIndexToWordPlace(nStartChar));
      }
    }
  }
}

void CPWL_EditImpl::SetSelection(const CPVT_WordPlace& begin,
                                 const CPVT_WordPlace& end) {
  if (!m_pVT->IsValid())
    return;

  SelectNone();
  m_SelState.Set(begin, end);
  SetCaret(m_SelState.EndPos);
  ScrollToCaret();
  if (!m_SelState.IsEmpty())
    Refresh();
  SetCaretInfo();
}

std::pair<int32_t, int32_t> CPWL_EditImpl::GetSelection() const {
  if (!m_pVT->IsValid())
    return std::make_pair(-1, -1);

  if (m_SelState.IsEmpty()) {
    return std::make_pair(m_pVT->WordPlaceToWordIndex(m_wpCaret),
                          m_pVT->WordPlaceToWordIndex(m_wpCaret));
  }
  if (m_SelState.BeginPos < m_SelState.EndPos) {
    return std::make_pair(m_pVT->WordPlaceToWordIndex(m_SelState.BeginPos),
                          m_pVT->WordPlaceToWordIndex(m_SelState.EndPos));
  }
  return std::make_pair(m_pVT->WordPlaceToWordIndex(m_SelState.EndPos),
                        m_pVT->WordPlaceToWordIndex(m_SelState.BeginPos));
}

int32_t CPWL_EditImpl::GetCaret() const {
  if (m_pVT->IsValid())
    return m_pVT->WordPlaceToWordIndex(m_wpCaret);

  return -1;
}

CPVT_WordPlace CPWL_EditImpl::GetCaretWordPlace() const {
  return m_wpCaret;
}

WideString CPWL_EditImpl::GetText() const {
  WideString swRet;
  if (!m_pVT->IsValid())
    return swRet;

  CPVT_VariableText::Iterator* pIterator = m_pVT->GetIterator();
  pIterator->SetAt(0);

  CPVT_Word wordinfo;
  CPVT_WordPlace oldplace = pIterator->GetWordPlace();
  while (pIterator->NextWord()) {
    CPVT_WordPlace place = pIterator->GetWordPlace();
    if (pIterator->GetWord(wordinfo))
      swRet += wordinfo.Word;
    if (oldplace.nSecIndex != place.nSecIndex)
      swRet += L"\r\n";
    oldplace = place;
  }
  return swRet;
}

WideString CPWL_EditImpl::GetRangeText(const CPVT_WordRange& range) const {
  WideString swRet;
  if (!m_pVT->IsValid())
    return swRet;

  CPVT_VariableText::Iterator* pIterator = m_pVT->GetIterator();
  CPVT_WordRange wrTemp = range;
  m_pVT->UpdateWordPlace(wrTemp.BeginPos);
  m_pVT->UpdateWordPlace(wrTemp.EndPos);
  pIterator->SetAt(wrTemp.BeginPos);

  CPVT_Word wordinfo;
  CPVT_WordPlace oldplace = wrTemp.BeginPos;
  while (pIterator->NextWord()) {
    CPVT_WordPlace place = pIterator->GetWordPlace();
    if (place > wrTemp.EndPos)
      break;
    if (pIterator->GetWord(wordinfo))
      swRet += wordinfo.Word;
    if (oldplace.nSecIndex != place.nSecIndex)
      swRet += L"\r\n";
    oldplace = place;
  }
  return swRet;
}

WideString CPWL_EditImpl::GetSelectedText() const {
  return GetRangeText(m_SelState.ConvertToWordRange());
}

int32_t CPWL_EditImpl::GetTotalLines() const {
  int32_t nLines = 1;

  CPVT_VariableText::Iterator* pIterator = m_pVT->GetIterator();
  pIterator->SetAt(0);
  while (pIterator->NextLine())
    ++nLines;

  return nLines;
}

CPVT_WordRange CPWL_EditImpl::GetSelectWordRange() const {
  return m_SelState.ConvertToWordRange();
}

void CPWL_EditImpl::SetText(const WideString& sText) {
  Clear();
  DoInsertText(CPVT_WordPlace(0, 0, -1), sText, FX_Charset::kDefault);
}

bool CPWL_EditImpl::InsertWord(uint16_t word, FX_Charset charset) {
  return InsertWord(word, charset, true);
}

bool CPWL_EditImpl::InsertReturn() {
  return InsertReturn(true);
}

bool CPWL_EditImpl::Backspace() {
  return Backspace(true);
}

bool CPWL_EditImpl::Delete() {
  return Delete(true);
}

bool CPWL_EditImpl::ClearSelection() {
  return Clear(true);
}

bool CPWL_EditImpl::InsertText(const WideString& sText, FX_Charset charset) {
  return InsertText(sText, charset, true);
}

float CPWL_EditImpl::GetFontSize() const {
  return m_pVT->GetFontSize();
}

uint16_t CPWL_EditImpl::GetPasswordChar() const {
  return m_pVT->GetPasswordChar();
}

int32_t CPWL_EditImpl::GetCharArray() const {
  return m_pVT->GetCharArray();
}

CFX_FloatRect CPWL_EditImpl::GetContentRect() const {
  return VTToEdit(m_pVT->GetContentRect());
}

CPVT_WordRange CPWL_EditImpl::GetWholeWordRange() const {
  if (m_pVT->IsValid())
    return CPVT_WordRange(m_pVT->GetBeginWordPlace(), m_pVT->GetEndWordPlace());

  return CPVT_WordRange();
}

CPVT_WordRange CPWL_EditImpl::GetVisibleWordRange() const {
  if (m_bEnableOverflow)
    return GetWholeWordRange();

  if (m_pVT->IsValid()) {
    CFX_FloatRect rcPlate = m_pVT->GetPlateRect();

    CPVT_WordPlace place1 =
        m_pVT->SearchWordPlace(EditToVT(CFX_PointF(rcPlate.left, rcPlate.top)));
    CPVT_WordPlace place2 = m_pVT->SearchWordPlace(
        EditToVT(CFX_PointF(rcPlate.right, rcPlate.bottom)));

    return CPVT_WordRange(place1, place2);
  }

  return CPVT_WordRange();
}

CPVT_WordPlace CPWL_EditImpl::SearchWordPlace(const CFX_PointF& point) const {
  if (m_pVT->IsValid()) {
    return m_pVT->SearchWordPlace(EditToVT(point));
  }

  return CPVT_WordPlace();
}

void CPWL_EditImpl::Paint() {
  if (m_pVT->IsValid()) {
    RearrangeAll();
    ScrollToCaret();
    Refresh();
    SetCaretOrigin();
    SetCaretInfo();
  }
}

void CPWL_EditImpl::RearrangeAll() {
  if (m_pVT->IsValid()) {
    m_pVT->UpdateWordPlace(m_wpCaret);
    m_pVT->RearrangeAll();
    m_pVT->UpdateWordPlace(m_wpCaret);
    SetScrollInfo();
    SetContentChanged();
  }
}

void CPWL_EditImpl::RearrangePart(const CPVT_WordRange& range) {
  if (m_pVT->IsValid()) {
    m_pVT->UpdateWordPlace(m_wpCaret);
    m_pVT->RearrangePart(range);
    m_pVT->UpdateWordPlace(m_wpCaret);
    SetScrollInfo();
    SetContentChanged();
  }
}

void CPWL_EditImpl::SetContentChanged() {
  if (m_pNotify) {
    CFX_FloatRect rcContent = m_pVT->GetContentRect();
    if (rcContent.Width() != m_rcOldContent.Width() ||
        rcContent.Height() != m_rcOldContent.Height()) {
      m_rcOldContent = rcContent;
    }
  }
}

void CPWL_EditImpl::SelectAll() {
  if (!m_pVT->IsValid())
    return;
  m_SelState = SelectState(GetWholeWordRange());
  SetCaret(m_SelState.EndPos);
  ScrollToCaret();
  Refresh();
  SetCaretInfo();
}

void CPWL_EditImpl::SelectNone() {
  if (!m_pVT->IsValid() || m_SelState.IsEmpty())
    return;

  m_SelState.Reset();
  Refresh();
}

bool CPWL_EditImpl::IsSelected() const {
  return !m_SelState.IsEmpty();
}

CFX_PointF CPWL_EditImpl::VTToEdit(const CFX_PointF& point) const {
  CFX_FloatRect rcContent = m_pVT->GetContentRect();
  CFX_FloatRect rcPlate = m_pVT->GetPlateRect();

  float fPadding = 0.0f;

  switch (m_nAlignment) {
    case 0:
      fPadding = 0.0f;
      break;
    case 1:
      fPadding = (rcPlate.Height() - rcContent.Height()) * 0.5f;
      break;
    case 2:
      fPadding = rcPlate.Height() - rcContent.Height();
      break;
  }

  return CFX_PointF(point.x - (m_ptScrollPos.x - rcPlate.left),
                    point.y - (m_ptScrollPos.y + fPadding - rcPlate.top));
}

CFX_PointF CPWL_EditImpl::EditToVT(const CFX_PointF& point) const {
  CFX_FloatRect rcContent = m_pVT->GetContentRect();
  CFX_FloatRect rcPlate = m_pVT->GetPlateRect();

  float fPadding = 0.0f;

  switch (m_nAlignment) {
    case 0:
      fPadding = 0.0f;
      break;
    case 1:
      fPadding = (rcPlate.Height() - rcContent.Height()) * 0.5f;
      break;
    case 2:
      fPadding = rcPlate.Height() - rcContent.Height();
      break;
  }

  return CFX_PointF(point.x + (m_ptScrollPos.x - rcPlate.left),
                    point.y + (m_ptScrollPos.y + fPadding - rcPlate.top));
}

CFX_FloatRect CPWL_EditImpl::VTToEdit(const CFX_FloatRect& rect) const {
  CFX_PointF ptLeftBottom = VTToEdit(CFX_PointF(rect.left, rect.bottom));
  CFX_PointF ptRightTop = VTToEdit(CFX_PointF(rect.right, rect.top));

  return CFX_FloatRect(ptLeftBottom.x, ptLeftBottom.y, ptRightTop.x,
                       ptRightTop.y);
}

void CPWL_EditImpl::SetScrollInfo() {
  if (!m_pNotify)
    return;

  CFX_FloatRect rcPlate = m_pVT->GetPlateRect();
  CFX_FloatRect rcContent = m_pVT->GetContentRect();
  if (m_bNotifyFlag)
    return;

  AutoRestorer<bool> restorer(&m_bNotifyFlag);
  m_bNotifyFlag = true;

  PWL_SCROLL_INFO Info;
  Info.fPlateWidth = rcPlate.top - rcPlate.bottom;
  Info.fContentMin = rcContent.bottom;
  Info.fContentMax = rcContent.top;
  Info.fSmallStep = rcPlate.Height() / 3;
  Info.fBigStep = rcPlate.Height();
  m_pNotify->SetScrollInfo(Info);
}

void CPWL_EditImpl::SetScrollPosX(float fx) {
  if (!m_bEnableScroll)
    return;

  if (m_pVT->IsValid()) {
    if (!FXSYS_IsFloatEqual(m_ptScrollPos.x, fx)) {
      m_ptScrollPos.x = fx;
      Refresh();
    }
  }
}

void CPWL_EditImpl::SetScrollPosY(float fy) {
  if (!m_bEnableScroll)
    return;

  if (m_pVT->IsValid()) {
    if (!FXSYS_IsFloatEqual(m_ptScrollPos.y, fy)) {
      m_ptScrollPos.y = fy;
      Refresh();

      if (m_pNotify) {
        if (!m_bNotifyFlag) {
          AutoRestorer<bool> restorer(&m_bNotifyFlag);
          m_bNotifyFlag = true;
          m_pNotify->SetScrollPosition(fy);
        }
      }
    }
  }
}

void CPWL_EditImpl::SetScrollPos(const CFX_PointF& point) {
  SetScrollPosX(point.x);
  SetScrollPosY(point.y);
  SetScrollLimit();
  SetCaretInfo();
}

CFX_PointF CPWL_EditImpl::GetScrollPos() const {
  return m_ptScrollPos;
}

void CPWL_EditImpl::SetScrollLimit() {
  if (m_pVT->IsValid()) {
    CFX_FloatRect rcContent = m_pVT->GetContentRect();
    CFX_FloatRect rcPlate = m_pVT->GetPlateRect();

    if (rcPlate.Width() > rcContent.Width()) {
      SetScrollPosX(rcPlate.left);
    } else {
      if (FXSYS_IsFloatSmaller(m_ptScrollPos.x, rcContent.left)) {
        SetScrollPosX(rcContent.left);
      } else if (FXSYS_IsFloatBigger(m_ptScrollPos.x,
                                     rcContent.right - rcPlate.Width())) {
        SetScrollPosX(rcContent.right - rcPlate.Width());
      }
    }

    if (rcPlate.Height() > rcContent.Height()) {
      SetScrollPosY(rcPlate.top);
    } else {
      if (FXSYS_IsFloatSmaller(m_ptScrollPos.y,
                               rcContent.bottom + rcPlate.Height())) {
        SetScrollPosY(rcContent.bottom + rcPlate.Height());
      } else if (FXSYS_IsFloatBigger(m_ptScrollPos.y, rcContent.top)) {
        SetScrollPosY(rcContent.top);
      }
    }
  }
}

void CPWL_EditImpl::ScrollToCaret() {
  SetScrollLimit();

  if (!m_pVT->IsValid())
    return;

  CPVT_VariableText::Iterator* pIterator = m_pVT->GetIterator();
  pIterator->SetAt(m_wpCaret);

  CFX_PointF ptHead;
  CFX_PointF ptFoot;
  CPVT_Word word;
  CPVT_Line line;
  if (pIterator->GetWord(word)) {
    ptHead.x = word.ptWord.x + word.fWidth;
    ptHead.y = word.ptWord.y + word.fAscent;
    ptFoot.x = word.ptWord.x + word.fWidth;
    ptFoot.y = word.ptWord.y + word.fDescent;
  } else if (pIterator->GetLine(line)) {
    ptHead.x = line.ptLine.x;
    ptHead.y = line.ptLine.y + line.fLineAscent;
    ptFoot.x = line.ptLine.x;
    ptFoot.y = line.ptLine.y + line.fLineDescent;
  }

  CFX_PointF ptHeadEdit = VTToEdit(ptHead);
  CFX_PointF ptFootEdit = VTToEdit(ptFoot);
  CFX_FloatRect rcPlate = m_pVT->GetPlateRect();
  if (!FXSYS_IsFloatEqual(rcPlate.left, rcPlate.right)) {
    if (FXSYS_IsFloatSmaller(ptHeadEdit.x, rcPlate.left) ||
        FXSYS_IsFloatEqual(ptHeadEdit.x, rcPlate.left)) {
      SetScrollPosX(ptHead.x);
    } else if (FXSYS_IsFloatBigger(ptHeadEdit.x, rcPlate.right)) {
      SetScrollPosX(ptHead.x - rcPlate.Width());
    }
  }

  if (!FXSYS_IsFloatEqual(rcPlate.top, rcPlate.bottom)) {
    if (FXSYS_IsFloatSmaller(ptFootEdit.y, rcPlate.bottom) ||
        FXSYS_IsFloatEqual(ptFootEdit.y, rcPlate.bottom)) {
      if (FXSYS_IsFloatSmaller(ptHeadEdit.y, rcPlate.top)) {
        SetScrollPosY(ptFoot.y + rcPlate.Height());
      }
    } else if (FXSYS_IsFloatBigger(ptHeadEdit.y, rcPlate.top)) {
      if (FXSYS_IsFloatBigger(ptFootEdit.y, rcPlate.bottom)) {
        SetScrollPosY(ptHead.y);
      }
    }
  }
}

void CPWL_EditImpl::Refresh() {
  if (m_bEnableRefresh && m_pVT->IsValid()) {
    m_Refresh.BeginRefresh();
    RefreshPushLineRects(GetVisibleWordRange());

    m_Refresh.NoAnalyse();
    m_ptRefreshScrollPos = m_ptScrollPos;

    if (m_pNotify) {
      if (!m_bNotifyFlag) {
        AutoRestorer<bool> restorer(&m_bNotifyFlag);
        m_bNotifyFlag = true;
        std::vector<CFX_FloatRect>* pRects = m_Refresh.GetRefreshRects();
        for (auto& rect : *pRects) {
          if (!m_pNotify->InvalidateRect(&rect)) {
            m_pNotify = nullptr;  // Gone, dangling even.
            break;
          }
        }
      }
    }

    m_Refresh.EndRefresh();
  }
}

void CPWL_EditImpl::RefreshPushLineRects(const CPVT_WordRange& wr) {
  if (!m_pVT->IsValid())
    return;

  CPVT_VariableText::Iterator* pIterator = m_pVT->GetIterator();
  CPVT_WordPlace wpBegin = wr.BeginPos;
  m_pVT->UpdateWordPlace(wpBegin);
  CPVT_WordPlace wpEnd = wr.EndPos;
  m_pVT->UpdateWordPlace(wpEnd);
  pIterator->SetAt(wpBegin);

  CPVT_Line lineinfo;
  do {
    if (!pIterator->GetLine(lineinfo))
      break;
    if (lineinfo.lineplace.LineCmp(wpEnd) > 0)
      break;

    CFX_FloatRect rcLine(lineinfo.ptLine.x,
                         lineinfo.ptLine.y + lineinfo.fLineDescent,
                         lineinfo.ptLine.x + lineinfo.fLineWidth,
                         lineinfo.ptLine.y + lineinfo.fLineAscent);

    m_Refresh.Push(CPVT_WordRange(lineinfo.lineplace, lineinfo.lineEnd),
                   VTToEdit(rcLine));
  } while (pIterator->NextLine());
}

void CPWL_EditImpl::RefreshWordRange(const CPVT_WordRange& wr) {
  CPVT_VariableText::Iterator* pIterator = m_pVT->GetIterator();
  CPVT_WordRange wrTemp = wr;

  m_pVT->UpdateWordPlace(wrTemp.BeginPos);
  m_pVT->UpdateWordPlace(wrTemp.EndPos);
  pIterator->SetAt(wrTemp.BeginPos);

  CPVT_Word wordinfo;
  CPVT_Line lineinfo;
  CPVT_WordPlace place;

  while (pIterator->NextWord()) {
    place = pIterator->GetWordPlace();
    if (place > wrTemp.EndPos)
      break;

    pIterator->GetWord(wordinfo);
    pIterator->GetLine(lineinfo);
    if (place.LineCmp(wrTemp.BeginPos) == 0 ||
        place.LineCmp(wrTemp.EndPos) == 0) {
      CFX_FloatRect rcWord(wordinfo.ptWord.x,
                           lineinfo.ptLine.y + lineinfo.fLineDescent,
                           wordinfo.ptWord.x + wordinfo.fWidth,
                           lineinfo.ptLine.y + lineinfo.fLineAscent);

      if (m_pNotify) {
        if (!m_bNotifyFlag) {
          AutoRestorer<bool> restorer(&m_bNotifyFlag);
          m_bNotifyFlag = true;
          CFX_FloatRect rcRefresh = VTToEdit(rcWord);
          if (!m_pNotify->InvalidateRect(&rcRefresh)) {
            m_pNotify = nullptr;  // Gone, dangling even.
          }
        }
      }
    } else {
      CFX_FloatRect rcLine(lineinfo.ptLine.x,
                           lineinfo.ptLine.y + lineinfo.fLineDescent,
                           lineinfo.ptLine.x + lineinfo.fLineWidth,
                           lineinfo.ptLine.y + lineinfo.fLineAscent);

      if (m_pNotify) {
        if (!m_bNotifyFlag) {
          AutoRestorer<bool> restorer(&m_bNotifyFlag);
          m_bNotifyFlag = true;
          CFX_FloatRect rcRefresh = VTToEdit(rcLine);
          if (!m_pNotify->InvalidateRect(&rcRefresh)) {
            m_pNotify = nullptr;  // Gone, dangling even.
          }
        }
      }

      pIterator->NextLine();
    }
  }
}

void CPWL_EditImpl::SetCaret(const CPVT_WordPlace& place) {
  m_wpOldCaret = m_wpCaret;
  m_wpCaret = place;
}

void CPWL_EditImpl::SetCaretInfo() {
  if (m_pNotify) {
    if (!m_bNotifyFlag) {
      CPVT_VariableText::Iterator* pIterator = m_pVT->GetIterator();
      pIterator->SetAt(m_wpCaret);

      CFX_PointF ptHead;
      CFX_PointF ptFoot;
      CPVT_Word word;
      CPVT_Line line;
      if (pIterator->GetWord(word)) {
        ptHead.x = word.ptWord.x + word.fWidth;
        ptHead.y = word.ptWord.y + word.fAscent;
        ptFoot.x = word.ptWord.x + word.fWidth;
        ptFoot.y = word.ptWord.y + word.fDescent;
      } else if (pIterator->GetLine(line)) {
        ptHead.x = line.ptLine.x;
        ptHead.y = line.ptLine.y + line.fLineAscent;
        ptFoot.x = line.ptLine.x;
        ptFoot.y = line.ptLine.y + line.fLineDescent;
      }

      AutoRestorer<bool> restorer(&m_bNotifyFlag);
      m_bNotifyFlag = true;
      m_pNotify->SetCaret(m_SelState.IsEmpty(), VTToEdit(ptHead),
                          VTToEdit(ptFoot));
    }
  }
}

void CPWL_EditImpl::OnMouseDown(const CFX_PointF& point,
                                bool bShift,
                                bool bCtrl) {
  if (!m_pVT->IsValid())
    return;

  SelectNone();
  SetCaret(m_pVT->SearchWordPlace(EditToVT(point)));
  m_SelState.Set(m_wpCaret, m_wpCaret);
  ScrollToCaret();
  SetCaretOrigin();
  SetCaretInfo();
}

void CPWL_EditImpl::OnMouseMove(const CFX_PointF& point,
                                bool bShift,
                                bool bCtrl) {
  if (!m_pVT->IsValid())
    return;

  SetCaret(m_pVT->SearchWordPlace(EditToVT(point)));
  if (m_wpCaret == m_wpOldCaret)
    return;

  m_SelState.SetEndPos(m_wpCaret);
  ScrollToCaret();
  Refresh();
  SetCaretOrigin();
  SetCaretInfo();
}

void CPWL_EditImpl::OnVK_UP(bool bShift) {
  if (!m_pVT->IsValid())
    return;

  SetCaret(m_pVT->GetUpWordPlace(m_wpCaret, m_ptCaret));
  if (bShift) {
    if (m_SelState.IsEmpty())
      m_SelState.Set(m_wpOldCaret, m_wpCaret);
    else
      m_SelState.SetEndPos(m_wpCaret);

    if (m_wpOldCaret != m_wpCaret) {
      ScrollToCaret();
      Refresh();
      SetCaretInfo();
    }
  } else {
    SelectNone();
    ScrollToCaret();
    SetCaretInfo();
  }
}

void CPWL_EditImpl::OnVK_DOWN(bool bShift) {
  if (!m_pVT->IsValid())
    return;

  SetCaret(m_pVT->GetDownWordPlace(m_wpCaret, m_ptCaret));
  if (bShift) {
    if (m_SelState.IsEmpty())
      m_SelState.Set(m_wpOldCaret, m_wpCaret);
    else
      m_SelState.SetEndPos(m_wpCaret);

    if (m_wpOldCaret != m_wpCaret) {
      ScrollToCaret();
      Refresh();
      SetCaretInfo();
    }
  } else {
    SelectNone();
    ScrollToCaret();
    SetCaretInfo();
  }
}

void CPWL_EditImpl::OnVK_LEFT(bool bShift) {
  if (!m_pVT->IsValid())
    return;

  if (bShift) {
    if (m_wpCaret == m_pVT->GetLineBeginPlace(m_wpCaret) &&
        m_wpCaret != m_pVT->GetSectionBeginPlace(m_wpCaret)) {
      SetCaret(m_pVT->GetPrevWordPlace(m_wpCaret));
    }
    SetCaret(m_pVT->GetPrevWordPlace(m_wpCaret));
    if (m_SelState.IsEmpty())
      m_SelState.Set(m_wpOldCaret, m_wpCaret);
    else
      m_SelState.SetEndPos(m_wpCaret);

    if (m_wpOldCaret != m_wpCaret) {
      ScrollToCaret();
      Refresh();
      SetCaretInfo();
    }
  } else {
    if (!m_SelState.IsEmpty()) {
      if (m_SelState.BeginPos < m_SelState.EndPos)
        SetCaret(m_SelState.BeginPos);
      else
        SetCaret(m_SelState.EndPos);

      SelectNone();
      ScrollToCaret();
      SetCaretInfo();
    } else {
      if (m_wpCaret == m_pVT->GetLineBeginPlace(m_wpCaret) &&
          m_wpCaret != m_pVT->GetSectionBeginPlace(m_wpCaret)) {
        SetCaret(m_pVT->GetPrevWordPlace(m_wpCaret));
      }
      SetCaret(m_pVT->GetPrevWordPlace(m_wpCaret));
      ScrollToCaret();
      SetCaretOrigin();
      SetCaretInfo();
    }
  }
}

void CPWL_EditImpl::OnVK_RIGHT(bool bShift) {
  if (!m_pVT->IsValid())
    return;

  if (bShift) {
    SetCaret(m_pVT->GetNextWordPlace(m_wpCaret));
    if (m_wpCaret == m_pVT->GetLineEndPlace(m_wpCaret) &&
        m_wpCaret != m_pVT->GetSectionEndPlace(m_wpCaret))
      SetCaret(m_pVT->GetNextWordPlace(m_wpCaret));

    if (m_SelState.IsEmpty())
      m_SelState.Set(m_wpOldCaret, m_wpCaret);
    else
      m_SelState.SetEndPos(m_wpCaret);

    if (m_wpOldCaret != m_wpCaret) {
      ScrollToCaret();
      Refresh();
      SetCaretInfo();
    }
  } else {
    if (!m_SelState.IsEmpty()) {
      if (m_SelState.BeginPos > m_SelState.EndPos)
        SetCaret(m_SelState.BeginPos);
      else
        SetCaret(m_SelState.EndPos);

      SelectNone();
      ScrollToCaret();
      SetCaretInfo();
    } else {
      SetCaret(m_pVT->GetNextWordPlace(m_wpCaret));
      if (m_wpCaret == m_pVT->GetLineEndPlace(m_wpCaret) &&
          m_wpCaret != m_pVT->GetSectionEndPlace(m_wpCaret)) {
        SetCaret(m_pVT->GetNextWordPlace(m_wpCaret));
      }
      ScrollToCaret();
      SetCaretOrigin();
      SetCaretInfo();
    }
  }
}

void CPWL_EditImpl::OnVK_HOME(bool bShift, bool bCtrl) {
  if (!m_pVT->IsValid())
    return;

  if (bShift) {
    if (bCtrl)
      SetCaret(m_pVT->GetBeginWordPlace());
    else
      SetCaret(m_pVT->GetLineBeginPlace(m_wpCaret));

    if (m_SelState.IsEmpty())
      m_SelState.Set(m_wpOldCaret, m_wpCaret);
    else
      m_SelState.SetEndPos(m_wpCaret);

    ScrollToCaret();
    Refresh();
    SetCaretInfo();
  } else {
    if (!m_SelState.IsEmpty()) {
      SetCaret(std::min(m_SelState.BeginPos, m_SelState.EndPos));
      SelectNone();
      ScrollToCaret();
      SetCaretInfo();
    } else {
      if (bCtrl)
        SetCaret(m_pVT->GetBeginWordPlace());
      else
        SetCaret(m_pVT->GetLineBeginPlace(m_wpCaret));

      ScrollToCaret();
      SetCaretOrigin();
      SetCaretInfo();
    }
  }
}

void CPWL_EditImpl::OnVK_END(bool bShift, bool bCtrl) {
  if (!m_pVT->IsValid())
    return;

  if (bShift) {
    if (bCtrl)
      SetCaret(m_pVT->GetEndWordPlace());
    else
      SetCaret(m_pVT->GetLineEndPlace(m_wpCaret));

    if (m_SelState.IsEmpty())
      m_SelState.Set(m_wpOldCaret, m_wpCaret);
    else
      m_SelState.SetEndPos(m_wpCaret);

    ScrollToCaret();
    Refresh();
    SetCaretInfo();
  } else {
    if (!m_SelState.IsEmpty()) {
      SetCaret(std::max(m_SelState.BeginPos, m_SelState.EndPos));
      SelectNone();
      ScrollToCaret();
      SetCaretInfo();
    } else {
      if (bCtrl)
        SetCaret(m_pVT->GetEndWordPlace());
      else
        SetCaret(m_pVT->GetLineEndPlace(m_wpCaret));

      ScrollToCaret();
      SetCaretOrigin();
      SetCaretInfo();
    }
  }
}

bool CPWL_EditImpl::InsertWord(uint16_t word,
                               FX_Charset charset,
                               bool bAddUndo) {
  if (IsTextOverflow() || !m_pVT->IsValid())
    return false;

  m_pVT->UpdateWordPlace(m_wpCaret);
  SetCaret(
      m_pVT->InsertWord(m_wpCaret, word, GetCharSetFromUnicode(word, charset)));
  m_SelState.Set(m_wpCaret, m_wpCaret);
  if (m_wpCaret == m_wpOldCaret)
    return false;

  if (bAddUndo && m_bEnableUndo) {
    AddEditUndoItem(std::make_unique<UndoInsertWord>(this, m_wpOldCaret,
                                                     m_wpCaret, word, charset));
  }
  PaintInsertText(m_wpOldCaret, m_wpCaret);
  return true;
}

bool CPWL_EditImpl::InsertReturn(bool bAddUndo) {
  if (IsTextOverflow() || !m_pVT->IsValid())
    return false;

  m_pVT->UpdateWordPlace(m_wpCaret);
  SetCaret(m_pVT->InsertSection(m_wpCaret));
  m_SelState.Set(m_wpCaret, m_wpCaret);
  if (m_wpCaret == m_wpOldCaret)
    return false;

  if (bAddUndo && m_bEnableUndo) {
    AddEditUndoItem(
        std::make_unique<UndoInsertReturn>(this, m_wpOldCaret, m_wpCaret));
  }
  RearrangePart(CPVT_WordRange(m_wpOldCaret, m_wpCaret));
  ScrollToCaret();
  Refresh();
  SetCaretOrigin();
  SetCaretInfo();
  return true;
}

bool CPWL_EditImpl::Backspace(bool bAddUndo) {
  if (!m_pVT->IsValid() || m_wpCaret == m_pVT->GetBeginWordPlace())
    return false;

  CPVT_Word word;
  if (bAddUndo) {
    CPVT_VariableText::Iterator* pIterator = m_pVT->GetIterator();
    pIterator->SetAt(m_wpCaret);
    pIterator->GetWord(word);
  }
  m_pVT->UpdateWordPlace(m_wpCaret);
  SetCaret(m_pVT->BackSpaceWord(m_wpCaret));
  m_SelState.Set(m_wpCaret, m_wpCaret);
  if (m_wpCaret == m_wpOldCaret)
    return false;

  if (bAddUndo && m_bEnableUndo) {
    AddEditUndoItem(std::make_unique<UndoBackspace>(
        this, m_wpOldCaret, m_wpCaret, word.Word, word.nCharset));
  }
  RearrangePart(CPVT_WordRange(m_wpCaret, m_wpOldCaret));
  ScrollToCaret();
  Refresh();
  SetCaretOrigin();
  SetCaretInfo();
  return true;
}

bool CPWL_EditImpl::Delete(bool bAddUndo) {
  if (!m_pVT->IsValid() || m_wpCaret == m_pVT->GetEndWordPlace())
    return false;

  CPVT_Word word;
  if (bAddUndo) {
    CPVT_VariableText::Iterator* pIterator = m_pVT->GetIterator();
    pIterator->SetAt(m_pVT->GetNextWordPlace(m_wpCaret));
    pIterator->GetWord(word);
  }
  m_pVT->UpdateWordPlace(m_wpCaret);
  bool bSecEnd = (m_wpCaret == m_pVT->GetSectionEndPlace(m_wpCaret));
  SetCaret(m_pVT->DeleteWord(m_wpCaret));
  m_SelState.Set(m_wpCaret, m_wpCaret);
  if (bAddUndo && m_bEnableUndo) {
    if (bSecEnd) {
      AddEditUndoItem(std::make_unique<UndoDelete>(
          this, m_wpOldCaret, m_wpCaret, word.Word, word.nCharset, bSecEnd));
    } else {
      AddEditUndoItem(std::make_unique<UndoDelete>(
          this, m_wpOldCaret, m_wpCaret, word.Word, word.nCharset, bSecEnd));
    }
  }
  RearrangePart(CPVT_WordRange(m_wpOldCaret, m_wpCaret));
  ScrollToCaret();
  Refresh();
  SetCaretOrigin();
  SetCaretInfo();
  return true;
}

bool CPWL_EditImpl::Clear() {
  if (m_pVT->IsValid()) {
    m_pVT->DeleteWords(GetWholeWordRange());
    SetCaret(m_pVT->GetBeginWordPlace());

    return true;
  }

  return false;
}

bool CPWL_EditImpl::Clear(bool bAddUndo) {
  if (!m_pVT->IsValid() || m_SelState.IsEmpty())
    return false;

  CPVT_WordRange range = m_SelState.ConvertToWordRange();
  if (bAddUndo && m_bEnableUndo) {
    AddEditUndoItem(
        std::make_unique<UndoClear>(this, range, GetSelectedText()));
  }
  SelectNone();
  SetCaret(m_pVT->DeleteWords(range));
  m_SelState.Set(m_wpCaret, m_wpCaret);
  RearrangePart(range);
  ScrollToCaret();
  Refresh();
  SetCaretOrigin();
  SetCaretInfo();
  return true;
}

bool CPWL_EditImpl::InsertText(const WideString& sText,
                               FX_Charset charset,
                               bool bAddUndo) {
  if (IsTextOverflow())
    return false;

  m_pVT->UpdateWordPlace(m_wpCaret);
  SetCaret(DoInsertText(m_wpCaret, sText, charset));
  m_SelState.Set(m_wpCaret, m_wpCaret);
  if (m_wpCaret == m_wpOldCaret)
    return false;

  if (bAddUndo && m_bEnableUndo) {
    AddEditUndoItem(std::make_unique<UndoInsertText>(
        this, m_wpOldCaret, m_wpCaret, sText, charset));
  }
  PaintInsertText(m_wpOldCaret, m_wpCaret);
  return true;
}

void CPWL_EditImpl::PaintInsertText(const CPVT_WordPlace& wpOld,
                                    const CPVT_WordPlace& wpNew) {
  if (m_pVT->IsValid()) {
    RearrangePart(CPVT_WordRange(wpOld, wpNew));
    ScrollToCaret();
    Refresh();
    SetCaretOrigin();
    SetCaretInfo();
  }
}

void CPWL_EditImpl::ReplaceAndKeepSelection(const WideString& text) {
  AddEditUndoItem(std::make_unique<UndoReplaceSelection>(this, false));
  bool is_insert_undo_clear = ClearSelection();
  // It is necessary to determine whether the value of `undo_remaining_` is 2 or
  // 3 based on ClearSelection().
  if (!is_insert_undo_clear) {
    m_Undo.GetLastAddItem()->set_undo_remaining(2);
  }
  // Select the inserted text.
  CPVT_WordPlace caret_before_insert = m_wpCaret;
  InsertText(text, FX_Charset::kDefault);
  CPVT_WordPlace caret_after_insert = m_wpCaret;
  m_SelState.Set(caret_before_insert, caret_after_insert);

  AddEditUndoItem(std::make_unique<UndoReplaceSelection>(this, true));
  if (!is_insert_undo_clear) {
    m_Undo.GetLastAddItem()->set_undo_remaining(2);
  }
}

void CPWL_EditImpl::ReplaceSelection(const WideString& text) {
  AddEditUndoItem(std::make_unique<UndoReplaceSelection>(this, false));
  bool is_insert_undo_clear = ClearSelection();
  // It is necessary to determine whether the value of `undo_remaining_` is 2 or
  // 3 based on ClearSelection().
  if (!is_insert_undo_clear) {
    m_Undo.GetLastAddItem()->set_undo_remaining(2);
  }
  InsertText(text, FX_Charset::kDefault);
  AddEditUndoItem(std::make_unique<UndoReplaceSelection>(this, true));
  if (!is_insert_undo_clear) {
    m_Undo.GetLastAddItem()->set_undo_remaining(2);
  }
}

bool CPWL_EditImpl::Redo() {
  if (m_bEnableUndo) {
    if (m_Undo.CanRedo()) {
      m_Undo.Redo();
      return true;
    }
  }

  return false;
}

bool CPWL_EditImpl::Undo() {
  if (m_bEnableUndo) {
    if (m_Undo.CanUndo()) {
      m_Undo.Undo();
      return true;
    }
  }

  return false;
}

void CPWL_EditImpl::SetCaretOrigin() {
  if (!m_pVT->IsValid())
    return;

  CPVT_VariableText::Iterator* pIterator = m_pVT->GetIterator();
  pIterator->SetAt(m_wpCaret);
  CPVT_Word word;
  CPVT_Line line;
  if (pIterator->GetWord(word)) {
    m_ptCaret.x = word.ptWord.x + word.fWidth;
    m_ptCaret.y = word.ptWord.y;
  } else if (pIterator->GetLine(line)) {
    m_ptCaret.x = line.ptLine.x;
    m_ptCaret.y = line.ptLine.y;
  }
}

CPVT_WordPlace CPWL_EditImpl::WordIndexToWordPlace(int32_t index) const {
  if (m_pVT->IsValid())
    return m_pVT->WordIndexToWordPlace(index);

  return CPVT_WordPlace();
}

bool CPWL_EditImpl::IsTextFull() const {
  int32_t nTotalWords = m_pVT->GetTotalWords();
  int32_t nLimitChar = m_pVT->GetLimitChar();
  int32_t nCharArray = m_pVT->GetCharArray();

  return IsTextOverflow() || (nLimitChar > 0 && nTotalWords >= nLimitChar) ||
         (nCharArray > 0 && nTotalWords >= nCharArray);
}

bool CPWL_EditImpl::IsTextOverflow() const {
  if (!m_bEnableScroll && !m_bEnableOverflow) {
    CFX_FloatRect rcPlate = m_pVT->GetPlateRect();
    CFX_FloatRect rcContent = m_pVT->GetContentRect();

    if (m_pVT->IsMultiLine() && GetTotalLines() > 1 &&
        FXSYS_IsFloatBigger(rcContent.Height(), rcPlate.Height())) {
      return true;
    }

    if (FXSYS_IsFloatBigger(rcContent.Width(), rcPlate.Width()))
      return true;
  }

  return false;
}

bool CPWL_EditImpl::CanUndo() const {
  if (m_bEnableUndo) {
    return m_Undo.CanUndo();
  }

  return false;
}

bool CPWL_EditImpl::CanRedo() const {
  if (m_bEnableUndo) {
    return m_Undo.CanRedo();
  }

  return false;
}

void CPWL_EditImpl::EnableRefresh(bool bRefresh) {
  m_bEnableRefresh = bRefresh;
}

void CPWL_EditImpl::EnableUndo(bool bUndo) {
  m_bEnableUndo = bUndo;
}

CPVT_WordPlace CPWL_EditImpl::DoInsertText(const CPVT_WordPlace& place,
                                           const WideString& sText,
                                           FX_Charset charset) {
  if (!m_pVT->IsValid())
    return place;

  CPVT_WordPlace wp = place;
  for (size_t i = 0; i < sText.GetLength(); ++i) {
    uint16_t word = sText[i];
    switch (word) {
      case '\r':
        wp = m_pVT->InsertSection(wp);
        if (i + 1 < sText.GetLength() && sText[i + 1] == '\n')
          i++;
        break;
      case '\n':
        wp = m_pVT->InsertSection(wp);
        break;
      case '\t':
        word = ' ';
        [[fallthrough]];
      default:
        wp = m_pVT->InsertWord(wp, word, GetCharSetFromUnicode(word, charset));
        break;
    }
  }
  return wp;
}

FX_Charset CPWL_EditImpl::GetCharSetFromUnicode(uint16_t word,
                                                FX_Charset nOldCharset) {
  if (IPVT_FontMap* pFontMap = GetFontMap())
    return pFontMap->CharSetFromUnicode(word, nOldCharset);
  return nOldCharset;
}

void CPWL_EditImpl::AddEditUndoItem(
    std::unique_ptr<UndoItemIface> pEditUndoItem) {
  m_Undo.AddItem(std::move(pEditUndoItem));
}

ByteString CPWL_EditImpl::GetPDFWordString(int32_t nFontIndex,
                                           uint16_t Word,
                                           uint16_t SubWord) {
  IPVT_FontMap* pFontMap = GetFontMap();
  RetainPtr<CPDF_Font> pPDFFont = pFontMap->GetPDFFont(nFontIndex);
  if (!pPDFFont)
    return ByteString();

  ByteString sWord;
  if (SubWord > 0) {
    Word = SubWord;
  } else {
    uint32_t dwCharCode = pPDFFont->IsUnicodeCompatible()
                              ? pPDFFont->CharCodeFromUnicode(Word)
                              : pFontMap->CharCodeFromUnicode(nFontIndex, Word);
    if (dwCharCode > 0) {
      pPDFFont->AppendChar(&sWord, dwCharCode);
      return sWord;
    }
  }
  pPDFFont->AppendChar(&sWord, Word);
  return sWord;
}

CPWL_EditImpl::SelectState::SelectState() = default;

CPWL_EditImpl::SelectState::SelectState(const CPVT_WordRange& range) {
  Set(range.BeginPos, range.EndPos);
}

CPVT_WordRange CPWL_EditImpl::SelectState::ConvertToWordRange() const {
  return CPVT_WordRange(BeginPos, EndPos);
}

void CPWL_EditImpl::SelectState::Reset() {
  BeginPos.Reset();
  EndPos.Reset();
}

void CPWL_EditImpl::SelectState::Set(const CPVT_WordPlace& begin,
                                     const CPVT_WordPlace& end) {
  BeginPos = begin;
  EndPos = end;
}

void CPWL_EditImpl::SelectState::SetEndPos(const CPVT_WordPlace& end) {
  EndPos = end;
}

bool CPWL_EditImpl::SelectState::IsEmpty() const {
  return BeginPos == EndPos;
}
