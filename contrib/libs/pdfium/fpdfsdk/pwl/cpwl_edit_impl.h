// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef FPDFSDK_PWL_CPWL_EDIT_IMPL_H_
#define FPDFSDK_PWL_CPWL_EDIT_IMPL_H_

#include <deque>
#include <memory>
#include <utility>
#include <vector>

#include "core/fpdfdoc/cpvt_variabletext.h"
#include "core/fpdfdoc/cpvt_wordrange.h"
#include "core/fxcrt/bytestring.h"
#include "core/fxcrt/fx_codepage_forward.h"
#include "core/fxcrt/unowned_ptr.h"
#include "core/fxge/dib/fx_dib.h"
#include "fpdfsdk/pwl/ipwl_fillernotify.h"

class CFX_RenderDevice;
class CPWL_Edit;

class CPWL_EditImpl {
 public:
  class Iterator {
   public:
    Iterator(CPWL_EditImpl* pEdit, CPVT_VariableText::Iterator* pVTIterator);
    ~Iterator();

    bool NextWord();
    bool GetWord(CPVT_Word& word) const;
    bool GetLine(CPVT_Line& line) const;
    void SetAt(int32_t nWordIndex);
    void SetAt(const CPVT_WordPlace& place);
    const CPVT_WordPlace& GetAt() const;

   private:
    UnownedPtr<CPWL_EditImpl> m_pEdit;
    UnownedPtr<CPVT_VariableText::Iterator> m_pVTIterator;
  };

  CPWL_EditImpl();
  ~CPWL_EditImpl();

  void DrawEdit(CFX_RenderDevice* pDevice,
                const CFX_Matrix& mtUser2Device,
                FX_COLORREF crTextFill,
                const CFX_FloatRect& rcClip,
                const CFX_PointF& ptOffset,
                const CPVT_WordRange* pRange,
                IPWL_FillerNotify* pFillerNotify,
                IPWL_FillerNotify::PerWindowData* pSystemData);

  void SetFontMap(IPVT_FontMap* pFontMap);
  void SetNotify(CPWL_Edit* pNotify);

  // Returns an iterator for the contents. Should not be released.
  Iterator* GetIterator();
  IPVT_FontMap* GetFontMap();
  void Initialize();

  // Set the bounding box of the text area.
  void SetPlateRect(const CFX_FloatRect& rect);
  void SetScrollPos(const CFX_PointF& point);

  // Set the horizontal text alignment. (nFormat [0:left, 1:middle, 2:right])
  void SetAlignmentH(int32_t nFormat);

  // Set the vertical text alignment. (nFormat [0:left, 1:middle, 2:right])
  void SetAlignmentV(int32_t nFormat);

  // Set the substitution character for hidden text.
  void SetPasswordChar(uint16_t wSubWord);

  // Set the maximum number of words in the text.
  void SetLimitChar(int32_t nLimitChar);
  void SetCharArray(int32_t nCharArray);
  void SetMultiLine(bool bMultiLine);
  void SetAutoReturn(bool bAuto);
  void SetAutoFontSize(bool bAuto);
  void SetAutoScroll(bool bAuto);
  void SetFontSize(float fFontSize);
  void SetTextOverflow(bool bAllowed);
  void OnMouseDown(const CFX_PointF& point, bool bShift, bool bCtrl);
  void OnMouseMove(const CFX_PointF& point, bool bShift, bool bCtrl);
  void OnVK_UP(bool bShift);
  void OnVK_DOWN(bool bShift);
  void OnVK_LEFT(bool bShift);
  void OnVK_RIGHT(bool bShift);
  void OnVK_HOME(bool bShift, bool bCtrl);
  void OnVK_END(bool bShift, bool bCtrl);
  void SetText(const WideString& sText);
  bool InsertWord(uint16_t word, FX_Charset charset);
  bool InsertReturn();
  bool Backspace();
  bool Delete();
  bool ClearSelection();
  bool InsertText(const WideString& sText, FX_Charset charset);
  void ReplaceAndKeepSelection(const WideString& text);
  void ReplaceSelection(const WideString& text);
  bool Redo();
  bool Undo();
  CPVT_WordPlace WordIndexToWordPlace(int32_t index) const;
  CPVT_WordPlace SearchWordPlace(const CFX_PointF& point) const;
  int32_t GetCaret() const;
  CPVT_WordPlace GetCaretWordPlace() const;
  WideString GetSelectedText() const;
  WideString GetText() const;
  float GetFontSize() const;
  uint16_t GetPasswordChar() const;
  CFX_PointF GetScrollPos() const;
  int32_t GetCharArray() const;
  CFX_FloatRect GetContentRect() const;
  WideString GetRangeText(const CPVT_WordRange& range) const;
  void SetSelection(int32_t nStartChar, int32_t nEndChar);
  std::pair<int32_t, int32_t> GetSelection() const;
  void SelectAll();
  void SelectNone();
  bool IsSelected() const;
  void Paint();
  void EnableRefresh(bool bRefresh);
  void RefreshWordRange(const CPVT_WordRange& wr);
  CPVT_WordRange GetWholeWordRange() const;
  CPVT_WordRange GetSelectWordRange() const;
  void EnableUndo(bool bUndo);
  bool IsTextFull() const;
  bool CanUndo() const;
  bool CanRedo() const;
  CPVT_WordRange GetVisibleWordRange() const;

  ByteString GetPDFWordString(int32_t nFontIndex,
                              uint16_t Word,
                              uint16_t SubWord);

 private:
  class RefreshState {
   public:
    RefreshState();
    ~RefreshState();

    void BeginRefresh();
    void Push(const CPVT_WordRange& linerange, const CFX_FloatRect& rect);
    void NoAnalyse();
    std::vector<CFX_FloatRect>* GetRefreshRects();
    void EndRefresh();

   private:
    struct LineRect {
      LineRect(const CPVT_WordRange& wrLine, const CFX_FloatRect& rcLine)
          : m_wrLine(wrLine), m_rcLine(rcLine) {}

      CPVT_WordRange m_wrLine;
      CFX_FloatRect m_rcLine;
    };

    void Add(const CFX_FloatRect& new_rect);

    std::vector<LineRect> m_NewLineRects;
    std::vector<LineRect> m_OldLineRects;
    std::vector<CFX_FloatRect> m_RefreshRects;
  };

  class SelectState {
   public:
    SelectState();
    explicit SelectState(const CPVT_WordRange& range);

    void Reset();
    void Set(const CPVT_WordPlace& begin, const CPVT_WordPlace& end);
    void SetEndPos(const CPVT_WordPlace& end);

    CPVT_WordRange ConvertToWordRange() const;
    bool IsEmpty() const;

    CPVT_WordPlace BeginPos;
    CPVT_WordPlace EndPos;
  };

  class UndoItemIface {
   public:
    virtual ~UndoItemIface() = default;

    // Undo/Redo the current undo item and returns the number of additional
    // items to be processed in |m_UndoItemStack| to fully undo/redo the action.
    // (An example is UndoReplaceSelection::Undo(), if UndoReplaceSelection
    // marks the end of a replace action, UndoReplaceSelection::Undo() returns
    // |undo_remaining_|. The default value of |undo_remaining_| in
    // UndoReplaceSelection is 3. because 3 more undo items need to be processed
    // to revert the replace action: insert text, clear selection and the
    // UndoReplaceSelection which marks the beginning of replace action. If
    // CPWL_EditImpl::ClearSelection() returns false, the value of
    // |undo_remaining_| in UndoReplaceSelection needs to be set to 2)
    // Implementations should return 0 by default.
    virtual int Undo() = 0;
    virtual int Redo() = 0;
    void set_undo_remaining(int undo_remaining) {
      undo_remaining_ = undo_remaining;
    }
    int undo_remaining() const { return undo_remaining_; }

   private:
    int undo_remaining_ = 0;
  };

  class UndoStack {
   public:
    UndoStack();
    ~UndoStack();

    void AddItem(std::unique_ptr<UndoItemIface> pItem);
    void Undo();
    void Redo();
    bool CanUndo() const;
    bool CanRedo() const;
    // GetLastAddItem() will never return null, so it can only be called after
    // calling AddItem().
    UndoItemIface* GetLastAddItem();

   private:
    void RemoveHeads();
    void RemoveTails();

    std::deque<std::unique_ptr<UndoItemIface>> m_UndoItemStack;
    size_t m_nCurUndoPos = 0;
    bool m_bWorking = false;
  };

  class Provider;
  class UndoBackspace;
  class UndoClear;
  class UndoDelete;
  class UndoInsertReturn;
  class UndoInsertText;
  class UndoInsertWord;
  class UndoReplaceSelection;

  bool IsTextOverflow() const;
  bool Clear();
  CPVT_WordPlace DoInsertText(const CPVT_WordPlace& place,
                              const WideString& sText,
                              FX_Charset charset);
  FX_Charset GetCharSetFromUnicode(uint16_t word, FX_Charset nOldCharset);
  int32_t GetTotalLines() const;
  void SetSelection(const CPVT_WordPlace& begin, const CPVT_WordPlace& end);
  bool Delete(bool bAddUndo);
  bool Clear(bool bAddUndo);
  bool InsertText(const WideString& sText, FX_Charset charset, bool bAddUndo);
  bool InsertWord(uint16_t word, FX_Charset charset, bool bAddUndo);
  bool InsertReturn(bool bAddUndo);
  bool Backspace(bool bAddUndo);
  void SetCaret(const CPVT_WordPlace& place);

  CFX_PointF VTToEdit(const CFX_PointF& point) const;

  void RearrangeAll();
  void RearrangePart(const CPVT_WordRange& range);
  void ScrollToCaret();
  void SetScrollInfo();
  void SetScrollPosX(float fx);
  void SetScrollPosY(float fy);
  void SetScrollLimit();
  void SetContentChanged();

  void PaintInsertText(const CPVT_WordPlace& wpOld,
                       const CPVT_WordPlace& wpNew);

  CFX_PointF EditToVT(const CFX_PointF& point) const;
  CFX_FloatRect VTToEdit(const CFX_FloatRect& rect) const;

  void Refresh();
  void RefreshPushLineRects(const CPVT_WordRange& wr);

  void SetCaretInfo();
  void SetCaretOrigin();

  void AddEditUndoItem(std::unique_ptr<UndoItemIface> pEditUndoItem);

  bool m_bEnableScroll = false;
  bool m_bNotifyFlag = false;
  bool m_bEnableOverflow = false;
  bool m_bEnableRefresh = true;
  bool m_bEnableUndo = true;
  int32_t m_nAlignment = 0;
  std::unique_ptr<Provider> m_pVTProvider;
  std::unique_ptr<CPVT_VariableText> m_pVT;  // Must outlive |m_pVTProvider|.
  UnownedPtr<CPWL_Edit> m_pNotify;
  CPVT_WordPlace m_wpCaret;
  CPVT_WordPlace m_wpOldCaret;
  SelectState m_SelState;
  CFX_PointF m_ptScrollPos;
  CFX_PointF m_ptRefreshScrollPos;
  std::unique_ptr<Iterator> m_pIterator;
  RefreshState m_Refresh;
  CFX_PointF m_ptCaret;
  UndoStack m_Undo;
  CFX_FloatRect m_rcOldContent;
};

#endif  // FPDFSDK_PWL_CPWL_EDIT_IMPL_H_
