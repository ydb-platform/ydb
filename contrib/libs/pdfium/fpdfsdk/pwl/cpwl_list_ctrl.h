// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef FPDFSDK_PWL_CPWL_LIST_CTRL_H_
#define FPDFSDK_PWL_CPWL_LIST_CTRL_H_

#include <map>
#include <memory>
#include <vector>

#include "core/fxcrt/fx_coordinates.h"
#include "core/fxcrt/unowned_ptr.h"
#include "core/fxcrt/widestring.h"
#include "fpdfsdk/pwl/cpwl_edit_impl.h"

class IPVT_FontMap;

class CPWL_ListCtrl {
 public:
  class NotifyIface {
   public:
    virtual ~NotifyIface();

    virtual void OnSetScrollInfoY(float fPlateMin,
                                  float fPlateMax,
                                  float fContentMin,
                                  float fContentMax,
                                  float fSmallStep,
                                  float fBigStep) = 0;
    virtual void OnSetScrollPosY(float fy) = 0;

    // Returns true if `this` is still allocated.
    [[nodiscard]] virtual bool OnInvalidateRect(const CFX_FloatRect& rect) = 0;
  };

  CPWL_ListCtrl();
  ~CPWL_ListCtrl();

  void SetNotify(NotifyIface* pNotify) { m_pNotify = pNotify; }
  void OnMouseDown(const CFX_PointF& point, bool bShift, bool bCtrl);
  void OnMouseMove(const CFX_PointF& point, bool bShift, bool bCtrl);
  void OnVK_UP(bool bShift, bool bCtrl);
  void OnVK_DOWN(bool bShift, bool bCtrl);
  void OnVK_LEFT(bool bShift, bool bCtrl);
  void OnVK_RIGHT(bool bShift, bool bCtrl);
  void OnVK_HOME(bool bShift, bool bCtrl);
  void OnVK_END(bool bShift, bool bCtrl);
  bool OnChar(uint16_t nChar, bool bShift, bool bCtrl);

  void SetScrollPos(const CFX_PointF& point);
  void ScrollToListItem(int32_t nItemIndex);
  CFX_FloatRect GetItemRect(int32_t nIndex) const;
  int32_t GetCaret() const { return m_nCaretIndex; }
  int32_t GetSelect() const { return m_nSelItem; }
  int32_t GetTopItem() const;
  CFX_FloatRect GetContentRect() const;

  int32_t GetItemIndex(const CFX_PointF& point) const;
  void AddString(const WideString& str);
  void SetTopItem(int32_t nIndex);
  void Select(int32_t nItemIndex);
  void Deselect(int32_t nItemIndex);
  void SetCaret(int32_t nItemIndex);
  WideString GetText() const;

  void SetFontMap(IPVT_FontMap* pFontMap) { m_pFontMap = pFontMap; }
  void SetFontSize(float fFontSize) { m_fFontSize = fFontSize; }
  CFX_FloatRect GetPlateRect() const { return m_rcPlate; }
  void SetPlateRect(const CFX_FloatRect& rect);

  float GetFontSize() const { return m_fFontSize; }
  CPWL_EditImpl* GetItemEdit(int32_t nIndex) const;
  int32_t GetCount() const;
  bool IsItemSelected(int32_t nIndex) const;
  float GetFirstHeight() const;
  void SetMultipleSel(bool bMultiple) { m_bMultiple = bMultiple; }
  bool IsMultipleSel() const { return m_bMultiple; }
  int32_t FindNext(int32_t nIndex, wchar_t nChar) const;
  int32_t GetFirstSelected() const;

 private:
  class Item {
   public:
    Item();
    ~Item();

    void SetFontMap(IPVT_FontMap* pFontMap);
    CPWL_EditImpl* GetEdit() const { return m_pEdit.get(); }

    void SetRect(const CFX_FloatRect& rect) { m_rcListItem = rect; }
    void SetSelect(bool bSelected) { m_bSelected = bSelected; }
    void SetText(const WideString& text);
    void SetFontSize(float fFontSize);
    WideString GetText() const;

    CFX_FloatRect GetRect() const { return m_rcListItem; }
    bool IsSelected() const { return m_bSelected; }
    float GetItemHeight() const;
    uint16_t GetFirstChar() const;

   private:
    CPWL_EditImpl::Iterator* GetIterator() const;

    bool m_bSelected = false;
    CFX_FloatRect m_rcListItem;
    std::unique_ptr<CPWL_EditImpl> const m_pEdit;
  };

  class SelectState {
   public:
    enum State { DESELECTING = -1, NORMAL = 0, SELECTING = 1 };
    using const_iterator = std::map<int32_t, State>::const_iterator;

    SelectState();
    ~SelectState();

    void Add(int32_t nItemIndex);
    void Add(int32_t nBeginIndex, int32_t nEndIndex);
    void Sub(int32_t nItemIndex);
    void Sub(int32_t nBeginIndex, int32_t nEndIndex);
    void DeselectAll();
    void Done();

    const_iterator begin() const { return m_Items.begin(); }
    const_iterator end() const { return m_Items.end(); }

   private:
    std::map<int32_t, State> m_Items;
  };

  CFX_PointF InToOut(const CFX_PointF& point) const;
  CFX_PointF OutToIn(const CFX_PointF& point) const;
  CFX_FloatRect InToOut(const CFX_FloatRect& rect) const;
  CFX_FloatRect OutToIn(const CFX_FloatRect& rect) const;

  CFX_PointF InnerToOuter(const CFX_PointF& point) const;
  CFX_PointF OuterToInner(const CFX_PointF& point) const;
  CFX_FloatRect InnerToOuter(const CFX_FloatRect& rect) const;
  CFX_FloatRect OuterToInner(const CFX_FloatRect& rect) const;

  void OnVK(int32_t nItemIndex, bool bShift, bool bCtrl);
  bool IsValid(int32_t nItemIndex) const;

  void ReArrange(int32_t nItemIndex);
  CFX_FloatRect GetItemRectInternal(int32_t nIndex) const;
  CFX_FloatRect GetContentRectInternal() const;
  void SetMultipleSelect(int32_t nItemIndex, bool bSelected);
  void SetSingleSelect(int32_t nItemIndex);
  void InvalidateItem(int32_t nItemIndex);
  void SelectItems();
  bool IsItemVisible(int32_t nItemIndex) const;
  void SetScrollInfo();
  void SetScrollPosY(float fy);
  void AddItem(const WideString& str);
  WideString GetItemText(int32_t nIndex) const;
  void SetItemSelect(int32_t nIndex, bool bSelected);
  int32_t GetLastSelected() const;
  CFX_PointF GetBTPoint() const {
    return CFX_PointF(m_rcPlate.left, m_rcPlate.top);
  }

  CFX_FloatRect m_rcPlate;
  CFX_FloatRect m_rcContent;
  CFX_PointF m_ptScrollPos;
  float m_fFontSize = 0.0f;

  // For single:
  int32_t m_nSelItem = -1;

  // For multiple:
  SelectState m_SelectState;
  int32_t m_nFootIndex = -1;
  int32_t m_nCaretIndex = -1;
  bool m_bCtrlSel = false;

  bool m_bMultiple = false;
  bool m_bNotifyFlag = false;
  UnownedPtr<NotifyIface> m_pNotify;
  std::vector<std::unique_ptr<Item>> m_ListItems;
  UnownedPtr<IPVT_FontMap> m_pFontMap;
};

#endif  // FPDFSDK_PWL_CPWL_LIST_CTRL_H_
