// Copyright 2022 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef FPDFSDK_PWL_CPWL_COMBO_BOX_EMBEDDERTEST_H_
#define FPDFSDK_PWL_CPWL_COMBO_BOX_EMBEDDERTEST_H_

#include "public/fpdfview.h"
#include "testing/embedder_test.h"
#include "testing/gtest/include/gtest/gtest.h"

class CFFL_FormField;
class CPDFSDK_FormFillEnvironment;
class CPDFSDK_PageView;
class CPDFSDK_Widget;
class CPWL_ComboBox;

class CPWLComboBoxEmbedderTest : public EmbedderTest {
 protected:
  void SetUp() override;
  void TearDown() override;

  void CreateAndInitializeFormComboboxPDF();
  void FormFillerAndWindowSetup(CPDFSDK_Widget* pAnnotCombobox);
  void TypeTextIntoTextField(int num_chars);
  FPDF_PAGE GetPage() const { return m_page; }
  CPWL_ComboBox* GetCPWLComboBox() const { return m_pComboBox; }
  CFFL_FormField* GetCFFLFormField() const { return m_pFormField; }
  CPDFSDK_Widget* GetCPDFSDKAnnotNormal() const { return m_pAnnotNormal; }
  CPDFSDK_Widget* GetCPDFSDKAnnotUserEditable() const {
    return m_pAnnotEditable;
  }
  CPDFSDK_FormFillEnvironment* GetCPDFSDKFormFillEnv() const {
    return m_pFormFillEnv;
  }
  CPDFSDK_PageView* GetPageView() const { return m_pPageView; }

 private:
  FPDF_PAGE m_page;
  CPWL_ComboBox* m_pComboBox = nullptr;
  CFFL_FormField* m_pFormField = nullptr;
  CPDFSDK_Widget* m_pAnnotNormal = nullptr;
  CPDFSDK_Widget* m_pAnnotEditable = nullptr;
  CPDFSDK_FormFillEnvironment* m_pFormFillEnv = nullptr;
  CPDFSDK_PageView* m_pPageView = nullptr;
};

#endif  // FPDFSDK_PWL_CPWL_COMBO_BOX_EMBEDDERTEST_H_
