// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "fxjs/global_timer.h"

#include <map>

#include "core/fxcrt/cfx_timer.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/containers/contains.h"
#include "fxjs/cjs_app.h"

namespace {

using TimerMap = std::map<int32_t, GlobalTimer*>;
TimerMap* g_global_timer_map = nullptr;

}  // namespace

// static
void GlobalTimer::InitializeGlobals() {
  CHECK(!g_global_timer_map);
  g_global_timer_map = new TimerMap();
}

// static
void GlobalTimer::DestroyGlobals() {
  delete g_global_timer_map;
  g_global_timer_map = nullptr;
}

GlobalTimer::GlobalTimer(CJS_App* pObj,
                         CJS_Runtime* pRuntime,
                         Type nType,
                         const WideString& script,
                         uint32_t dwElapse,
                         uint32_t dwTimeOut)
    : m_nType(nType),
      m_nTimerID(pRuntime->GetTimerHandler()->SetTimer(dwElapse, Trigger)),
      m_dwTimeOut(dwTimeOut),
      m_swJScript(script),
      m_pRuntime(pRuntime),
      m_pEmbedApp(pObj) {
  if (HasValidID()) {
    DCHECK(!pdfium::Contains((*g_global_timer_map), m_nTimerID));
    (*g_global_timer_map)[m_nTimerID] = this;
  }
}

GlobalTimer::~GlobalTimer() {
  if (!HasValidID())
    return;

  if (m_pRuntime && m_pRuntime->GetTimerHandler())
    m_pRuntime->GetTimerHandler()->KillTimer(m_nTimerID);

  DCHECK(pdfium::Contains((*g_global_timer_map), m_nTimerID));
  g_global_timer_map->erase(m_nTimerID);
}

// static
void GlobalTimer::Trigger(int32_t nTimerID) {
  auto it = g_global_timer_map->find(nTimerID);
  if (it == g_global_timer_map->end()) {
    return;
  }

  GlobalTimer* pTimer = it->second;
  if (pTimer->m_bProcessing)
    return;

  pTimer->m_bProcessing = true;
  if (pTimer->m_pEmbedApp)
    pTimer->m_pEmbedApp->TimerProc(pTimer);

  // Timer proc may have destroyed timer, find it again.
  it = g_global_timer_map->find(nTimerID);
  if (it == g_global_timer_map->end()) {
    return;
  }

  pTimer = it->second;
  pTimer->m_bProcessing = false;
  if (pTimer->IsOneShot())
    pTimer->m_pEmbedApp->CancelProc(pTimer);
}

// static
void GlobalTimer::Cancel(int32_t nTimerID) {
  auto it = g_global_timer_map->find(nTimerID);
  if (it == g_global_timer_map->end()) {
    return;
  }

  GlobalTimer* pTimer = it->second;
  pTimer->m_pEmbedApp->CancelProc(pTimer);
}

bool GlobalTimer::HasValidID() const {
  return m_nTimerID != CFX_Timer::HandlerIface::kInvalidTimerID;
}
