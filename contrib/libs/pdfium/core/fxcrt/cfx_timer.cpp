// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxcrt/cfx_timer.h"

#include <map>

#include "core/fxcrt/check.h"

namespace {

using TimerMap = std::map<int32_t, CFX_Timer*>;
TimerMap* g_pwl_timer_map = nullptr;

}  // namespace

// static
void CFX_Timer::InitializeGlobals() {
  CHECK(!g_pwl_timer_map);
  g_pwl_timer_map = new TimerMap();
}

// static
void CFX_Timer::DestroyGlobals() {
  delete g_pwl_timer_map;
  g_pwl_timer_map = nullptr;
}

CFX_Timer::CFX_Timer(HandlerIface* pHandlerIface,
                     CallbackIface* pCallbackIface,
                     int32_t nInterval)
    : m_pHandlerIface(pHandlerIface), m_pCallbackIface(pCallbackIface) {
  DCHECK(m_pCallbackIface);
  if (m_pHandlerIface) {
    m_nTimerID = m_pHandlerIface->SetTimer(nInterval, TimerProc);
    if (HasValidID())
      (*g_pwl_timer_map)[m_nTimerID] = this;
  }
}

CFX_Timer::~CFX_Timer() {
  if (HasValidID()) {
    g_pwl_timer_map->erase(m_nTimerID);
    if (m_pHandlerIface)
      m_pHandlerIface->KillTimer(m_nTimerID);
  }
}

// static
void CFX_Timer::TimerProc(int32_t idEvent) {
  auto it = g_pwl_timer_map->find(idEvent);
  if (it != g_pwl_timer_map->end()) {
    it->second->m_pCallbackIface->OnTimerFired();
  }
}
