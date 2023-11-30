// Copyright (c) 2005, Google Inc.
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

// ---
// Author: Sanjay Ghemawat <opensource@google.com>

#include <assert.h>
#include <string.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>

#include <new>
#include <string>

#include "malloc_extension.h"

// Note: this routine is meant to be called before threads are spawned.
void MallocExtension::Initialize() {
  static bool initialize_called = false;

  if (initialize_called) return;
  initialize_called = true;

  // GNU libc++ versions 3.3 and 3.4 obey the environment variables
  // GLIBCPP_FORCE_NEW and GLIBCXX_FORCE_NEW respectively.  Setting
  // one of these variables forces the STL default allocator to call
  // new() or delete() for each allocation or deletion.  Otherwise
  // the STL allocator tries to avoid the high cost of doing
  // allocations by pooling memory internally.  However, tcmalloc
  // does allocations really fast, especially for the types of small
  // items one sees in STL, so it's better off just using us.
  // TODO: control whether we do this via an environment variable?
  setenv("GLIBCPP_FORCE_NEW", "1", false /* no overwrite*/);
  setenv("GLIBCXX_FORCE_NEW", "1", false /* no overwrite*/);

  // Now we need to make the setenv 'stick', which it may not do since
  // the env is flakey before main() is called.  But luckily stl only
  // looks at this env var the first time it tries to do an alloc, and
  // caches what it finds.  So we just cause an stl alloc here.
  std::string dummy("I need to be allocated");
  dummy += "!"; // so the definition of dummy isn't optimized out
}

// Default implementation -- does nothing
MallocExtension::~MallocExtension() { }
bool MallocExtension::VerifyAllMemory() { return true; }
bool MallocExtension::VerifyNewMemory(void* /*p*/) { return true; }
bool MallocExtension::VerifyArrayNewMemory(void* /*p*/) { return true; }
bool MallocExtension::VerifyMallocMemory(void* /*p*/) { return true; }

bool MallocExtension::GetNumericProperty(const char* /*property*/, size_t* /*value*/) {
  return false;
}

bool MallocExtension::SetNumericProperty(const char* /*property*/, size_t /*value*/) {
  return false;
}

void MallocExtension::GetStats(char* buffer, int length) {
  assert(length > 0);
  (void)length;
  buffer[0] = '\0';
}

bool MallocExtension::MallocMemoryStats(int* blocks, size_t* total,
                                       int histogram[kMallocHistogramSize]) {
  *blocks = 0;
  *total = 0;
  memset(histogram, 0, sizeof(histogram));
  return true;
}

void** MallocExtension::ReadStackTraces() {
  return NULL;
}

void** MallocExtension::ReadHeapGrowthStackTraces() {
  return NULL;
}

// The current malloc extension object.  We also keep a pointer to
// the default implementation so that the heap-leak checker does not
// complain about a memory leak.

static pthread_once_t module_init = PTHREAD_ONCE_INIT;
static MallocExtension* default_instance = NULL;
static MallocExtension* current_instance = NULL;

static void InitModule() {
  default_instance = new MallocExtension;
  current_instance = default_instance;
}

MallocExtension* MallocExtension::instance() {
  pthread_once(&module_init, InitModule);
  return current_instance;
}

void MallocExtension::Register(MallocExtension* implementation) {
  pthread_once(&module_init, InitModule);
  current_instance = implementation;
}
