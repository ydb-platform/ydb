// Copyright 2016-2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//


#include "defaults.h"

#include <utility>

namespace fst {
namespace internal {

StringDefaultsStack *StringDefaultsStack::Get() {
  static auto *kInstance = new StringDefaultsStack();
  return kInstance;
}

TokenType StringDefaultsStack::GetTokenType() const {
  return stack_.top().first;
}

const SymbolTable *StringDefaultsStack::GetSymbols() const {
  return stack_.top().second.get();
}

// The stack is initialized with default values: BYTE and a null symbol table.
StringDefaultsStack::StringDefaultsStack() { Push(); }

void StringDefaultsStack::Push(TokenType token_type,
                               const SymbolTable *symbols) {
  stack_.emplace(token_type, symbols ? symbols->Copy() : nullptr);
}

void StringDefaultsStack::Pop() { stack_.pop(); }

}  // namespace internal

TokenType GetDefaultTokenType() {
  static const auto *stack = internal::StringDefaultsStack::Get();
  return stack->GetTokenType();
}

const SymbolTable *GetDefaultSymbols() {
  static const auto *stack = internal::StringDefaultsStack::Get();
  return stack->GetSymbols();
}

void PushDefaults(TokenType token_type, const SymbolTable *symbols) {
  static auto *stack = internal::StringDefaultsStack::Get();
  stack->Push(token_type, symbols);
}

void PopDefaults() {
  static auto *stack = internal::StringDefaultsStack::Get();
  stack->Pop();
}

}  // namespace fst

