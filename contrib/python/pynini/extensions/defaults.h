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


#ifndef PYNINI_DEFAULTS_H_
#define PYNINI_DEFAULTS_H_

// This module defines a singleton class which stores defaults for string
// compilation.

#include <memory>
#include <stack>
#include <utility>

#include <fst/string.h>
#include <fst/symbol-table.h>

namespace fst {
namespace internal {

// This defines a stack of defaults for string compilation. It is a singleton
// and can only be accessed via `Get`. Each element in the stack is a pair of
// a TokenType and an owned SymbolTable (or null). At creation, the stack is
// initialized to {BYTE, nullptr}. Getter methods return the values at the
// top of the stack.
class StringDefaultsStack {
 public:
  static StringDefaultsStack *Get();

  // Returns the token type at the top of the stack.
  TokenType GetTokenType() const;

  // Returns the symbol table at the top of the stack.
  const SymbolTable *GetSymbols() const;

  // A copy of the symbol table is taken if it is non-null.
  void Push(TokenType token_type = TokenType::BYTE,
            const SymbolTable *symbols = nullptr);

  void Pop();

 private:
  // Standard constructor is private; other constructors are deleted. This
  // enforces the desired singleton pattern.
  StringDefaultsStack();
  StringDefaultsStack(const StringDefaultsStack &) = delete;
  StringDefaultsStack &operator=(const StringDefaultsStack &) = delete;

  using DefaultState = std::pair<TokenType, const std::unique_ptr<SymbolTable>>;

  std::stack<DefaultState> stack_;
};

}  // namespace internal

// Convenience methods, to eliminate the need to call Get on the singleton.

TokenType GetDefaultTokenType();

const SymbolTable *GetDefaultSymbols();

// A copy of the symbol table is taken if it is non-null.
void PushDefaults(TokenType token_type, const SymbolTable *symbols = nullptr);

void PopDefaults();

}  // namespace fst

#endif  // PYNINI_DEFAULTS_H_

