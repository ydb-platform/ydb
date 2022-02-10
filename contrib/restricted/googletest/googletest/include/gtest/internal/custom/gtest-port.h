// Copyright 2015, Google Inc. 
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
// 
// Injection point for custom user configurations. See README for details
// 
// ** Custom implementation starts here ** 
 
#ifndef GTEST_INCLUDE_GTEST_INTERNAL_CUSTOM_GTEST_PORT_H_ 
#define GTEST_INCLUDE_GTEST_INTERNAL_CUSTOM_GTEST_PORT_H_ 
 
#include <re2/re2.h>

// this macro disables built-in implementation of class RE
#define GTEST_USES_PCRE 1

namespace testing::internal {
    // custom implementation of class RE based on re2

    class RE {
    public:
        RE(const char* regex) : re2_(regex) {}
        RE(const std::string& regex) : re2_(regex) {}

        RE(const RE& other) : RE(other.pattern()) {}

        // Returns the string representation of the regex.
        const char* pattern() const {
            return re2_.pattern().c_str();
        }

        // FullMatch(str, re) returns true if and only if regular expression re
        // matches the entire str.
        static bool FullMatch(const ::std::string& str, const RE& re) {
            return FullMatch(str.c_str(), re);
        }

        // PartialMatch(str, re) returns true if and only if regular expression re
        // matches a substring of str (including str itself).
        static bool PartialMatch(const ::std::string& str, const RE& re) {
            return PartialMatch(str.c_str(), re);
        }

        static bool FullMatch(const char* str, const RE& re) {
            return re2::RE2::FullMatchN(str, re.re2_, nullptr, 0);
        }

        static bool PartialMatch(const char* str, const RE& re) {
            return re2::RE2::PartialMatchN(str, re.re2_, nullptr, 0);
        }

    private:
        re2::RE2 re2_;
    };
}

#endif  // GTEST_INCLUDE_GTEST_INTERNAL_CUSTOM_GTEST_PORT_H_ 
