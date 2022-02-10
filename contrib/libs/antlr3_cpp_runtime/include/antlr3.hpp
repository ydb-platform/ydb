#ifndef	_ANTLR3_HPP
#define	_ANTLR3_HPP

// [The "BSD licence"]
// Copyright (c) 2005-2009 Gokulakannan Somasundaram, ElectronDB
//
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
// 1. Redistributions of source code must retain the above copyright
//    notice, this list of conditions and the following disclaimer.
// 2. Redistributions in binary form must reproduce the above copyright
//    notice, this list of conditions and the following disclaimer in the
//    documentation and/or other materials provided with the distribution.
// 3. The name of the author may not be used to endorse or promote products
//    derived from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
// IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
// OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
// IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
// INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
// NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
// THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#include <cassert>
#include <cstddef> // ptrdiff_t
#include <cstdint> // uint32_t, ...
#include <cstdio>  // stderr (TODO remove fprintf(stderr)
#include <cstdlib> // malloc, calloc
#include <cstring>

#include <algorithm>
#include <deque>
#include <exception>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <new>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "antlr3defs.hpp"

/* Pre declare the typedefs for all the interfaces, then
 * they can be inter-dependant and we will let the linker
 * sort it out for us.
 */
#include "antlr3interfaces.hpp"

// Include the unicode.org conversion library header.
//
#include "antlr3convertutf.hpp"

#include "antlr3errors.hpp"
#include "antlr3memory.hpp"

#include "antlr3collections.hpp"
#include "antlr3recognizersharedstate.hpp"
#include "antlr3baserecognizer.hpp"
#include "antlr3bitset.hpp"
#include "antlr3commontoken.hpp"
#include "antlr3commontree.hpp"
#include "antlr3commontreeadaptor.hpp"
#include "antlr3cyclicdfa.hpp"
#include "antlr3debugeventlistener.hpp"
#include "antlr3exception.hpp"
#include "antlr3filestream.hpp"
#include "antlr3intstream.hpp"
#include "antlr3input.hpp"
#include "antlr3tokenstream.hpp"
#include "antlr3commontreenodestream.hpp"
#include "antlr3lexer.hpp"
#include "antlr3parser.hpp"
//#include "antlr3rewritestreams.hpp"
#include "antlr3rewriteruletokenstream.hpp"
#include "antlr3rewriterulesubtreestream.hpp"
#include "antlr3traits.hpp"
#include "antlr3treeparser.hpp"

#endif
