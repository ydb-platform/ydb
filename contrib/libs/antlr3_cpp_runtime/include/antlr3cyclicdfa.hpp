/// Definition of a cyclic dfa structure such that it can be
/// initialized at compile time and have only a single
/// runtime function that can deal with all cyclic dfa
/// structures and show Java how it is done ;-)
///
#ifndef	ANTLR3_CYCLICDFA_HPP
#define	ANTLR3_CYCLICDFA_HPP

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

namespace antlr3 {

template<class ImplTraits, class CtxType>
class CyclicDFA : public ImplTraits::AllocPolicyType
{
public:
	typedef typename CtxType::StreamType StreamType;
	typedef typename CtxType::ExceptionBaseType ExceptionBaseType;
	typedef typename ImplTraits::template RecognizerType<StreamType> RecognizerType;
	typedef typename StreamType::IntStreamType IntStreamType;
	typedef typename StreamType::TokenType	TokenType;
	typedef TokenType	CommonTokenType;
	typedef CtxType ContextType;

private:
    /// Decision number that a particular static structure
    ///  represents.
    ///
    const ANTLR_INT32		m_decisionNumber;

    /// What this decision represents
    ///
    const ANTLR_UCHAR*			m_description;
	const ANTLR_INT32* const	m_eot;
    const ANTLR_INT32* const	m_eof;
    const ANTLR_INT32* const	m_min;
    const ANTLR_INT32* const	m_max;
    const ANTLR_INT32* const	m_accept;
    const ANTLR_INT32* const	m_special;
    const ANTLR_INT32* const *const	m_transition;

public:
	CyclicDFA( ANTLR_INT32	decisionNumber
				, const ANTLR_UCHAR*	description
				, const ANTLR_INT32* const	eot
				, const ANTLR_INT32* const	eof
				, const ANTLR_INT32* const	min
				, const ANTLR_INT32* const	max
				, const ANTLR_INT32* const	accept
				, const ANTLR_INT32* const	special
				, const ANTLR_INT32* const *const	transition );
	CyclicDFA( const CyclicDFA& cdfa );
    CyclicDFA& operator=( const CyclicDFA& dfa);
	
	ANTLR_INT32	specialStateTransition(CtxType * ctx, RecognizerType* recognizer, IntStreamType* is, ANTLR_INT32 s);
    ANTLR_INT32	specialTransition(CtxType * ctx, RecognizerType* recognizer, IntStreamType* is, ANTLR_INT32 s);

	template<typename SuperType>
    ANTLR_INT32	predict(CtxType* ctx, RecognizerType* recognizer, IntStreamType* is, SuperType& super);
	
private:
	void noViableAlt(RecognizerType* rec, ANTLR_UINT32	s);
};

}

#include "antlr3cyclicdfa.inl"

#endif
