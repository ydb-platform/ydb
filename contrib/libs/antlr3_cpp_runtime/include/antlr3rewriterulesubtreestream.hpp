#ifndef	ANTLR3REWRITERULESUBTREESTREAM_HPP
#define	ANTLR3REWRITERULESUBTREESTREAM_HPP

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

/// This is an implementation of a subtree stream which is a set of trees
///  modeled as an element stream.
///
template<class ImplTraits>
class RewriteRuleSubtreeStream
{
public:
	typedef typename ImplTraits::StringType StringType;
	typedef typename ImplTraits::AllocPolicyType AllocPolicyType;
	typedef typename ImplTraits::TreeAdaptorType TreeAdaptorType;
	typedef typename ImplTraits::TreeParserType ComponentType;
	typedef typename ComponentType::StreamType StreamType;
	typedef typename ImplTraits::TreeType TreeType;
	typedef typename ImplTraits::TreeTypePtr TreeTypePtr;
	typedef TreeType TokenType;
	typedef typename ImplTraits::template RecognizerType< StreamType > RecognizerType;
	typedef typename AllocPolicyType::template VectorType< TreeTypePtr > ElementsType;
	typedef typename ImplTraits::TreeType ElementType;

	RewriteRuleSubtreeStream(TreeAdaptorType* adaptor, const char* description);
	RewriteRuleSubtreeStream(TreeAdaptorType* adaptor, const char* description, TreeType* oneElement);
	RewriteRuleSubtreeStream(TreeAdaptorType* adaptor, const char* description, TreeTypePtr& oneElement);
	RewriteRuleSubtreeStream(TreeAdaptorType* adaptor, const char* description, const ElementsType& elements);
	~RewriteRuleSubtreeStream();

    /// Reset the condition of this stream so that it appears we have
    ///  not consumed any of its elements.  Elements themselves are untouched.
    ///
    void	reset();

	TreeTypePtr nextNode();

	/// TODO copied from RewriteRuleElementStreamType
    /// Add a new pANTLR3_BASE_TREE to this stream
    ///
    void	add(TreeTypePtr& el);
    bool	hasNext();
    TreeTypePtr nextTree();
    typename ElementsType::iterator _next();
    ElementType* toTree(ElementType* el);

    /// Number of elements available in the stream
    ///
    ANTLR_UINT32	size();

    /// Returns the description string if there is one available (check for NULL).
    ///
    StringType getDescription();

protected:
	TreeTypePtr dup(const TreeTypePtr& el );
	TreeTypePtr dup(const TreeType* el );

	TreeTypePtr& leftestNode(TreeTypePtr& node) const;
private:
	/// Pointer to the tree adaptor in use for this stream
	///
    TreeAdaptorType*	m_adaptor;

	/// Cursor 0..n-1.  If singleElement!=NULL, cursor is 0 until you next(),
    /// which bumps it to 1 meaning no more elements.
    ///
    typename ElementsType::iterator m_cursor;

    /// The element or stream description; usually has name of the token or
    /// rule reference that this list tracks.  Can include rulename too, but
    /// the exception would track that info.
    ///
    StringType		m_elementDescription;

    /// The list of tokens or subtrees we are tracking
    ///
    ElementsType		m_elements;

	TreeTypePtr dupTree(const TreeTypePtr& el );
	TreeTypePtr dupTree(const TreeType* el );

	/// Once a node / subtree has been used in a stream, it must be dup'ed
	/// from then on.  Streams are reset after sub rules so that the streams
	/// can be reused in future sub rules.  So, reset must set a dirty bit.
	/// If dirty, then next() always returns a dup.
	///
	bool m_dirty;
};

}

#include "antlr3rewriterulesubtreestream.inl"

#endif
