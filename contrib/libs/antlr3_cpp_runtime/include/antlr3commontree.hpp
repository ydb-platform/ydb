/** Interface for an ANTLR3 common tree which is what gets
 *  passed around by the AST producing parser.
 */

#ifndef	_ANTLR3_COMMON_TREE_HPP
#define	_ANTLR3_COMMON_TREE_HPP

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

template<class ImplTraits>
class CommonTree : public ImplTraits::AllocPolicyType				   
{
public:
	typedef typename ImplTraits::AllocPolicyType AllocPolicyType;
	typedef typename ImplTraits::StringType	StringType;
	typedef typename ImplTraits::CommonTokenType CommonTokenType;
	typedef typename ImplTraits::TreeType TreeType;
	typedef typename ImplTraits::TreeTypePtr TreeTypePtr;
	//typedef CommonTree TokenType;
	typedef typename AllocPolicyType::template VectorType<TreeTypePtr> ChildrenType;
	typedef typename AllocPolicyType::template ListType<TreeTypePtr> ChildListType;
	typedef typename ImplTraits::TreeUserDataType UserDataType;
protected:
	/// The list of all the children that belong to this node. They are not part of the node
    /// as they belong to the common tree node that implements this.
    ///
    ChildrenType		m_children;


    /// Start token index that encases this tree
    ///
    ANTLR_MARKER		m_startIndex;

    /// End token that encases this tree
    ///
    ANTLR_MARKER		m_stopIndex;

    /// A single token, this is the payload for the tree
    ///
    const CommonTokenType*    m_token;

	/// Points to the node that has this node as a child.
	/// If this is NULL, then this is the root node.
	///
	CommonTree*			m_parent;

	/// What index is this particular node in the child list it
	/// belongs to?
	///
	ANTLR_INT32			m_childIndex;

public:
	CommonTree();
	CommonTree( const CommonTokenType* token );
	CommonTree( const CommonTree* token );
	CommonTree( const CommonTree& ctree );
	~CommonTree();

	const CommonTokenType* get_token() const;
	void set_token(CommonTokenType const*);

	ChildrenType& get_children();
	const ChildrenType& get_children() const;
	ANTLR_INT32	get_childIndex() const;
	TreeType* get_parent() const;

    ANTLR_MARKER   get_startIndex() const;
    void	set_startIndex(ANTLR_MARKER index);

    ANTLR_MARKER  get_stopIndex() const;
    void	set_stopIndex(ANTLR_MARKER index);

	void    set_parent( TreeType* parent);
	void    set_childIndex( ANTLR_INT32 );

	void	addChild(TreeTypePtr& child);
	/// Add all elements of the supplied list as children of this node
	///
	void	addChildren(const ChildListType& kids);

	TreeTypePtr	deleteChild(ANTLR_UINT32 i);
	/// Delete children from start to stop and replace with t even if t is
	/// a list (nil-root tree). Num of children can increase or decrease.
	/// For huge child lists, inserting children can force walking rest of
	/// children to set their child index; could be slow.
	///
	void	replaceChildren(ANTLR_INT32 startChildIndex, ANTLR_INT32 stopChildIndex, TreeTypePtr t);

	// clone itself
	CommonTree*	dupNode() const;
	// clone itself in pre-allocated storage
	CommonTree*	dupNode(void *) const;


	ANTLR_UINT32	get_charPositionInLine() const;
	ANTLR_UINT32	get_line() const;

	TreeTypePtr&	getChild(ANTLR_UINT32 i);
	ANTLR_UINT32	getChildCount() const;
	ANTLR_UINT32	getType();
	TreeTypePtr&	getFirstChildWithType(ANTLR_UINT32 type);

	StringType	getText();
	bool	isNilNode();
	void	setChild(ANTLR_UINT32 i, TreeTypePtr child);
	StringType	toStringTree();
	StringType	toString();
	void	freshenParentAndChildIndexes();
	void	freshenParentAndChildIndexes(ANTLR_UINT32 offset);
	void	freshenParentAndChildIndexesDeeply();
	void	freshenParentAndChildIndexesDeeply(ANTLR_UINT32 offset);
	// Prepare tree node to be re-used
	void	reuse();

	UserDataType UserData;
};

}

#include "antlr3commontree.inl"

#endif


