/** \file
 * Definition of the ANTLR3 common tree adaptor.
 */

#ifndef	_ANTLR3_COMMON_TREE_ADAPTOR_HPP
#define	_ANTLR3_COMMON_TREE_ADAPTOR_HPP

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

template <typename ImplTraits> class CommonTreeStore;

/** Helper class for unique_ptr. Implements deleter for instances of unique_ptr
    While building AST tree dangling pointers are automatically put back into pool
 */
template <typename ImplTraits>
class CommonResourcePoolManager
{
public:
	typedef typename ImplTraits::TreeType TreeType;
	CommonResourcePoolManager(CommonTreeStore<ImplTraits> * pool);
	CommonResourcePoolManager();

	~CommonResourcePoolManager();

	void operator()(TreeType* releasedResource) const;
private:
	CommonTreeStore<ImplTraits> * m_pool;
};

template <class ImplTraits>
class CommonTreeStore
{
public:
	typedef typename ImplTraits::TreeType TreeType;
	typedef typename ImplTraits::CommonTokenType CommonTokenType;
	typedef CommonResourcePoolManager<ImplTraits> ResourcePoolManagerType;
	typedef std::unique_ptr<TreeType, CommonResourcePoolManager<ImplTraits> > TreeTypePtr;
	//typedef typename ImplTraits::TreeTypePtr TreeTypePtr;

	CommonTreeStore();
	
	TreeTypePtr create();
	CommonTokenType* createToken();
	CommonTokenType* createToken(const CommonTokenType* fromToken);
	
	// Return special kind of NULL pointer wrapped into TreeTypePtr
	TreeTypePtr
	null()
	{
		return TreeTypePtr(NULL, m_manager);
	}

	std::size_t size() const
	{
	    return m_treeStore.size();
	}

	template <class T>
	bool contains(const std::vector<T> &vec, const T &value)
	{
		return std::find(vec.begin(), vec.end(), value) != vec.end();
	}
	
protected:
	template<typename> friend class CommonResourcePoolManager;
	template<typename> friend class CommonTreeAdaptor;

	void reuse(TreeType* releasedResource);

	std::vector<TreeType *> m_recycleBin;
	std::vector<std::unique_ptr<TreeType> > m_treeStore;
	std::vector<std::unique_ptr<CommonTokenType> > m_tokenStore;
	ResourcePoolManagerType m_manager;
};

template<class ImplTraits>
class CommonTreeAdaptor
	: public ImplTraits::AllocPolicyType
	, public CommonTreeStore<ImplTraits>
{
public:
	typedef typename ImplTraits::StringType StringType;
	typedef typename ImplTraits::TreeType TreeType;
	typedef typename ImplTraits::TreeTypePtr TreeTypePtr;
	typedef typename TreeType::ChildrenType ChildrenType;

	typedef	TreeType TokenType;
	typedef typename ImplTraits::CommonTokenType CommonTokenType;
	typedef typename ImplTraits::DebugEventListenerType DebuggerType;
	typedef typename ImplTraits::TokenStreamType TokenStreamType;
	typedef CommonTreeStore<ImplTraits> TreeStoreType;

public:
	//The parameter is there only to provide uniform constructor interface
	CommonTreeAdaptor(DebuggerType* dbg = nullptr);

	TreeTypePtr	nilNode();
	TreeTypePtr	dupTree( const TreeTypePtr& tree);
	TreeTypePtr	dupTree( const TreeType* tree);

	TreeTypePtr	dupNode(const TreeTypePtr& treeNode);
	TreeTypePtr	dupNode(const TreeType* treeNode);

	void	addChild( TreeTypePtr& t, TreeTypePtr& child);
	void	addChild( TreeTypePtr& t, TreeTypePtr&& child);
	void	addChildToken( TreeTypePtr& t, CommonTokenType* child);
	void	setParent( TreeTypePtr& child, TreeType* parent);
	TreeType*	getParent( TreeTypePtr& child);

	TreeTypePtr	errorNode( CommonTokenType* tnstream, const CommonTokenType* startToken, const CommonTokenType* stopToken);
	bool	isNilNode( TreeTypePtr& t);

	TreeTypePtr	becomeRoot( TreeTypePtr& newRoot, TreeTypePtr& oldRoot);
	TreeTypePtr	becomeRoot( TreeTypePtr&& newRoot, TreeTypePtr& oldRoot);
	TreeTypePtr	becomeRootToken(CommonTokenType* newRoot, TreeTypePtr& oldRoot);
	TreeTypePtr	rulePostProcessing( TreeTypePtr& root);

	TreeTypePtr create( CommonTokenType const* payload);
	TreeTypePtr create( ANTLR_UINT32 tokenType, const CommonTokenType* fromToken);
	TreeTypePtr create( ANTLR_UINT32 tokenType, const CommonTokenType* fromToken, const char* text);
	TreeTypePtr create( ANTLR_UINT32 tokenType, const CommonTokenType* fromToken, StringType const& text);
	TreeTypePtr create( ANTLR_UINT32 tokenType, const char* text);
	TreeTypePtr create( ANTLR_UINT32 tokenType, StringType const& text);
	
	CommonTokenType* createToken( ANTLR_UINT32 tokenType, const char* text);
  	CommonTokenType* createToken( ANTLR_UINT32 tokenType, StringType const& text);
	CommonTokenType* createToken( const CommonTokenType* fromToken);

	ANTLR_UINT32	getType( TreeTypePtr& t);
	StringType	getText( TreeTypePtr& t);
        
	TreeTypePtr&	getChild( TreeTypePtr& t, ANTLR_UINT32 i);
	void	setChild( TreeTypePtr& t, ANTLR_UINT32 i, TreeTypePtr& child);
	void	deleteChild( TreeTypePtr& t, ANTLR_UINT32 i);
	void	setChildIndex( TreeTypePtr& t, ANTLR_INT32 i);
	ANTLR_INT32	getChildIndex( TreeTypePtr& t);
	
	ANTLR_UINT32	getChildCount( TreeTypePtr&);
	ANTLR_UINT64	getUniqueID( TreeTypePtr&);

	CommonTokenType*	getToken( TreeTypePtr& t);

	void setTokenBoundaries( TreeTypePtr& t, const CommonTokenType* startToken, const CommonTokenType* stopToken);
	ANTLR_MARKER	getTokenStartIndex( TreeTypePtr& t);
	ANTLR_MARKER	getTokenStopIndex( TreeTypePtr& t);

	/// Produce a DOT (see graphviz freeware suite) from a base tree
	///
	StringType	makeDot( TreeTypePtr& theTree);

	/// Replace from start to stop child index of parent with t, which might
	/// be a list.  Number of children may be different
	/// after this call.
	///
	/// If parent is null, don't do anything; must be at root of overall tree.
	/// Can't replace whatever points to the parent externally.  Do nothing.
	///
	void replaceChildren( TreeTypePtr parent, ANTLR_INT32 startChildIndex,
			      ANTLR_INT32 stopChildIndex, TreeTypePtr t);

	~CommonTreeAdaptor();

protected:
	TreeTypePtr	dupTreeImpl( const TreeType* root, TreeType* parent);

	void defineDotNodes(TreeTypePtr t, const StringType& dotSpec);
	void defineDotEdges(TreeTypePtr t, const StringType& dotSpec);
};

//If someone can override the CommonTreeAdaptor at the compile time, that will be 
//inherited here. Still you can choose to override the DebugTreeAdaptor, if you wish to
//change the DebugTreeAdaptor
template<class ImplTraits>
class DebugTreeAdaptor : public ImplTraits::CommonTreeAdaptorType
{
public:
	//DebugEventListener implements functionality through virtual functions
	//the template parameter is required for pointing back at the adaptor
	typedef typename ImplTraits::DebugEventListener DebuggerType;
	typedef typename ImplTraits::TreeType TreeType;
	typedef typename ImplTraits::TreeTypePtr TreeTypePtr;
	typedef typename ImplTraits::CommonTokenType CommonTokenType;
	typedef typename ImplTraits::CommonTreeAdaptorType super;

private:
	/// If set to something other than NULL, then this structure is
	/// points to an instance of the debugger interface. In general, the
	/// debugger is only referenced internally in recovery/error operations
	/// so that it does not cause overhead by having to check this pointer
	/// in every function/method
	///
	DebuggerType*		m_debugger;

public:
	DebugTreeAdaptor( DebuggerType* debugger );
	void setDebugEventListener( DebuggerType* debugger);
	TreeTypePtr	  nilNode();
	void	addChild(TreeTypePtr& t, TreeTypePtr& child);
	void	addChildToken(TreeTypePtr& t, CommonTokenType* child);
	TreeTypePtr becomeRoot( TreeTypePtr& newRootTree, TreeTypePtr& oldRootTree );
	TreeTypePtr becomeRootToken( CommonTokenType* newRoot, TreeTypePtr& oldRoot);

	TreeTypePtr createTypeToken(ANTLR_UINT32 tokenType, CommonTokenType* fromToken);
	TreeTypePtr createTypeTokenText(ANTLR_UINT32 tokenType, CommonTokenType* fromToken, ANTLR_UINT8* text);
	TreeTypePtr createTypeText( ANTLR_UINT32 tokenType, ANTLR_UINT8* text);

	TreeTypePtr dupTree( const TreeTypePtr& tree);
	TreeTypePtr dupTree( const TreeType* tree);

	/// Sends the required debugging events for duplicating a tree
	/// to the debugger.
	///
	void simulateTreeConstruction(TreeTypePtr& tree);
};

}

#include "antlr3commontreeadaptor.inl"

#endif
