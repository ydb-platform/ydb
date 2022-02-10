/// \file
/// Definition of the ANTLR3 common tree node stream.
///

#ifndef	_ANTLR_COMMON_TREE_NODE_STREAM__HPP
#define	_ANTLR_COMMON_TREE_NODE_STREAM__HPP

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
class CommonTreeNodeStream : public ImplTraits::TreeNodeIntStreamType
{
public:
	enum Constants
	{
		/// Token buffer initial size settings ( will auto increase)
		///
		DEFAULT_INITIAL_BUFFER_SIZE	= 100
		, INITIAL_CALL_STACK_SIZE	= 10
	};

	typedef typename ImplTraits::TreeType TreeType;
	typedef typename ImplTraits::TreeTypePtr TreeTypePtr;
	typedef TreeType UnitType;
	typedef typename ImplTraits::StringType StringType;
	typedef typename ImplTraits::StringStreamType StringStreamType;
	typedef typename ImplTraits::TreeAdaptorType TreeAdaptorType;
	typedef typename ImplTraits::TreeNodeIntStreamType IntStreamType;
	typedef typename ImplTraits::AllocPolicyType AllocPolicyType;
	typedef typename AllocPolicyType::template VectorType<TreeTypePtr>	NodesType;
	typedef typename AllocPolicyType::template VectorType< TreeWalkState<ImplTraits> > MarkersType;
	typedef typename AllocPolicyType::template StackType< ANTLR_INT32 > NodeStackType;
	typedef typename ImplTraits::TreeParserType ComponentType;
	typedef typename ImplTraits::CommonTokenType CommonTokenType;
	typedef typename ImplTraits::TreeNodeIntStreamType BaseType;

public:
    /// Dummy tree node that indicates a descent into a child
    /// tree. Initialized by a call to create a new interface.
    ///
    TreeType			m_DOWN;

    /// Dummy tree node that indicates a descent up to a parent
    /// tree. Initialized by a call to create a new interface.
    ///
    TreeType			m_UP;

    /// Dummy tree node that indicates the termination point of the
    /// tree. Initialized by a call to create a new interface.
    ///
    TreeType			m_EOF_NODE;

    /// Dummy node that is returned if we need to indicate an invalid node
    /// for any reason.
    ///
    TreeType			m_INVALID_NODE;

	/// The complete mapping from stream index to tree node.
	/// This buffer includes pointers to DOWN, UP, and EOF nodes.
	/// It is built upon ctor invocation.  The elements are type
	/// Object as we don't what the trees look like.
	///
	/// Load upon first need of the buffer so we can set token types
	/// of interest for reverseIndexing.  Slows us down a wee bit to
	/// do all of the if p==-1 testing everywhere though, though in C
	/// you won't really be able to measure this.
	///
	/// Must be freed when the tree node stream is torn down.
	///
	NodesType				m_nodes;

	/// Which tree are we navigating ?
    ///
    TreeTypePtr				m_root;

    /// Pointer to tree adaptor interface that manipulates/builds
    /// the tree.
    ///
    TreeAdaptorType*		m_adaptor;

    /// As we walk down the nodes, we must track parent nodes so we know
    /// where to go after walking the last child of a node.  When visiting
    /// a child, push current node and current index (current index
    /// is first stored in the tree node structure to avoid two stacks.
    ///
    NodeStackType			m_nodeStack;

	/// The current index into the nodes vector of the current tree
	/// we are parsing and possibly rewriting.
	///
	ANTLR_INT32			m_p;

    /// Which node are we currently visiting?
    ///
    TreeTypePtr		m_currentNode;

    /// Which node did we last visit? Used for LT(-1)
    ///
    TreeTypePtr		m_previousNode;

    /// Which child are we currently visiting?  If -1 we have not visited
    /// this node yet; next consume() request will set currentIndex to 0.
    ///
    ANTLR_INT32		m_currentChildIndex;

    /// What node index did we just consume?  i=0..n-1 for n node trees.
    /// IntStream.next is hence 1 + this value.  Size will be same.
    ///
    ANTLR_MARKER	m_absoluteNodeIndex;

    /// Buffer tree node stream for use with LT(i).  This list grows
    /// to fit new lookahead depths, but consume() wraps like a circular
    /// buffer.
    ///
    TreeTypePtr*		m_lookAhead;

    /// Number of elements available in the lookahead buffer at any point in
    ///  time. This is the current size of the array.
    ///
    ANTLR_UINT32		m_lookAheadLength;

    /// lookAhead[head] is the first symbol of lookahead, LT(1). 
    ///
    ANTLR_UINT32		m_head;

    /// Add new lookahead at lookahead[tail].  tail wraps around at the
    /// end of the lookahead buffer so tail could be less than head.
    ///
    ANTLR_UINT32		m_tail;

    /// Calls to mark() may be nested so we have to track a stack of
    /// them.  The marker is an index into this stack.  Index 0 is
    /// the first marker.  This is a List<TreeWalkState>
    ///
    MarkersType			m_markers;

	/// Indicates whether this node stream was derived from a prior
	/// node stream to be used by a rewriting tree parser for instance.
	/// If this flag is set to ANTLR_TRUE, then when this stream is
	/// closed it will not free the root tree as this tree always
	/// belongs to the origniating node stream.
	///
	bool				m_isRewriter;

    /// If set to ANTLR_TRUE then the navigation nodes UP, DOWN are
    /// duplicated rather than reused within the tree.
    ///
    bool				m_uniqueNavigationNodes;

public:
    // INTERFACE
	//
	CommonTreeNodeStream( ANTLR_UINT32 hint );
	CommonTreeNodeStream( const CommonTreeNodeStream& ctn );
	CommonTreeNodeStream( TreeTypePtr tree, ANTLR_UINT32 hint );

	void init( ANTLR_UINT32 hint );
	~CommonTreeNodeStream();

	/// Get tree node at current input pointer + i ahead where i=1 is next node.
	/// i<0 indicates nodes in the past.  So LT(-1) is previous node, but
	/// implementations are not required to provide results for k < -1.
	/// LT(0) is undefined.  For i>=n, return null.
	/// Return NULL for LT(0) and any index that results in an absolute address
	/// that is negative (beyond the start of the list).
	///
	/// This is analogous to the LT() method of the TokenStream, but this
	/// returns a tree node instead of a token.  Makes code gen identical
	/// for both parser and tree grammars. :)
	///
    TreeTypePtr	_LT(ANTLR_INT32 k);

	/// Where is this stream pulling nodes from?  This is not the name, but
	/// the object that provides node objects.
	///
    TreeTypePtr	getTreeSource();

	/// What adaptor can tell me how to interpret/navigate nodes and
	/// trees.  E.g., get text of a node.
	///
    TreeAdaptorType*	getTreeAdaptor();

	/// As we flatten the tree, we use UP, DOWN nodes to represent
	/// the tree structure.  When debugging we need unique nodes
	/// so we have to instantiate new ones.  When doing normal tree
	/// parsing, it's slow and a waste of memory to create unique
	/// navigation nodes.  Default should be false;
	///
    void  set_uniqueNavigationNodes(bool uniqueNavigationNodes);

    StringType	toString();

	/// Return the text of all nodes from start to stop, inclusive.
	/// If the stream does not buffer all the nodes then it can still
	/// walk recursively from start until stop.  You can always return
	/// null or "" too, but users should not access $ruleLabel.text in
	/// an action of course in that case.
	///
    StringType	toStringSS(TreeTypePtr start, TreeTypePtr stop);

	/// Return the text of all nodes from start to stop, inclusive, into the
	/// supplied buffer.
	/// If the stream does not buffer all the nodes then it can still
	/// walk recursively from start until stop.  You can always return
	/// null or "" too, but users should not access $ruleLabel.text in
	/// an action of course in that case.
	///
    void toStringWork(TreeTypePtr start, TreeTypePtr stop, StringType& buf);

	/// Get a tree node at an absolute index i; 0..n-1.
	/// If you don't want to buffer up nodes, then this method makes no
	/// sense for you.
	///
	TreeTypePtr	get(ANTLR_INT32 i);

	// REWRITING TREES (used by tree parser)

	/// Replace from start to stop child index of parent with t, which might
	/// be a list.  Number of children may be different
	/// after this call.  The stream is notified because it is walking the
	/// tree and might need to know you are monkeying with the underlying
	/// tree.  Also, it might be able to modify the node stream to avoid
	/// restreaming for future phases.
	///
	/// If parent is null, don't do anything; must be at root of overall tree.
	/// Can't replace whatever points to the parent externally.  Do nothing.
	///
	void replaceChildren(TreeTypePtr parent, ANTLR_INT32 startChildIndex, 
										ANTLR_INT32 stopChildIndex, TreeTypePtr t);

	TreeTypePtr LB(ANTLR_INT32 k);

	/// As we flatten the tree, we use UP, DOWN nodes to represent
	/// the tree structure.  When debugging we need unique nodes
	/// so instantiate new ones when uniqueNavigationNodes is true.
	///
    void	addNavigationNode(ANTLR_UINT32 ttype);

    TreeTypePtr	newDownNode();

	TreeTypePtr	newUpNode();

    bool	hasUniqueNavigationNodes() const;

    ANTLR_UINT32	getLookaheadSize();

	void	push(ANTLR_INT32 index);

	ANTLR_INT32	pop();

    void	reset();

	void fillBufferRoot();
	void fillBuffer(TreeTypePtr t);
	
};

/** This structure is used to save the state information in the treenodestream
 *  when walking ahead with cyclic DFA or for syntactic predicates,
 *  we need to record the state of the tree node stream.  This
 *  class wraps up the current state of the CommonTreeNodeStream.
 *  Calling mark() will push another of these on the markers stack.
 */
template<class ImplTraits>
class TreeWalkState : public ImplTraits::AllocPolicyType
{
public:
	typedef typename ImplTraits::TreeType TreeType;
	typedef typename ImplTraits::TreeTypePtr TreeTypePtr;

private:
    ANTLR_UINT32		m_currentChildIndex;
    ANTLR_MARKER		m_absoluteNodeIndex;
    TreeTypePtr		m_currentNode;
    TreeTypePtr		m_previousNode;
    ANTLR_UINT32		m_nodeStackSize;
    TreeTypePtr		m_lookAhead;
    ANTLR_UINT32		m_lookAheadLength;
    ANTLR_UINT32		m_tail;
    ANTLR_UINT32		m_head;


};

}

#include "antlr3commontreenodestream.inl"

#endif
