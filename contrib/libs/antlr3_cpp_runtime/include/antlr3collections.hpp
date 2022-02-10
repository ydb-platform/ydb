#ifndef	ANTLR3COLLECTIONS_HPP
#define	ANTLR3COLLECTIONS_HPP

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

/* -------------- TRIE Interfaces ---------------- */

/** Structure that holds the payload entry in an ANTLR3_INT_TRIE or ANTLR3_STRING_TRIE
 */
template< class ImplTraits, class DataType >
class TrieEntry : public ImplTraits::AllocPolicyType
{
public:
	typedef typename ImplTraits::AllocPolicyType AllocPolicy;

private:
	DataType			m_data;
	TrieEntry*			m_next;	    /* Allows duplicate entries for same key in insertion order	*/

public:
	TrieEntry(const DataType& data, TrieEntry* next);
	DataType& get_data();
	const DataType& get_data() const;
	TrieEntry* get_next() const;
	void set_next( TrieEntry* next );
};

/** Structure that defines an element/node in an ANTLR_INT_TRIE
 */
template< class ImplTraits, class DataType >
class IntTrieNode : public ImplTraits::AllocPolicyType
{
public:
	typedef TrieEntry<ImplTraits, DataType> TrieEntryType;
	typedef TrieEntryType BucketsType;
	
private:
    ANTLR_UINT32	m_bitNum;	/**< This is the left/right bit index for traversal along the nodes				*/
    ANTLR_INTKEY	m_key;		/**< This is the actual key that the entry represents if it is a terminal node  */
    BucketsType*	m_buckets;	/**< This is the data bucket(s) that the key indexes, which may be NULL			*/
    IntTrieNode*	m_leftN;	/**< Pointer to the left node from here when sKey & bitNum = 0					*/
    IntTrieNode*	m_rightN;	/**< Pointer to the right node from here when sKey & bitNum, = 1				*/

public:
	IntTrieNode();
	~IntTrieNode();

	ANTLR_UINT32 get_bitNum() const;
	ANTLR_INTKEY get_key() const;
	BucketsType* get_buckets() const;
	IntTrieNode* get_leftN() const;
	IntTrieNode* get_rightN() const;
	void  set_bitNum( ANTLR_UINT32 bitNum );
	void  set_key( ANTLR_INTKEY key );
	void  set_buckets( BucketsType* buckets );
	void  set_leftN( IntTrieNode* leftN );
	void  set_rightN( IntTrieNode* rightN );
};
  
/** Structure that defines an ANTLR3_INT_TRIE. For this particular implementation,
 *  as you might expect, the key is turned into a "string" by looking at bit(key, depth)
 *  of the integer key. Using 64 bit keys gives us a depth limit of 64 (or bit 0..63)
 *  and potentially a huge trie. This is the algorithm for a Patricia Trie.
 *  Note also that this trie [can] accept multiple entries for the same key and is
 *  therefore a kind of elastic bucket patricia trie.
 *
 *  If you find this code useful, please feel free to 'steal' it for any purpose
 *  as covered by the BSD license under which ANTLR is issued. You can cut the code
 *  but as the ANTLR library is only about 50K (Windows Vista), you might find it 
 *  easier to just link the library. Please keep all comments and licenses and so on
 *  in any version of this you create of course.
 *
 *  Jim Idle.
 *  
 */
class IntTrieBase
{
public:
	static const ANTLR_UINT8* get_bitIndex();
	static const ANTLR_UINT64* get_bitMask();
};
 
template< class ImplTraits, class DataType >
class IntTrie : public ImplTraits::AllocPolicyType, public IntTrieBase
{
public:
	typedef TrieEntry<ImplTraits, DataType> TrieEntryType;
	typedef IntTrieNode<ImplTraits, DataType> IntTrieNodeType;
	
private:
    IntTrieNodeType*	m_root;			/* Root node of this integer trie					*/
    IntTrieNodeType*	m_current;		/* Used to traverse the TRIE with the next() method	*/
    ANTLR_UINT32	m_count;			/* Current entry count								*/
    bool			m_allowDups;		/* Whether this trie accepts duplicate keys			*/

public:
	/* INT TRIE Implementation of depth 64 bits, being the number of bits
	 * in a 64 bit integer. 
	 */
    IntTrie( ANTLR_UINT32 depth );

	/** Search the int Trie and return a pointer to the first bucket indexed
	 *  by the key if it is contained in the trie, otherwise NULL.
	 */
    TrieEntryType*	get( ANTLR_INTKEY key);
    bool		del( ANTLR_INTKEY key);

	/** Add an entry into the INT trie.
	 *  Basically we descend the trie as we do when searching it, which will
	 *  locate the only node in the trie that can be reached by the bit pattern of the
	 *  key. If the key is actually at that node, then if the trie accepts duplicates
	 *  we add the supplied data in a new chained bucket to that data node. If it does
	 *  not accept duplicates then we merely return FALSE in case the caller wants to know
	 *  whether the key was already in the trie.
	 *  If the node we locate is not the key we are looking to add, then we insert a new node
	 *  into the trie with a bit index of the leftmost differing bit and the left or right 
	 *  node pointing to itself or the data node we are inserting 'before'. 
	 */
    bool		add( ANTLR_INTKEY key, const DataType& data );
    ~IntTrie();
};

/**
 * A topological sort system that given a set of dependencies of a node m on node n,
 * can sort them in dependency order. This is a generally useful utility object
 * that does not care what the things are it is sorting. Generally the set
 * to be sorted will be numeric indexes into some other structure such as an ANTLR3_VECTOR.
 * I have provided a sort method that given ANTLR3_VECTOR as an input will sort
 * the vector entries in place, as well as a sort method that just returns an
 * array of the sorted noded indexes, in case you are not sorting ANTLR3_VECTORS but
 * some set of your own device.
 *
 * Of the two main algorithms that could be used, I chose to use the depth first
 * search for unvisited nodes as a) This runs in linear time, and b) it is what
 * we used in the ANTLR Tool to perform a topological sort of the input grammar files
 * based on their dependencies.
 */
template<class ImplTraits>
class Topo : public ImplTraits::AllocPolicyType
{
public:
	typedef typename ImplTraits::BitsetType BitsetType;
	typedef typename ImplTraits::AllocPolicyType AllocPolicyType;

private:
    /**
     * A vector of vectors of edges, built by calling the addEdge method()
     * to indicate that node number n depends on node number m. Each entry in the vector
     * contains a bitset, which has a bit index set for each node upon which the
     * entry node depends.
     */
    BitsetType**	m_edges;

    /**
     * A vector used to build up the sorted output order. Note that
     * as the vector contains UINT32 then the maximum node index is
     * 'limited' to 2^32, as nodes should be zero based.
     */
    ANTLR_UINT32*				m_sorted;

    /**
     * A vector used to detect cycles in the edge dependecies. It is used
     * as a stack and each time we descend a node to one of its edges we
     * add the node into this stack. If we find a node that we have already
     * visited in the stack, then it means there wasa cycle such as 9->8->1->9
     * as the only way a node can be on the stack is if we are currently
     * descnding from it as we remove it from the stack as we exit from
     * descending its dependencies
     */
    ANTLR_UINT32*		m_cycle;

    /**
     * A flag that indicates the algorithm found a cycle in the edges
     * such as 9->8->1->9
     * If this flag is set after you have called one of the sort routines
     * then the detected cycle will be contained in the cycle array and
     * cycleLimit will point to the one after the last entry in the cycle.
     */
    bool				m_hasCycle;

    /**
     * A watermark used to accumulate potential cycles in the cycle array.
     * This should be zero when we are done. Check hasCycle after calling one
     * of the sort methods and if it is true then you can find the cycle
     * in cycle[0]...cycle[cycleMark-1]
     */
    ANTLR_UINT32		m_cycleMark;
    
    /**
     * One more than the largest node index that is contained in edges/sorted.
     */
    ANTLR_UINT32		m_limit;

    /**
     * The set of visited nodes as determined by a set entry in
     * the bitmap.
     */
    BitsetType*			m_visited;

public:
	Topo();
    /**
     * A method that adds an edge from one node to another. An edge
     * of n -> m indicates that node n is dependent on node m. Note that
     * while building these edges, it is perfectly OK to add nodes out of
     * sequence. So, if you have edges:
     *
     * 3 -> 0
     * 2 -> 1
     * 1 -> 3
     *
     * The you can add them in that order and so add node 3 before nodes 2 and 1
     *
     */
    void  addEdge(ANTLR_UINT32 edge, ANTLR_UINT32 dependency);


    /**
     * A method that returns a pointer to an array of sorted node indexes.
     * The array is sorted in topological sorted order. Note that the array
     * is only as large as the largest node index you created an edge for. This means
     * that if you had an input of 32 nodes, but that largest node with an edge
     * was 16, then the returned array will be the sorted order of the first 16
     * nodes and the last 16 nodes of your array are basically fine as they are
     * as they had no dependencies and do not need any particular sort order.
     *
     * NB: If the structure that contains the array is freed, then the sorted
     * array will be freed too so you should use the value of limit to
     * make a long term copy of this array if you do not want to keep the topo
     * structure around as well.
     */
    ANTLR_UINT32*  sortToArray();

    /** 
     * A method that sorts the supplied ANTLR3_VECTOR in place based
     * on the previously supplied edge data.
     */
	template<typename DataType>
    void   sortVector( typename ImplTraits::template VectorType<DataType>& v);

	void   DFS(ANTLR_UINT32 node);

    /**
     *  A method to free this structure and any associated memory.
     */
	~Topo();
};

}

#include "antlr3collections.inl"
    
#endif


