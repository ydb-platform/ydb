namespace antlr3 {

template< class ImplTraits, class DataType >
ANTLR_INLINE TrieEntry<ImplTraits, DataType>::TrieEntry(const DataType& data, TrieEntry* next)
	:m_data(data)
{
	m_next = next;
}
template< class ImplTraits, class DataType >
ANTLR_INLINE DataType& TrieEntry<ImplTraits, DataType>::get_data()
{
	return m_data;
}
template< class ImplTraits, class DataType >
ANTLR_INLINE const DataType& TrieEntry<ImplTraits, DataType>::get_data() const
{
	return m_data;
}
template< class ImplTraits, class DataType >
ANTLR_INLINE TrieEntry<ImplTraits, DataType>* TrieEntry<ImplTraits, DataType>::get_next() const
{
	return m_next;
}

template< class ImplTraits, class DataType >
ANTLR_INLINE void TrieEntry<ImplTraits, DataType>::set_next( TrieEntry* next )
{
	m_next = next;
}

template< class ImplTraits, class DataType >
ANTLR_INLINE ANTLR_UINT32 IntTrieNode<ImplTraits, DataType>::get_bitNum() const
{
	return m_bitNum;
}
template< class ImplTraits, class DataType >
ANTLR_INLINE ANTLR_INTKEY IntTrieNode<ImplTraits, DataType>::get_key() const
{
	return m_key;
}
template< class ImplTraits, class DataType >
ANTLR_INLINE typename IntTrieNode<ImplTraits, DataType>::BucketsType* IntTrieNode<ImplTraits, DataType>::get_buckets() const
{
	return m_buckets;
}
template< class ImplTraits, class DataType >
ANTLR_INLINE IntTrieNode<ImplTraits, DataType>* IntTrieNode<ImplTraits, DataType>::get_leftN() const
{
	return m_leftN;
}
template< class ImplTraits, class DataType >
ANTLR_INLINE IntTrieNode<ImplTraits, DataType>* IntTrieNode<ImplTraits, DataType>::get_rightN() const
{
	return m_rightN;
}
template< class ImplTraits, class DataType >
ANTLR_INLINE void IntTrieNode<ImplTraits, DataType>::set_bitNum( ANTLR_UINT32 bitNum )
{
	m_bitNum = bitNum;
}
template< class ImplTraits, class DataType >
ANTLR_INLINE void IntTrieNode<ImplTraits, DataType>::set_key( ANTLR_INTKEY key )
{
	m_key = key;
}
template< class ImplTraits, class DataType >
ANTLR_INLINE void IntTrieNode<ImplTraits, DataType>::set_buckets( BucketsType* buckets )
{
	m_buckets = buckets;
}
template< class ImplTraits, class DataType >
ANTLR_INLINE void IntTrieNode<ImplTraits, DataType>::set_leftN( IntTrieNode* leftN )
{
	m_leftN = leftN;
}
template< class ImplTraits, class DataType >
ANTLR_INLINE void IntTrieNode<ImplTraits, DataType>::set_rightN( IntTrieNode* rightN )
{
	m_rightN = rightN;
}

ANTLR_INLINE const ANTLR_UINT8* IntTrieBase::get_bitIndex()
{
	static ANTLR_UINT8 bitIndex[256] = 
	{ 
		0,													// 0 - Just for padding
		0,													// 1
		1, 1,												// 2..3
		2, 2, 2, 2,											// 4..7
		3, 3, 3, 3, 3, 3, 3, 3,								// 8+
		4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,	    // 16+
		5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5,	    // 32+
		5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5,	    
		6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,	    // 64+
		6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
		6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
		6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 
		7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,	    // 128+
		7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
		7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
		7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
		7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
		7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 
		7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
		7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7
	};
	return bitIndex;
}

ANTLR_INLINE const ANTLR_UINT64* IntTrieBase::get_bitMask()
{
	static ANTLR_UINT64 bitMask[64] = 
	{
		0x0000000000000001ULL, 0x0000000000000002ULL, 0x0000000000000004ULL, 0x0000000000000008ULL,
		0x0000000000000010ULL, 0x0000000000000020ULL, 0x0000000000000040ULL, 0x0000000000000080ULL,
		0x0000000000000100ULL, 0x0000000000000200ULL, 0x0000000000000400ULL, 0x0000000000000800ULL,
		0x0000000000001000ULL, 0x0000000000002000ULL, 0x0000000000004000ULL, 0x0000000000008000ULL,
		0x0000000000010000ULL, 0x0000000000020000ULL, 0x0000000000040000ULL, 0x0000000000080000ULL,
		0x0000000000100000ULL, 0x0000000000200000ULL, 0x0000000000400000ULL, 0x0000000000800000ULL,
		0x0000000001000000ULL, 0x0000000002000000ULL, 0x0000000004000000ULL, 0x0000000008000000ULL,
		0x0000000010000000ULL, 0x0000000020000000ULL, 0x0000000040000000ULL, 0x0000000080000000ULL,
		0x0000000100000000ULL, 0x0000000200000000ULL, 0x0000000400000000ULL, 0x0000000800000000ULL,
		0x0000001000000000ULL, 0x0000002000000000ULL, 0x0000004000000000ULL, 0x0000008000000000ULL,
		0x0000010000000000ULL, 0x0000020000000000ULL, 0x0000040000000000ULL, 0x0000080000000000ULL,
		0x0000100000000000ULL, 0x0000200000000000ULL, 0x0000400000000000ULL, 0x0000800000000000ULL,
		0x0001000000000000ULL, 0x0002000000000000ULL, 0x0004000000000000ULL, 0x0008000000000000ULL,
		0x0010000000000000ULL, 0x0020000000000000ULL, 0x0040000000000000ULL, 0x0080000000000000ULL,
		0x0100000000000000ULL, 0x0200000000000000ULL, 0x0400000000000000ULL, 0x0800000000000000ULL,
		0x1000000000000000ULL, 0x2000000000000000ULL, 0x4000000000000000ULL, 0x8000000000000000ULL
	};

	return bitMask;
}

template< class ImplTraits, class DataType >
IntTrie<ImplTraits, DataType>::IntTrie( ANTLR_UINT32 depth )
{
	/* Now we need to allocate the root node. This makes it easier
	 * to use the tree as we don't have to do anything special 
	 * for the root node.
	 */
	m_root	= new IntTrieNodeType;

	/* Now we seed the root node with the index being the
	 * highest left most bit we want to test, which limits the
	 * keys in the trie. This is the trie 'depth'. The limit for
	 * this implementation is 63 (bits 0..63).
	 */
	m_root->set_bitNum( depth );

	/* And as we have nothing in here yet, we set both child pointers
	 * of the root node to point back to itself.
	 */
	m_root->set_leftN( m_root );
	m_root->set_rightN( m_root );
	m_count			= 0;

	/* Finally, note that the key for this root node is 0 because
	 * we use calloc() to initialise it.
	 */
	m_allowDups = false;
	m_current   = NULL;
}

template< class ImplTraits, class DataType >
IntTrie<ImplTraits, DataType>::~IntTrie()
{
    /* Descend from the root and free all the nodes
     */
    delete m_root;

    /* the nodes are all gone now, so we need only free the memory
     * for the structure itself
     */
}

template< class ImplTraits, class DataType >
typename IntTrie<ImplTraits, DataType>::TrieEntryType*	IntTrie<ImplTraits, DataType>::get( ANTLR_INTKEY key)
{
	IntTrieNodeType*    thisNode; 
	IntTrieNodeType*    nextNode; 

	if (m_count == 0)
		return NULL;	    /* Nothing in this trie yet	*/

	/* Starting at the root node in the trie, compare the bit index
	 * of the current node with its next child node (starts left from root).
	 * When the bit index of the child node is greater than the bit index of the current node
	 * then by definition (as the bit index decreases as we descent the trie)
	 * we have reached a 'backward' pointer. A backward pointer means we
	 * have reached the only node that can be reached by the bits given us so far
	 * and it must either be the key we are looking for, or if not then it
	 * means the entry was not in the trie, and we return NULL. A backward pointer
	 * points back in to the tree structure rather than down (deeper) within the
	 * tree branches.
	 */
	thisNode	= m_root;		/* Start at the root node		*/
	nextNode	= thisNode->get_leftN();	/* Examine the left node from the root	*/

	/* While we are descending the tree nodes...
	 */
	const ANTLR_UINT64* bitMask = this->get_bitMask();
	while( thisNode->get_bitNum() > nextNode->get_bitNum() )
	{
		/* Next node now becomes the new 'current' node
		 */
		thisNode    = nextNode;

		/* We now test the bit indicated by the bitmap in the next node
		 * in the key we are searching for. The new next node is the
		 * right node if that bit is set and the left node it is not.
		 */
		if (key & bitMask[nextNode->get_bitNum()])
		{
			nextNode = nextNode->get_rightN();	/* 1 is right	*/
		}
		else
		{
			nextNode = nextNode->get_leftN();		/* 0 is left	*/
		}
	}

	/* Here we have reached a node where the bitMap index is lower than
	 * its parent. This means it is pointing backward in the tree and
	 * must therefore be a terminal node, being the only point than can
	 * be reached with the bits seen so far. It is either the actual key
	 * we wanted, or if that key is not in the trie it is another key
	 * that is currently the only one that can be reached by those bits.
	 * That situation would obviously change if the key was to be added
	 * to the trie.
	 *
	 * Hence it only remains to test whether this is actually the key or not.
	 */
	if (nextNode->get_key() == key)
	{
		/* This was the key, so return the entry pointer
		 */
		return	nextNode->get_buckets();
	}
	else
	{
		return	NULL;	/* That key is not in the trie (note that we set the pointer to -1 if no payload) */
	}
}

template< class ImplTraits, class DataType >
bool	IntTrie<ImplTraits, DataType>::del( ANTLR_INTKEY /*key*/)
{
    IntTrieNodeType*   p;

    p = m_root;
    
    return false;

}

template< class ImplTraits, class DataType >
bool	IntTrie<ImplTraits, DataType>::add( ANTLR_INTKEY key, const DataType& data  )
{
	IntTrieNodeType*   thisNode;
	IntTrieNodeType*   nextNode;
	IntTrieNodeType*   entNode;
	ANTLR_UINT32	depth;
	TrieEntryType*	    newEnt;
	TrieEntryType*	    nextEnt;
	ANTLR_INTKEY		    xorKey;

	/* Cache the bit depth of this trie, which is always the highest index, 
	 * which is in the root node
	 */
	depth   = m_root->get_bitNum();

	thisNode	= m_root;		/* Start with the root node	    */
	nextNode	= m_root->get_leftN();	/* And assume we start to the left  */

	/* Now find the only node that can be currently reached by the bits in the
	 * key we are being asked to insert.
	 */
	const ANTLR_UINT64* bitMask = this->get_bitMask();
	while (thisNode->get_bitNum()  > nextNode->get_bitNum() )
	{
		/* Still descending the structure, next node becomes current.
		 */
		thisNode = nextNode;

		if (key & bitMask[nextNode->get_bitNum()])
		{
			/* Bit at the required index was 1, so travers the right node from here
			 */
			nextNode = nextNode->get_rightN();
		}
		else
		{
			/* Bit at the required index was 0, so we traverse to the left
			 */
			nextNode = nextNode->get_leftN();
		}
	}
	/* Here we have located the only node that can be reached by the
	 * bits in the requested key. It could in fact be that key or the node
	 * we need to use to insert the new key.
	 */
	if (nextNode->get_key() == key)
	{
		/* We have located an exact match, but we will only append to the bucket chain
		 * if this trie accepts duplicate keys.
		 */
		if (m_allowDups ==true)
		{
			/* Yes, we are accepting duplicates
			 */
			newEnt = new TrieEntryType(data, NULL);

			/* We want to be able to traverse the stored elements in the order that they were
			 * added as duplicate keys. We might need to revise this opinion if we end up having many duplicate keys
			 * as perhaps reverse order is just as good, so long as it is ordered.
			 */
			nextEnt = nextNode->get_buckets();
			while (nextEnt->get_next() != NULL)
			{
				nextEnt = nextEnt->get_next();    
			}
			nextEnt->set_next(newEnt);

			m_count++;
			return  true;
		}
		else
		{
			/* We found the key is already there and we are not allowed duplicates in this
			 * trie.
			 */
			return  false;
		}
	}

	/* Here we have discovered the only node that can be reached by the bits in the key
	 * but we have found that this node is not the key we need to insert. We must find the
	 * the leftmost bit by which the current key for that node and the new key we are going 
	 * to insert, differ. While this nested series of ifs may look a bit strange, experimentation
	 * showed that it allows a machine code path that works well with predicated execution
	 */
	xorKey = (key ^ nextNode->get_key() );   /* Gives 1 bits only where they differ then we find the left most 1 bit*/

	/* Most common case is a 32 bit key really
	 */
	const ANTLR_UINT8* bitIndex = this->get_bitIndex();
#ifdef	ANTLR_USE_64BIT
	if	(xorKey & 0xFFFFFFFF00000000)
	{
		if  (xorKey & 0xFFFF000000000000)
		{
			if	(xorKey & 0xFF00000000000000)
			{
				depth = 56 + bitIndex[((xorKey & 0xFF00000000000000)>>56)];
			}
			else
			{
				depth = 48 + bitIndex[((xorKey & 0x00FF000000000000)>>48)];
			}
		}
		else
		{
			if	(xorKey & 0x0000FF0000000000)
			{
				depth = 40 + bitIndex[((xorKey & 0x0000FF0000000000)>>40)];
			}
			else
			{
				depth = 32 + bitIndex[((xorKey & 0x000000FF00000000)>>32)];
			}
		}
	}
	else
#endif
	{
		if  (xorKey & 0x00000000FFFF0000)
		{
			if	(xorKey & 0x00000000FF000000)
			{
				depth = 24 + bitIndex[((xorKey & 0x00000000FF000000)>>24)];
			}
			else
			{
				depth = 16 + bitIndex[((xorKey & 0x0000000000FF0000)>>16)];
			}
		}
		else
		{
			if	(xorKey & 0x000000000000FF00)
			{
				depth = 8 + bitIndex[((xorKey & 0x0000000000000FF00)>>8)];
			}
			else
			{
				depth = bitIndex[xorKey & 0x00000000000000FF];
			}
		}
	}

    /* We have located the leftmost differing bit, indicated by the depth variable. So, we know what
     * bit index we are to insert the new entry at. There are two cases, being where the two keys
     * differ at a bit position that is not currently part of the bit testing, where they differ on a bit
     * that is currently being skipped in the indexed comparisons, and where they differ on a bit
     * that is merely lower down in the current bit search. If the bit index went bit 4, bit 2 and they differ
     * at bit 3, then we have the "skipped" bit case. But if that chain was Bit 4, Bit 2 and they differ at bit 1
     * then we have the easy bit <pun>.
     *
     * So, set up to descend the tree again, but this time looking for the insert point
     * according to whether we skip the bit that differs or not.
     */
    thisNode	= m_root;
    entNode	= m_root->get_leftN();

    /* Note the slight difference in the checks here to cover both cases
     */
    while (thisNode->get_bitNum() > entNode->get_bitNum() && entNode->get_bitNum() > depth)
    {
		/* Still descending the structure, next node becomes current.
		 */
		thisNode = entNode;

		if (key & bitMask[entNode->get_bitNum()])
		{
			/* Bit at the required index was 1, so traverse the right node from here
			 */
			entNode = entNode->get_rightN();
		}
		else
		{
			/* Bit at the required index was 0, so we traverse to the left
			 */
			entNode = entNode->get_leftN();
		}
    }

    /* We have located the correct insert point for this new key, so we need
     * to allocate our entry and insert it etc.
     */
    nextNode	= new IntTrieNodeType();

    /* Build a new entry block for the new node
     */
    newEnt = new TrieEntryType(data, NULL);

	/* Install it
     */
    nextNode->set_buckets(newEnt);
    nextNode->set_key(key);
    nextNode->set_bitNum( depth );

    /* Work out the right and left pointers for this new node, which involve
     * terminating with the current found node either right or left according
     * to whether the current index bit is 1 or 0
     */
    if (key & bitMask[depth])
    {
		nextNode->set_leftN(entNode);	    /* Terminates at previous position	*/
		nextNode->set_rightN(nextNode);	    /* Terminates with itself		*/
    }
    else
    {
		nextNode->set_rightN(entNode);	    /* Terminates at previous position	*/
		nextNode->set_leftN(nextNode);	    /* Terminates with itself		*/		
    }

    /* Finally, we need to change the pointers at the node we located
     * for inserting. If the key bit at its index is set then the right
     * pointer for that node becomes the newly created node, otherwise the left 
     * pointer does.
     */
    if (key & bitMask[thisNode->get_bitNum()] )
    {
		thisNode->set_rightN( nextNode );
    }
    else
    {
		thisNode->set_leftN(nextNode);
    }

    /* Et voila
     */
    m_count++;
    return  true;
}

template< class ImplTraits, class DataType >
IntTrieNode<ImplTraits, DataType>::IntTrieNode()
{
	m_bitNum = 0;
	m_key = 0;
	m_buckets = NULL;
	m_leftN = NULL;
	m_rightN = NULL;
}

template< class ImplTraits, class DataType >
IntTrieNode<ImplTraits, DataType>::~IntTrieNode()
{
	TrieEntryType*	thisEntry;
    TrieEntryType*	nextEntry;

    /* If this node has a left pointer that is not a back pointer
     * then recursively call to free this
     */
    if ( m_bitNum > m_leftN->get_bitNum())
    {
		/* We have a left node that needs descending, so do it.
		 */
		delete m_leftN;
    }

    /* The left nodes from here should now be dealt with, so 
     * we need to descend any right nodes that are not back pointers
     */
    if ( m_bitNum > m_rightN->get_bitNum() )
    {
		/* There are some right nodes to descend and deal with.
		 */
		delete m_rightN;
    }

    /* Now all the children are dealt with, we can destroy
     * this node too
     */
    thisEntry	= m_buckets;

    while (thisEntry != NULL)
    {
		nextEntry   = thisEntry->get_next();

		/* Now free the data for this bucket entry
		 */
		delete thisEntry;
		thisEntry = nextEntry;	    /* See if there are any more to free    */
    }

    /* The bucket entry is now gone, so we can free the memory for
     * the entry itself.
     */

    /* And that should be it for everything under this node and itself
     */
}

/**
 * Allocate and initialize a new ANTLR3 topological sorter, which can be
 * used to define edges that identify numerical node indexes that depend on other
 * numerical node indexes, which can then be sorted topologically such that
 * any node is sorted after all its dependent nodes.
 *
 * Use:
 *
 * /verbatim

  pANTLR3_TOPO topo;
  topo = antlr3NewTopo();

  if (topo == NULL) { out of memory }

  topo->addEdge(topo, 3, 0); // Node 3 depends on node 0
  topo->addEdge(topo, 0, 1); // Node - depends on node 1
  topo->sortVector(topo, myVector); // Sort the vector in place (node numbers are the vector entry numbers)

 * /verbatim
 */
template<class ImplTraits>
Topo<ImplTraits>::Topo()
{
    // Initialize variables
    //
    m_visited   = NULL;                 // Don't know how big it is yet
    m_limit     = 1;                    // No edges added yet
    m_edges     = NULL;                 // No edges added yet
    m_sorted    = NULL;                 // Nothing sorted at the start
    m_cycle     = NULL;                 // No cycles at the start
    m_cycleMark = 0;                    // No cycles at the start
    m_hasCycle  = false;         // No cycle at the start
}

// Topological sorter
//
template<class ImplTraits>
void Topo<ImplTraits>::addEdge(ANTLR_UINT32 edge, ANTLR_UINT32 dependency)
{
	ANTLR_UINT32   i;
    ANTLR_UINT32   maxEdge;
    BitsetType*  edgeDeps;

    if (edge>dependency)
    {
        maxEdge = edge;
    }
    else
    {
        maxEdge = dependency;
    }
    // We need to add an edge to says that the node indexed by 'edge' is
    // dependent on the node indexed by 'dependency'
    //

    // First see if we have enough room in the edges array to add the edge?
    //
    if ( m_edges == NULL)
    {
        // We don't have any edges yet, so create an array to hold them
        //
        m_edges = AllocPolicyType::alloc0(sizeof(BitsetType*) * (maxEdge + 1));

        // Set the limit to what we have now
        //
        m_limit = maxEdge + 1;
    }
    else if (m_limit <= maxEdge)
    {
        // WE have some edges but not enough
        //
        m_edges = AllocPolicyType::realloc(m_edges, sizeof(BitsetType*) * (maxEdge + 1));

        // Initialize the new bitmaps to ;indicate we have no edges defined yet
        //
        for (i = m_limit; i <= maxEdge; i++)
        {
            *((m_edges) + i) = NULL;
        }

        // Set the limit to what we have now
        //
        m_limit = maxEdge + 1;
    }

    // If the edge was flagged as depending on itself, then we just
    // do nothing as it means this routine was just called to add it
    // in to the list of nodes.
    //
    if  (edge == dependency)
    {
        return;
    }

    // Pick up the bit map for the requested edge
    //
    edgeDeps = *((m_edges) + edge);

    if  (edgeDeps == NULL)
    {
        // No edges are defined yet for this node
        //
        edgeDeps                = new BitsetType(0);
        *((m_edges) + edge) = edgeDeps;
    }

    // Set the bit in the bitmap that corresponds to the requested
    // dependency.
    //
    edgeDeps->add(dependency);

    // And we are all set
    //
    return;

}

/**
 * Given a starting node, descend its dependent nodes (ones that it has edges
 * to) until we find one without edges. Having found a node without edges, we have
 * discovered the bottom of a depth first search, which we can then ascend, adding
 * the nodes in order from the bottom, which gives us the dependency order.
 */
template<class ImplTraits>
void Topo<ImplTraits>::DFS(ANTLR_UINT32 node)
{
	BitsetType* edges;

    // Guard against a revisit and check for cycles
    //
    if  (m_hasCycle == true)
    {
        return; // We don't do anything else if we found a cycle
    }

    if  ( m_visited->isMember(node))
    {
        // Check to see if we found a cycle. To do this we search the
        // current cycle stack and see if we find this node already in the stack.
        //
        ANTLR_UINT32   i;

        for (i=0; i< m_cycleMark; i++)
        {
            if  ( m_cycle[i] == node)
            {
                // Stop! We found a cycle in the input, so rejig the cycle
                // stack so that it only contains the cycle and set the cycle flag
                // which will tell the caller what happened
                //
                ANTLR_UINT32 l;

                for (l = i; l < m_cycleMark; l++)
                {
                    m_cycle[l - i] = m_cycle[l];    // Move to zero base in the cycle list
                }

                // Recalculate the limit
                //
                m_cycleMark -= i;

                // Signal disaster
                //
                m_hasCycle = true;
            }
        }
        return;
    }

    // So far, no cycles have been found and we have not visited this node yet,
    // so this node needs to go into the cycle stack before we continue
    // then we will take it out of the stack once we have descended all its
    // dependencies.
    //
    m_cycle[m_cycleMark++] = node;

    // First flag that we have visited this node
    //
    m_visited->add(node);

    // Now, if this node has edges, then we want to ensure we visit
    // them all before we drop through and add this node into the sorted
    // list.
    //
    edges = *((m_edges) + node);
    if  (edges != NULL)
    {
        // We have some edges, so visit each of the edge nodes
        // that have not already been visited.
        //
        ANTLR_UINT32   numBits;	    // How many bits are in the set
        ANTLR_UINT32   i;
        ANTLR_UINT32   range;

        numBits = edges->numBits();
        range   = edges->size();   // Number of set bits

        // Stop if we exahust the bit list or have checked the
        // number of edges that this node refers to (so we don't
        // check bits at the end that cannot possibly be set).
        //
        for (i=0; i<= numBits && range > 0; i++)
        {
            if  (edges->isMember(i))
            {
                range--;        // About to check another one

                // Found an edge, make sure we visit and descend it
                //
                this->DFS(i);
            }
        }
    }

    // At this point we will have visited all the dependencies
    // of this node and they will be ordered (even if there are cycles)
    // So we just add the node into the sorted list at the
    // current index position.
    //
    m_sorted[m_limit++] = node;

    // Remove this node from the cycle list if we have not detected a cycle
    //
    if  (m_hasCycle == false)
    {
        m_cycleMark--;
    }

    return;
}

template<class ImplTraits>
ANTLR_UINT32*  Topo<ImplTraits>::sortToArray()
{
	ANTLR_UINT32 v;
    ANTLR_UINT32 oldLimit;

    // Guard against being called with no edges defined
    //
    if  (m_edges == NULL)
    {
        return 0;
    }
    // First we need a vector to populate with enough
    // entries to accomodate the sorted list and another to accomodate
    // the maximum cycle we could detect which is all nodes such as 0->1->2->3->0
    //
    m_sorted    = AllocPolicyType::alloc( m_limit * sizeof(ANTLR_UINT32) );
    m_cycle     = AllocPolicyType::alloc( m_limit * sizeof(ANTLR_UINT32));

    // Next we need an empty bitset to show whether we have visited a node
    // or not. This is the bit that gives us linear time of course as we are essentially
    // dropping through the nodes in depth first order and when we get to a node that
    // has no edges, we pop back up the stack adding the nodes we traversed in reverse
    // order.
    //
    m_visited   = new BitsetType(0);

    // Now traverse the nodes as if we were just going left to right, but
    // then descend each node unless it has already been visited.
    //
    oldLimit    = m_limit;     // Number of nodes to traverse linearly
    m_limit = 0;               // Next entry in the sorted table

    for (v = 0; v < oldLimit; v++)
    {
        // If we did not already visit this node, then descend it until we
        // get a node without edges or arrive at a node we have already visited.
        //
        if  (m_visited->isMember(v) == false)
        {
            // We have not visited this one so descend it
            //
            this->DFS(v);
        }

        // Break the loop if we detect a cycle as we have no need to go any
        // further
        //
        if  (m_hasCycle == true)
        {
            break;
        }
    }

    // Reset the limit to the number we recorded as if we hit a
    // cycle, then limit will have stopped at the node where we
    // discovered the cycle, but in order to free the edge bitmaps
    // we need to know how many we may have allocated and traverse them all.
    //
    m_limit = oldLimit;

    // Having traversed all the nodes we were given, we
    // are guaranteed to have ordered all the nodes or detected a
    // cycle.
    //
    return m_sorted;
}

template<class ImplTraits>
	template<typename DataType>
void   Topo<ImplTraits>::sortVector(  typename ImplTraits::template VectorType<DataType>& v )
{
    // To sort a vector, we first perform the
    // sort to an array, then use the results to reorder the vector
    // we are given. This is just a convenience routine that allows you to
    // sort the children of a tree node into topological order before or
    // during an AST walk. This can be useful for optimizations that require
    // dag reorders and also when the input stream defines thigns that are
    // interdependent and you want to walk the list of the generated trees
    // for those things in topological order so you can ignore the interdependencies
    // at that point.
    //
    ANTLR_UINT32 i;

    // Used as a lookup index to find the current location in the vector of
    // the vector entry that was originally at position [0], [1], [2] etc
    //
    ANTLR_UINT32*  vIndex;

    // Sort into an array, then we can use the array that is
    // stored in the topo
    //
    if  (this->sortToArray() == 0)
    {
        return;     // There were no edges
    }

    if  (m_hasCycle == true)
    {
        return;  // Do nothing if we detected a cycle
    }

    // Ensure that the vector we are sorting is at least as big as the
    // the input sequence we were adsked to sort. It does not matter if it is
    // bigger as thaat probably just means that nodes numbered higher than the
    // limit had no dependencies and so can be left alone.
    //
    if  (m_limit > v.size() )
    {
        // We can only sort the entries that we have dude! The caller is
        // responsible for ensuring the vector is the correct one and is the
        // correct size etc.
        //
        m_limit = v.size();
    }
    // We need to know the locations of each of the entries
    // in the vector as we don't want to duplicate them in a new vector. We
    // just use an indirection table to get the vector entry for a particular sequence
    // acording to where we moved it last. Then we can just swap vector entries until
    // we are done :-)
    //
    vIndex = AllocPolicyType::alloc(m_limit * sizeof(ANTLR_UINT32));

    // Start index, each vector entry is located where you think it is
    //
    for (i = 0; i < m_limit; i++)
    {
        vIndex[i] = i;
    }

    // Now we traverse the sorted array and moved the entries of
    // the vector around according to the sort order and the indirection
    // table we just created. The index telsl us where in the vector the
    // original element entry n is now located via vIndex[n].
    //
    for (i=0; i < m_limit; i++)
    {
        ANTLR_UINT32   ind;

        // If the vector entry at i is already the one that it
        // should be, then we skip moving it of course.
        //
        if  (vIndex[m_sorted[i]] == i)
        {
            continue;
        }

        // The vector entry at i, should be replaced with the
        // vector entry indicated by topo->sorted[i]. The vector entry
        // at topo->sorted[i] may have already been swapped out though, so we
        // find where it is now and move it from there to i.
        //
        ind     = vIndex[m_sorted[i]];
		std::swap( v[i], v[ind] );

        // Update our index. The element at i is now the one we wanted
        // to be sorted here and the element we swapped out is now the
        // element that was at i just before we swapped it. If you are lost now
        // don't worry about it, we are just reindexing on the fly is all.
        //
        vIndex[m_sorted[i]] = i;
        vIndex[i] = ind;
    }

    // Having traversed all the entries, we have sorted the vector in place.
    //
    AllocPolicyType::free(vIndex);
    return;
}

template<class ImplTraits>
Topo<ImplTraits>::~Topo()
{
    ANTLR_UINT32   i;

    // Free the result vector
    //
    if  (m_sorted != NULL)
    {
        AllocPolicyType::free(m_sorted);
    }

    // Free the visited map
    //
    if  (m_visited != NULL)
    {
		delete m_visited;
    }

    // Free any edgemaps
    //
    if  (m_edges != NULL)
    {
        Bitset<AllocPolicyType>* edgeList;

        for (i=0; i<m_limit; i++)
        {
            edgeList = *((m_edges) + i);
            if  (edgeList != NULL)
            {
				delete edgeList;
            }
        }

        AllocPolicyType::free( m_edges );
    }
    m_edges = NULL;
    
    // Free any cycle map
    //
    if  (m_cycle != NULL)
    {
        AllocPolicyType::free(m_cycle);
    }
}


}
