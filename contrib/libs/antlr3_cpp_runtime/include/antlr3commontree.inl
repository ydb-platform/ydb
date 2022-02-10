namespace antlr3 {

template<class ImplTraits>
CommonTree<ImplTraits>::CommonTree()
{
	m_startIndex = -1;
	m_stopIndex  = -1;
	m_childIndex = -1;
	m_token		 = NULL;
	m_parent     = NULL;
}

template<class ImplTraits>
CommonTree<ImplTraits>::CommonTree( const CommonTree& ctree )
	:m_children( ctree.m_children)
	,UserData(ctree.UserData)
{
	m_startIndex = ctree.m_startIndex;
	m_stopIndex  = ctree.m_stopIndex;
	m_childIndex = ctree.m_childIndex;
	m_token		 = ctree.m_token;
	m_parent     = ctree.m_parent;
}

template<class ImplTraits>
CommonTree<ImplTraits>::CommonTree( const CommonTokenType* token )
{
	m_startIndex = -1;
	m_stopIndex  = -1;
	m_childIndex = -1;
	m_token		 = token;
	m_parent     = NULL;
}

template<class ImplTraits>
CommonTree<ImplTraits>::CommonTree( const CommonTree* tree )
	:UserData(tree->UserData)
{
	m_startIndex = tree->get_startIndex();
	m_stopIndex  = tree->get_stopIndex();
	m_childIndex = -1;
	m_token		 = tree->get_token();
	m_parent     = NULL;
}

template<class ImplTraits>
const typename CommonTree<ImplTraits>::CommonTokenType* CommonTree<ImplTraits>::get_token() const
{
	return m_token;
}

template<class ImplTraits>
void CommonTree<ImplTraits>::set_token(typename CommonTree<ImplTraits>::CommonTokenType const* token)
{
	m_token = token;
}

template<class ImplTraits>
typename CommonTree<ImplTraits>::ChildrenType& CommonTree<ImplTraits>::get_children()
{
	return m_children;
}

template<class ImplTraits>
const typename CommonTree<ImplTraits>::ChildrenType& CommonTree<ImplTraits>::get_children() const
{
	return m_children;
}

template<class ImplTraits>
void	CommonTree<ImplTraits>::addChild(TreeTypePtr& child)
{
	if	(child == NULL)
		return;

	ChildrenType& child_children = child->get_children();
	//ChildrenType& tree_children  = this->get_children();

	if	(child->isNilNode() == true)
	{
		if ( !child_children.empty() && child_children == m_children )
		{
			// TODO: Change to exception rather than ANTLR3_FPRINTF?
			fprintf(stderr, "ANTLR3: An attempt was made to add a child list to itself!\n");
			return;
		}

        // Add all of the children's children to this list
        //
        if ( !child_children.empty() )
        {
            if (!m_children.empty())
            {
                // Need to copy(append) the children
                for(auto i = child_children.begin(); i != child_children.end(); ++i)
                {
                    // ANTLR3 lists can be sparse, unlike Array Lists (TODO: really?)
					if ((*i) != NULL)
					{
						m_children.push_back(std::move(*i));
						// static_cast to possible subtype (if TreeType trait defined)
						TreeType* tree = static_cast<TreeType*>(this);
						m_children.back()->set_parent(tree);
						m_children.back()->set_childIndex(m_children.size() - 1);
					}
                }
            } else {
                // We are build ing the tree structure here, so we need not
                // worry about duplication of pointers as the tree node
                // factory will only clean up each node once. So we just
                // copy in the child's children pointer as the child is
                // a nil node (has not root itself).
                //
                m_children.swap( child_children );
                this->freshenParentAndChildIndexes();
            }
		}
	}
	else
	{
		// Tree we are adding is not a Nil and might have children to copy
		m_children.push_back( std::move(child) );
		// static_cast to possible subtype (if TreeType trait defined)
		TreeType* tree = static_cast<TreeType*>(this);
		m_children.back()->set_parent(tree);
		m_children.back()->set_childIndex(m_children.size() - 1);
	}
}

template<class ImplTraits>
void CommonTree<ImplTraits>::addChildren(const ChildListType& kids)
{
	for( typename ChildListType::const_iterator iter = kids.begin();
		 iter != kids.end(); ++iter )
	{
		this->addChild( *iter );
	}
}

template<class ImplTraits>
typename CommonTree<ImplTraits>::TreeTypePtr	CommonTree<ImplTraits>::deleteChild(ANTLR_UINT32 i)
{
	if( m_children.empty() )
		return	NULL;
	TreeTypePtr killed = m_children.erase( m_children.begin() + i);
	this->freshenParentAndChildIndexes(i);
	return killed;
}

template<class ImplTraits>
void	CommonTree<ImplTraits>::replaceChildren(ANTLR_INT32 startChildIndex, ANTLR_INT32 stopChildIndex, TreeTypePtr newTree)
{

	ANTLR_INT32	numNewChildren;			// Tracking variable
	ANTLR_INT32	delta;					// Difference in new vs existing count

	if	( m_children.empty() )
	{
		fprintf(stderr, "replaceChildren call: Indexes are invalid; no children in list for %s", this->get_text().c_str() );
		// TODO throw here
		return;
	}
	// How many nodes will go away
	ANTLR_INT32 replacingHowMany		= stopChildIndex - startChildIndex + 1;
	ANTLR_INT32	replacingWithHowMany;	// How many nodes will replace them

	// Either use the existing list of children in the supplied nil node, or build a vector of the
	// tree we were given if it is not a nil node, then we treat both situations exactly the same
	//
	ChildrenType    newChildren;
	ChildrenType   &newChildrenRef(newChildren);

	if	(newTree->isNilNode())
	{
		newChildrenRef = newTree->get_children();
	} else {
		newChildrenRef.push_back(newTree);
	}

	// Initialize
	replacingWithHowMany	= newChildrenRef.size();
	numNewChildren			= newChildrenRef.size();
	delta					= replacingHowMany - replacingWithHowMany;

	// If it is the same number of nodes, then do a direct replacement
	//
	if	(delta == 0)
	{
		ANTLR_INT32 j = 0;
		for	(ANTLR_INT32 i = startChildIndex; i <= stopChildIndex; i++)
		{
			TreeType *child = newChildrenRef.at(j);
			m_children[i] = child;
			TreeType* tree = static_cast<TreeType*>(this);
			child->set_parent(tree);
			child->set_childIndex(i);
			j++;
		}
	}
	else if (delta > 0)
	{
		// Less nodes than there were before
		// reuse what we have then delete the rest
		for	(ANTLR_UINT32 j = 0; j < numNewChildren; j++)
		{
			m_children[ startChildIndex + j ] = newChildrenRef.at(j);
		}
		// We just delete the same index position until done
		ANTLR_UINT32 indexToDelete = startChildIndex + numNewChildren;
		for	(ANTLR_UINT32 j = indexToDelete; j <= stopChildIndex; j++)
		{
			m_children.erase( m_children.begin() + indexToDelete);
		}
		this->freshenParentAndChildIndexes(startChildIndex);
	}
	else
	{
		// More nodes than there were before
		// Use what we can, then start adding
		for	(ANTLR_UINT32 j = 0; j < replacingHowMany; j++)
		{
			m_children[ startChildIndex + j ] = newChildrenRef.at(j);
		}

		for	(ANTLR_UINT32 j = replacingHowMany; j < replacingWithHowMany; j++)
		{
			m_children.push_back( newChildrenRef.at(j) );
		}

		this->freshenParentAndChildIndexes(startChildIndex);
	}
}

template<class ImplTraits>
CommonTree<ImplTraits>*	CommonTree<ImplTraits>::dupNode() const
{
	return new CommonTree<ImplTraits>(this);
}

template<class ImplTraits>
CommonTree<ImplTraits>*	CommonTree<ImplTraits>::dupNode(void *p) const
{
	return new (p) CommonTree<ImplTraits>(this);
}

template<class ImplTraits>
ANTLR_UINT32	CommonTree<ImplTraits>::get_charPositionInLine() const
{
	if(m_token == NULL || (m_token->get_charPositionInLine() == 0) )
	{
		if(m_children.empty())
			return 0;
		if(m_children.front())
			return m_children.front()->get_charPositionInLine();
		return 0;
	}
	return m_token->get_charPositionInLine();
}

template<class ImplTraits>
typename CommonTree<ImplTraits>::TreeTypePtr& CommonTree<ImplTraits>::getChild(ANTLR_UINT32 i)
{
	static TreeTypePtr nul;
	if	(  m_children.empty() || i >= m_children.size() )
	{
		// TODO throw here should not happen
		return nul;
	}
	return  m_children.at(i);
}

template<class ImplTraits>
void    CommonTree<ImplTraits>::set_childIndex( ANTLR_INT32 i)
{
	m_childIndex = i;
}

template<class ImplTraits>
ANTLR_INT32	CommonTree<ImplTraits>::get_childIndex() const
{
	return m_childIndex;
}

template<class ImplTraits>
ANTLR_UINT32	CommonTree<ImplTraits>::getChildCount() const
{
	return static_cast<ANTLR_UINT32>( m_children.size() );
}

template<class ImplTraits>
typename CommonTree<ImplTraits>::TreeType* CommonTree<ImplTraits>::get_parent() const
{
	return m_parent;
}

template<class ImplTraits>
void     CommonTree<ImplTraits>::set_parent( TreeType* parent)
{
	m_parent = parent;
}

template<class ImplTraits>
ANTLR_MARKER CommonTree<ImplTraits>::get_startIndex() const
{
	if( m_startIndex==-1 && m_token!=NULL)
		return m_token->get_tokenIndex();
	return m_startIndex;
}

template<class ImplTraits>
void     CommonTree<ImplTraits>::set_startIndex( ANTLR_MARKER index)
{
	m_startIndex = index;
}

template<class ImplTraits>
ANTLR_MARKER CommonTree<ImplTraits>::get_stopIndex() const
{
	if( m_stopIndex==-1 && m_token!=NULL)
		return m_token->get_tokenIndex();
	return m_stopIndex;
}

template<class ImplTraits>
void     CommonTree<ImplTraits>::set_stopIndex( ANTLR_MARKER index)
{
	m_stopIndex = index;
}

template<class ImplTraits>
ANTLR_UINT32	CommonTree<ImplTraits>::getType()
{
	if	(m_token == NULL)
		return	CommonTokenType::TOKEN_INVALID;
	else
		return	m_token->get_type();
}

template<class ImplTraits>
typename CommonTree<ImplTraits>::TreeTypePtr& CommonTree<ImplTraits>::getFirstChildWithType(ANTLR_UINT32 type)
{
	ANTLR_UINT32   i;
	std::size_t   cs;

	TreeTypePtr	t;
	if	( !m_children.empty() )
	{
		cs	= m_children.size();
		for	(i = 0; i < cs; i++)
		{
			t = m_children[i];
			if  (t->getType() == type)
			{
				return  t;
			}
		}
	}
	return  NULL;
}

template<class ImplTraits>
ANTLR_UINT32	CommonTree<ImplTraits>::get_line() const
{
	if(m_token == NULL || m_token->get_line() == 0)
	{
		if ( m_children.empty())
			return 0;
		if ( m_children.front())
			return m_children.front()->get_line();
		return 0;
	}
	return m_token->get_line();
}

template<class ImplTraits>
typename CommonTree<ImplTraits>::StringType	CommonTree<ImplTraits>::getText()
{
	return this->toString();
}

template<class ImplTraits>
bool CommonTree<ImplTraits>::isNilNode()
{
	// This is a Nil tree if it has no payload (Token in our case)
	if(m_token == NULL)
		return true;
	else
		return false;
}

template<class ImplTraits>
void CommonTree<ImplTraits>::setChild(ANTLR_UINT32 i, TreeTypePtr child)
{
	if( child==NULL)
		return;

	if( child->isNilNode())
	{
		// TODO: throw IllegalArgumentException
		return;
	}

	if( m_children.size() <= i )
		m_children.resize(i+1);

	m_children[i] = child;
	TreeType* tree = static_cast<TreeType*>(this);
	child->set_parent(tree);
	child->set_childIndex(i);
}

template<class ImplTraits>
typename CommonTree<ImplTraits>::StringType	CommonTree<ImplTraits>::toStringTree()
{
	StringType retval;

	if( m_children.empty() )
		return	this->toString();

	/* Need a new string with nothing at all in it.
	*/
	if(this->isNilNode() == false)
	{
		retval.append("(");
		retval.append(this->toString());
		retval.append(" ");
	}

	if	( !m_children.empty())
	{
		retval.append( m_children.front()->toStringTree());
		for (auto i = std::next(m_children.begin()); i != m_children.end(); ++i)
		{
			retval.append(" ");
			retval.append((*i)->toStringTree());
		}
	}

	if	(this->isNilNode() == false)
	{
		retval.append(")");
	}
	return  retval;
}

template<class ImplTraits>
typename CommonTree<ImplTraits>::StringType	CommonTree<ImplTraits>::toString()
{
	if( this->isNilNode())
		return StringType("nil");
	return	m_token->toString();
}

template<class ImplTraits>
void	CommonTree<ImplTraits>::freshenParentAndChildIndexes()
{
	this->freshenParentAndChildIndexes(0);
}

template<class ImplTraits>
void	CommonTree<ImplTraits>::freshenParentAndChildIndexes(ANTLR_UINT32 offset)
{
//	ANTLR_UINT32 count = this->getChildCount();
	// Loop from the supplied index and set the indexes and parent
//	for	(ANTLR_UINT32 c = offset; c < count; c++)
//	{
//		TreeTypePtr child = this->getChild(c);
//		child->set_childIndex(c);
//		child->set_parent(this);
//	}
	// Loop from the supplied index and set the indexes and parent
	auto i = m_children.begin();
	int c = offset;
	if(offset)
		std::advance( i, offset );
	for(; i != m_children.end(); ++i, ++c)
	{
		(*i)->set_childIndex(c);
		TreeType* tree = static_cast<TreeType*>(this);
		(*i)->set_parent(tree);
	}
}

template<class ImplTraits>
void	CommonTree<ImplTraits>::freshenParentAndChildIndexesDeeply()
{
	this->freshenParentAndChildIndexes(0);
}

template<class ImplTraits>
void	CommonTree<ImplTraits>::freshenParentAndChildIndexesDeeply(ANTLR_UINT32 offset)
{
	ANTLR_UINT32 count = this->getChildCount();
	for (ANTLR_UINT32 c = offset; c < count; c++) {
		TreeTypePtr child = getChild(c);
		child->set_childIndex(c);
		child->set_parent(this);
		child->freshenParentAndChildIndexesDeeply();
	}
}

template<class ImplTraits>
void	CommonTree<ImplTraits>::reuse()
{
	m_startIndex = -1;
	m_stopIndex  = -1;
	m_childIndex = -1;
	m_token		 = NULL;
	m_parent     = NULL;

	ChildrenType empty;
	m_children.swap(empty);
}

template<class ImplTraits>
CommonTree<ImplTraits>::~CommonTree()
{
}

}
