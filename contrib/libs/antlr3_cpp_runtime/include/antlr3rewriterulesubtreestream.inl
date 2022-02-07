namespace antlr3 {

template<class ImplTraits>
RewriteRuleSubtreeStream<ImplTraits>::RewriteRuleSubtreeStream(TreeAdaptorType* adaptor, const char* description)
	: m_adaptor(adaptor)
	, m_elementDescription(description)
	, m_dirty(false)
{
	m_cursor = m_elements.begin();
}

template<class ImplTraits>
RewriteRuleSubtreeStream<ImplTraits>::RewriteRuleSubtreeStream(TreeAdaptorType* adaptor, const char* description,
	TreeType* oneElement
	)
	: m_adaptor(adaptor)
	, m_elementDescription(description)
	, m_dirty(false)
{
	if( oneElement != NULL )
	{
		auto tree_clone = this->dup(oneElement);
		this->add( tree_clone );
	}
	m_cursor = m_elements.begin();
}

template<class ImplTraits>
RewriteRuleSubtreeStream<ImplTraits>::RewriteRuleSubtreeStream(TreeAdaptorType* adaptor, const char* description,
	TreeTypePtr& oneElement
	)
	: m_adaptor(adaptor)
	, m_elementDescription(description)
	, m_dirty(false)
{
	if( oneElement != NULL )
		this->add( oneElement );
	m_cursor = m_elements.begin();
}

template<class ImplTraits>
RewriteRuleSubtreeStream<ImplTraits>::RewriteRuleSubtreeStream(TreeAdaptorType* adaptor, const char* description,
	const ElementsType& elements
	)
	: m_adaptor(adaptor)
	, m_elementDescription(description)
	, m_dirty(false)
	, m_elements(elements)
{
	m_cursor = m_elements.begin();
}

template<class ImplTraits>
void
RewriteRuleSubtreeStream<ImplTraits>::reset()
{
	m_cursor = m_elements.begin();
	m_dirty = true;
}

template<class ImplTraits>
void
RewriteRuleSubtreeStream<ImplTraits>::add(TreeTypePtr& el)
{
	if ( el == NULL )
		return;

	m_elements.push_back(std::move(el));
	m_cursor = m_elements.begin();
}

template<class ImplTraits>
typename RewriteRuleSubtreeStream<ImplTraits>::ElementsType::iterator
RewriteRuleSubtreeStream<ImplTraits>::_next()
{
	if (m_elements.empty())
	{
		// This means that the stream is empty
		// Caller must cope with this (TODO throw RewriteEmptyStreamException)
		return m_elements.end();
	}

	if (m_dirty || m_cursor == m_elements.end())
	{
		if( m_elements.size() == 1)
		{
			// Special case when size is single element, it will just dup a lot
			return m_elements.begin();
		}

		// Out of elements and the size is not 1, so we cannot assume
		// that we just duplicate the entry n times (such as ID ent+ -> ^(ID ent)+)
		// This means we ran out of elements earlier than was expected.
		//
		return m_elements.end();	// Caller must cope with this (TODO throw RewriteEmptyStreamException)
	}

	// More than just a single element so we extract it from the
	// vector.
	return m_cursor++;
}

template<class ImplTraits>
typename RewriteRuleSubtreeStream<ImplTraits>::TreeTypePtr
RewriteRuleSubtreeStream<ImplTraits>::nextTree()
{
	if ( m_dirty || ( m_cursor == m_elements.end() && m_elements.size() == 1 ))
	{
		// if out of elements and size is 1, dup
		typename ElementsType::iterator el = this->_next();
		return this->dup(*el);
	}

	// test size above then fetch
	typename ElementsType::iterator el = this->_next();
	return std::move(*el);
}

/*
template<class ImplTraits, class SuperType>
typename RewriteRuleSubtreeStream<ImplTraits, SuperType>::TokenType*
RewriteRuleSubtreeStream<ImplTraits, SuperType>::nextToken()
{
	return this->_next();
}

template<class ImplTraits, class SuperType>
typename RewriteRuleSubtreeStream<ImplTraits, SuperType>::TokenType*
RewriteRuleSubtreeStream<ImplTraits, SuperType>::next()
{
	ANTLR_UINT32   s;
	s = this->size();
	if ( (m_cursor >= s) && (s == 1) )
	{
		TreeTypePtr el;
		el = this->_next();
		return	this->dup(el);
	}
	return this->_next();
}

*/

template<class ImplTraits>
typename RewriteRuleSubtreeStream<ImplTraits>::TreeTypePtr
RewriteRuleSubtreeStream<ImplTraits>::dup(const TreeTypePtr& element)
{
	return this->dupTree(element);
}

template<class ImplTraits>
typename RewriteRuleSubtreeStream<ImplTraits>::TreeTypePtr
RewriteRuleSubtreeStream<ImplTraits>::dup(const TreeType* element)
{
	return std::move(this->dupTree(element));
}

template<class ImplTraits>
typename RewriteRuleSubtreeStream<ImplTraits>::TreeTypePtr
RewriteRuleSubtreeStream<ImplTraits>::dupTree(const TreeTypePtr& element)
{
	return std::move(m_adaptor->dupTree(element));
}

template<class ImplTraits>
typename RewriteRuleSubtreeStream<ImplTraits>::TreeTypePtr
RewriteRuleSubtreeStream<ImplTraits>::dupTree(const TreeType* element)
{
	return std::move(m_adaptor->dupTree(element));
}

template<class ImplTraits>
typename RewriteRuleSubtreeStream<ImplTraits>::ElementType*
RewriteRuleSubtreeStream<ImplTraits>::toTree( ElementType* element)
{
	return element;
}

template<class ImplTraits>
bool RewriteRuleSubtreeStream<ImplTraits>::hasNext()
{
	return m_cursor != m_elements.end();
}

/// Number of elements available in the stream
///
template<class ImplTraits>
ANTLR_UINT32 RewriteRuleSubtreeStream<ImplTraits>::size()
{
	return (ANTLR_UINT32)(m_elements.size());
}

template<class ImplTraits>
typename RewriteRuleSubtreeStream<ImplTraits>::StringType
RewriteRuleSubtreeStream<ImplTraits>::getDescription()
{
	if ( m_elementDescription.empty() )
	{
		m_elementDescription = "<unknown source>";
	}
	return  m_elementDescription;
}

template<class ImplTraits>
RewriteRuleSubtreeStream<ImplTraits>::~RewriteRuleSubtreeStream()
{
    // Before placing the stream back in the pool, we
	// need to clear any vector it has.
	m_elements.clear();
}

template<class ImplTraits>
typename RewriteRuleSubtreeStream<ImplTraits>::TreeTypePtr
RewriteRuleSubtreeStream<ImplTraits>::nextNode()
{
	//System.out.println("nextNode: elements="+elements+", singleElement="+((Tree)singleElement).toStringTree());
	//ANTLR_UINT32 n = this->size();
	if (m_dirty || (m_cursor == m_elements.end() && m_elements.size() == 1)) {
		// if out of elements and size is 1, dup (at most a single node
		// since this is for making root nodes).
		typename ElementsType::iterator el = this->_next();
		return m_adaptor->dupNode(*el);
	}

	typename ElementsType::iterator el = this->_next();
	//while (m_adaptor->isNilNode(el) && m_adaptor->getChildCount(el) == 1)
	//	tree = m_adaptor->getChild(tree, 0);
	TreeTypePtr& node = leftestNode(*el);
	//System.out.println("_next="+((Tree)tree).toStringTree());
	return m_adaptor->dupNode(node); // dup just the root (want node here)
}

template<class ImplTraits>
ANTLR_INLINE
typename RewriteRuleSubtreeStream<ImplTraits>::TreeTypePtr&
RewriteRuleSubtreeStream<ImplTraits>::leftestNode(TreeTypePtr& node) const
{
	if(m_adaptor->isNilNode(node) && m_adaptor->getChildCount(node) == 1)
		return leftestNode(node->getChild(0));
	else
		return node;
}

}
