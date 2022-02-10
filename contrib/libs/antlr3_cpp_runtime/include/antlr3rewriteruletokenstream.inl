namespace antlr3 {

template<class ImplTraits>
RewriteRuleTokenStream<ImplTraits>::RewriteRuleTokenStream(TreeAdaptorType* adaptor, const char* /*description*/)
	: m_adaptor(adaptor)
	, m_elements()
	, m_dirty(false)
{
	m_cursor = m_elements.begin();
}

template<class ImplTraits>
RewriteRuleTokenStream<ImplTraits>::RewriteRuleTokenStream(TreeAdaptorType* adaptor, const char* /*description*/,
	const TokenType* oneElement
	)
	: m_adaptor(adaptor)
	, m_elements()
	, m_dirty(false)
{
	if( oneElement != NULL )
		this->add( oneElement );
	m_cursor = m_elements.begin();
}

template<class ImplTraits>
RewriteRuleTokenStream<ImplTraits>::RewriteRuleTokenStream(TreeAdaptorType* adaptor, const char* /*description*/,
	const ElementsType& elements
	)
	: m_adaptor(adaptor)
	, m_elements(elements)
	, m_dirty(false)
{
	m_cursor = m_elements.begin();
}

template<class ImplTraits>
void RewriteRuleTokenStream<ImplTraits>::reset()
{
	m_cursor = m_elements.begin();
	m_dirty = true;
}

template<class ImplTraits>
void RewriteRuleTokenStream<ImplTraits>::add(const ElementType* el)
{
	if ( el == NULL)
		return;
	m_elements.push_back(el);
	m_cursor = m_elements.begin();
}

template<class ImplTraits>
typename RewriteRuleTokenStream<ImplTraits>::ElementsType::iterator
RewriteRuleTokenStream<ImplTraits>::_next()
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
			//return this->toTree(m_singleElement);
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
typename RewriteRuleTokenStream<ImplTraits>::ElementType
RewriteRuleTokenStream<ImplTraits>::nextTree()
{
	ANTLR_UINT32 n = this->size();
	if ( m_dirty || ( (m_cursor >=n) && (n==1)) )
	{
		// if out of elements and size is 1, dup
		typename ElementsType::iterator el = this->_next();
		return this->dup(*el);
	}

	// test size above then fetch
	typename ElementsType::iterator el = this->_next();
	return *el;
}

/*
template<class ImplTraits, class SuperType>
typename RewriteRuleTokenStream<ImplTraits, SuperType>::TokenType*
RewriteRuleTokenStream<ImplTraits, SuperType>::nextToken()
{
	return this->_next();
}

template<class ImplTraits, class SuperType>
typename RewriteRuleTokenStream<ImplTraits, SuperType>::TokenType*
RewriteRuleTokenStream<ImplTraits, SuperType>::next()
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
typename RewriteRuleTokenStream<ImplTraits>::ElementType*
RewriteRuleTokenStream<ImplTraits>::dup( ElementType* element)
{
	return dupImpl(element);
}

template<class ImplTraits>
typename RewriteRuleTokenStream<ImplTraits>::ElementType*
RewriteRuleTokenStream<ImplTraits>::dupImpl( typename ImplTraits::CommonTokenType* /*element*/)
{
	return NULL; // TODO throw here
}

template<class ImplTraits>
typename RewriteRuleTokenStream<ImplTraits>::ElementType*
RewriteRuleTokenStream<ImplTraits>::dupImpl( typename ImplTraits::TreeTypePtr element)
{
	return m_adaptor->dupTree(element);
}

template<class ImplTraits>
typename RewriteRuleTokenStream<ImplTraits>::TreeTypePtr
RewriteRuleTokenStream<ImplTraits>::toTree(const ElementType* element)
{
	return m_adaptor->create(element);
}

template<class ImplTraits>
bool
RewriteRuleTokenStream<ImplTraits>::hasNext()
{
	return m_cursor != m_elements.end();
}

template<class ImplTraits >
typename RewriteRuleTokenStream<ImplTraits>::TreeTypePtr
RewriteRuleTokenStream<ImplTraits>::nextNode()
{
	const TokenType *Token = this->nextToken();
	return m_adaptor->create(Token);
}

/// Number of elements available in the stream
///
template<class ImplTraits>
ANTLR_UINT32 RewriteRuleTokenStream<ImplTraits>::size()
{
	return (ANTLR_UINT32)(m_elements.size());
}

template<class ImplTraits>
typename RewriteRuleTokenStream<ImplTraits>::StringType
RewriteRuleTokenStream<ImplTraits>::getDescription()
{
	if ( m_elementDescription.empty() )
	{
		m_elementDescription = "<unknown source>";
	}
	return  m_elementDescription;
}

template<class ImplTraits>
RewriteRuleTokenStream<ImplTraits>::~RewriteRuleTokenStream()
{
    // Before placing the stream back in the pool, we
	// need to clear any vector it has. This is so any
	// free pointers that are associated with the
	// entries are called. However, if this particular function is called
    // then we know that the entries in the stream are definitely
    // tree nodes. Hence we check to see if any of them were nilNodes as
    // if they were, we can reuse them.
	//
	// We have some elements to traverse
	//
	for (ANTLR_UINT32 i = 0; i < m_elements.size(); i++)
	{
		const ElementType *tree = m_elements.at(i);
		//if  ( (tree != NULL) && tree->isNilNode() )
		{
			// Had to remove this for now, check is not comprehensive enough
			// tree->reuse(tree);
		}
	}
	m_elements.clear();
}

template<class ImplTraits>
const typename RewriteRuleTokenStream<ImplTraits>::TokenType*
RewriteRuleTokenStream<ImplTraits>::nextToken()
{
	auto retval = this->_next();
	if (retval == m_elements.end())
		return NULL;
	else
		return *retval;
}

}
