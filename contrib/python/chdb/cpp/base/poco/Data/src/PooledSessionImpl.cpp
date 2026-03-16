//
// PooledSessionImpl.cpp
//
// Library: Data
// Package: SessionPooling
// Module:  PooledSessionImpl
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/Data/PooledSessionImpl.h"
#include "CHDBPoco/Data/DataException.h"
#include "CHDBPoco/Data/SessionPool.h"


namespace CHDBPoco {
namespace Data {


PooledSessionImpl::PooledSessionImpl(PooledSessionHolder* pHolder):
	SessionImpl(pHolder->session()->connectionString(),
		pHolder->session()->getLoginTimeout()),
	_pHolder(pHolder, true)
{
}


PooledSessionImpl::~PooledSessionImpl()
{
	try
	{
		close();
	}
	catch (...)
	{
		CHDB_poco_unexpected();
	}
}


StatementImpl* PooledSessionImpl::createStatementImpl()
{
	return access()->createStatementImpl();
}


void PooledSessionImpl::begin()
{
	return access()->begin();
}


void PooledSessionImpl::commit()
{
	return access()->commit();
}


bool PooledSessionImpl::isConnected()
{
	return access()->isConnected();
}


void PooledSessionImpl::setConnectionTimeout(std::size_t timeout)
{
	return access()->setConnectionTimeout(timeout);
}


std::size_t PooledSessionImpl::getConnectionTimeout()
{
	return access()->getConnectionTimeout();
}


bool PooledSessionImpl::canTransact()
{
	return access()->canTransact();
}


bool PooledSessionImpl::isTransaction()
{
	return access()->isTransaction();
}


void PooledSessionImpl::setTransactionIsolation(CHDBPoco::UInt32 ti)
{
	access()->setTransactionIsolation(ti);
}


CHDBPoco::UInt32 PooledSessionImpl::getTransactionIsolation()
{
	return access()->getTransactionIsolation();
}


bool PooledSessionImpl::hasTransactionIsolation(CHDBPoco::UInt32 ti)
{
	return access()->hasTransactionIsolation(ti);
}


bool PooledSessionImpl::isTransactionIsolation(CHDBPoco::UInt32 ti)
{
	return access()->isTransactionIsolation(ti);
}


void PooledSessionImpl::rollback()
{
	return access()->rollback();
}


void PooledSessionImpl::open(const std::string& connect)
{
	access()->open(connect);
}


void PooledSessionImpl::close()
{
	if (_pHolder)
	{
		if (isTransaction())
		{
			try
			{
				rollback();
			}
			catch (...)
			{
				// Something's wrong with the session. Get rid of it.
				access()->close();
			}
		}
		_pHolder->owner().putBack(_pHolder);
		_pHolder = 0;
	}
}


const std::string& PooledSessionImpl::connectorName() const
{
	return access()->connectorName();
}


void PooledSessionImpl::setFeature(const std::string& name, bool state)	
{
	access()->setFeature(name, state);
}


bool PooledSessionImpl::getFeature(const std::string& name)
{
	return access()->getFeature(name);
}


void PooledSessionImpl::setProperty(const std::string& name, const CHDBPoco::Any& value)
{
	access()->setProperty(name, value);
}


CHDBPoco::Any PooledSessionImpl::getProperty(const std::string& name)
{
	return access()->getProperty(name);
}


SessionImpl* PooledSessionImpl::access() const
{
	if (_pHolder)
	{
		_pHolder->access();
		return impl();
	}
	else throw SessionUnavailableException();
}


} } // namespace CHDBPoco::Data
