//
// AbstractBinding.h
//
// Library: Data
// Package: DataCore
// Module:  AbstractBinding
//
// Definition of the AbstractBinding class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CHDB_Data_AbstractBinding_INCLUDED
#define CHDB_Data_AbstractBinding_INCLUDED


#include <cstddef>
#include <deque>
#include <list>
#include <vector>
#include "CHDBPoco/Any.h"
#include "CHDBPoco/AutoPtr.h"
#include "CHDBPoco/Data/AbstractBinder.h"
#include "CHDBPoco/Data/Data.h"
#include "CHDBPoco/RefCountedObject.h"


namespace CHDBPoco
{
namespace Data
{


    class Data_API AbstractBinding
    /// AbstractBinding connects a value with a placeholder via an AbstractBinder interface.
    {
    public:
        typedef SharedPtr<AbstractBinding> Ptr;
        typedef AbstractBinder::Ptr BinderPtr;

        enum Direction
        {
            PD_IN = AbstractBinder::PD_IN,
            PD_OUT = AbstractBinder::PD_OUT,
            PD_IN_OUT = AbstractBinder::PD_IN_OUT
        };

        AbstractBinding(const std::string & name = "", Direction direction = PD_IN, CHDBPoco::UInt32 bulkSize = 0);
        /// Creates the AbstractBinding.

        virtual ~AbstractBinding();
        /// Destroys the AbstractBinding.

        void setBinder(BinderPtr pBinder);
        /// Sets the object used for binding; object does NOT take ownership of the pointer.

        BinderPtr getBinder() const;
        /// Returns the AbstractBinder used for binding data.

        virtual std::size_t numOfColumnsHandled() const = 0;
        /// Returns the number of columns that the binding handles.
        ///
        /// The trivial case will be one single column but when
        /// complex types are used this value can be larger than one.

        virtual std::size_t numOfRowsHandled() const = 0;
        /// Returns the number of rows that the binding handles.
        ///
        /// The trivial case will be one single row but
        /// for collection data types it can be larger.

        virtual bool canBind() const = 0;
        /// Returns true if we have enough data to bind

        virtual void bind(std::size_t pos) = 0;
        /// Binds a value to the given column position

        virtual void reset() = 0;
        /// Allows a binding to be reused.

        AbstractBinder::Direction getDirection() const;
        /// Returns the binding direction.

        const std::string & name() const;
        /// Returns the name for this binding.

        bool isBulk() const;
        /// Returns true if extraction is bulk.

        CHDBPoco::UInt32 bulkSize() const;
        /// Returns the size of the bulk binding.

    private:
        BinderPtr _pBinder;
        std::string _name;
        Direction _direction;
        CHDBPoco::UInt32 _bulkSize;
    };


    typedef std::vector<AbstractBinding::Ptr> AbstractBindingVec;
    typedef std::deque<AbstractBinding::Ptr> AbstractBindingDeq;
    typedef std::list<AbstractBinding::Ptr> AbstractBindingLst;


    //
    // inlines
    //
    inline AbstractBinder::Ptr AbstractBinding::getBinder() const
    {
        return _pBinder;
    }


    inline const std::string & AbstractBinding::name() const
    {
        return _name;
    }


    inline AbstractBinder::Direction AbstractBinding::getDirection() const
    {
        return (AbstractBinder::Direction)_direction;
    }


    inline bool AbstractBinding::isBulk() const
    {
        return _bulkSize > 0;
    }


    inline CHDBPoco::UInt32 AbstractBinding::bulkSize() const
    {
        return _bulkSize;
    }


}
} // namespace CHDBPoco::Data


#endif // CHDB_Data_AbstractBinding_INCLUDED
