//
// AbstractPreparation.h
//
// Library: Data
// Package: DataCore
// Module:  AbstractPreparation
//
// Definition of the AbstractPreparation class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CHDB_Data_AbstractPreparation_INCLUDED
#define CHDB_Data_AbstractPreparation_INCLUDED


#include <cstddef>
#include "CHDBPoco/Data/AbstractPreparator.h"
#include "CHDBPoco/Data/Data.h"
#include "CHDBPoco/SharedPtr.h"


namespace CHDBPoco
{
namespace Data
{


    class Data_API AbstractPreparation
    /// Interface for calling the appropriate AbstractPreparator method
    {
    public:
        typedef SharedPtr<AbstractPreparation> Ptr;
        typedef AbstractPreparator::Ptr PreparatorPtr;

        AbstractPreparation(PreparatorPtr pPreparator);
        /// Creates the AbstractPreparation.

        virtual ~AbstractPreparation();
        /// Destroys the AbstractPreparation.

        virtual void prepare() = 0;
        /// Prepares data.

    protected:
        AbstractPreparation();
        AbstractPreparation(const AbstractPreparation &);
        AbstractPreparation & operator=(const AbstractPreparation &);

        PreparatorPtr preparation();
        /// Returns the preparation object

        PreparatorPtr _pPreparator;
    };


    //
    // inlines
    //
    inline AbstractPreparation::PreparatorPtr AbstractPreparation::preparation()
    {
        return _pPreparator;
    }


}
} // namespace CHDBPoco::Data


#endif // CHDB_Data_AbstractPreparation_INCLUDED
