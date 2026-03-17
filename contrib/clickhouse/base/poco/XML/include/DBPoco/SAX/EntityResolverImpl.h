//
// EntityResolverImpl.h
//
// Library: XML
// Package: SAX
// Module:  SAX
//
// An implementation of EntityResolver.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_SAX_EntityResolverImpl_INCLUDED
#define DB_SAX_EntityResolverImpl_INCLUDED


#include "DBPoco/SAX/EntityResolver.h"
#include "DBPoco/URIStreamOpener.h"
#include "DBPoco/XML/XML.h"
#include "DBPoco/XML/XMLString.h"


namespace DBPoco
{
namespace XML
{


    class XML_API EntityResolverImpl : public EntityResolver
    /// A default implementation of the EntityResolver interface.
    ///
    /// The system ID is first interpreted as an URI and the
    /// URIStreamOpener is used to create and open an istream
    /// for an InputSource.
    ///
    /// If the system ID is not a valid URI, it is
    /// interpreted as a filesystem path and a DBPoco::FileInputStream
    /// is opened for it.
    {
    public:
        EntityResolverImpl();
        /// Creates an EntityResolverImpl that uses the default
        /// URIStreamOpener.

        EntityResolverImpl(const DBPoco::URIStreamOpener & opener);
        /// Creates an EntityResolverImpl that uses the given
        /// URIStreamOpener.

        ~EntityResolverImpl();
        /// Destroys the EntityResolverImpl.

        InputSource * resolveEntity(const XMLString * publicId, const XMLString & systemId);
        /// Tries to use the URIStreamOpener to create and open an istream
        /// for the given systemId, which is interpreted as an URI.
        ///
        /// If the systemId is not a valid URI, it is interpreted as
        /// a local filesystem path and a DBPoco::FileInputStream is opened for it.

        void releaseInputSource(InputSource * pSource);
        /// Deletes the InputSource's stream.

    protected:
        std::istream * resolveSystemId(const XMLString & systemId);

    private:
        EntityResolverImpl(const EntityResolverImpl &);
        EntityResolverImpl & operator=(const EntityResolverImpl &);

        const DBPoco::URIStreamOpener & _opener;
    };


}
} // namespace DBPoco::XML


#endif // DB_SAX_EntityResolverImpl_INCLUDED
