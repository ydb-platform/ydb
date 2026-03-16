//
// NullPartHandler.h
//
// Library: Net
// Package: Messages
// Module:  NullPartHandler
//
// Definition of the NullPartHandler class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CHDB_Net_NullPartHandler_INCLUDED
#define CHDB_Net_NullPartHandler_INCLUDED


#include "CHDBPoco/Net/Net.h"
#include "CHDBPoco/Net/PartHandler.h"


namespace CHDBPoco
{
namespace Net
{


    class Net_API NullPartHandler : public PartHandler
    /// A very special PartHandler that simply discards all data.
    {
    public:
        NullPartHandler();
        /// Creates the NullPartHandler.

        ~NullPartHandler();
        /// Destroys the NullPartHandler.

        void handlePart(const MessageHeader & header, std::istream & stream);
        /// Reads and discards all data from the stream.
    };


}
} // namespace CHDBPoco::Net


#endif // CHDB_Net_NullPartHandler_INCLUDED
