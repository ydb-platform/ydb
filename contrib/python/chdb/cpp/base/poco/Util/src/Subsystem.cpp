//
// Subsystem.cpp
//
// Library: Util
// Package: Application
// Module:  Subsystem
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/Util/Subsystem.h"


namespace CHDBPoco {
namespace Util {


Subsystem::Subsystem()
{
}


Subsystem::~Subsystem()
{
}


void Subsystem::reinitialize(Application& app)
{
	uninitialize();
	initialize(app);
}


void Subsystem::defineOptions(OptionSet& options)
{
}


} } // namespace CHDBPoco::Util
