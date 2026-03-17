//
// Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
//

#ifndef PC_PYTHON_HELPERS_HPP
#define PC_PYTHON_HELPERS_HPP

#include "logging.hpp"
#include <string>

namespace sf
{

namespace py
{

class UniqueRef;

using Logger = ::sf::Logger;

/**
 * \brief: import a python module
 * \param moduleName: the name of the python module
 * \param ref: the RAII object to manage the PyObject
 * \return:
 */
void importPythonModule(const std::string& moduleName, UniqueRef& ref);

void importPythonModule(const std::string& moduleName, UniqueRef& ref,
                        const Logger& logger);

void importFromModule(const UniqueRef& moduleRef, const std::string& name,
                      UniqueRef& ref);

void importFromModule(const UniqueRef& moduleRef, const std::string& name,
                      UniqueRef& ref, const Logger& logger);

}  // namespace py
}  // namespace sf

#endif  // PC_PYTHON_HELPERS_HPP
