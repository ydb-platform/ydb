//
// Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
//

#include "logging.hpp"
#include "Python/Helpers.hpp"
#include <cstdio>

namespace sf
{
std::string Logger::formatString(const char *format, ...)
{
  char msg[1000] = {0};
  va_list args;
  va_start(args, format);
  vsnprintf(msg, sizeof(msg), format, args);
  va_end(args);

  return std::string(msg);
}

void Logger::setupPyLogger()
{
  py::UniqueRef pyLoggingModule;
  py::importPythonModule("snowflake.connector.snow_logging", pyLoggingModule);
  PyObject *logger =
      PyObject_CallMethod(pyLoggingModule.get(), "getSnowLogger", "s", m_name);

  m_pyLogger.reset(logger);
}

Logger::Logger(const char *name)
: m_name(name)
{
}

void Logger::log(int level, const char *path_name, const char *func_name, int line_num, const char *msg)
{
  if (m_pyLogger.get() == nullptr)
  {
    setupPyLogger();
  }

  PyObject *logger = m_pyLogger.get();
  py::UniqueRef keywords(PyDict_New());
  py::UniqueRef call_log(PyObject_GetAttrString(logger, "log"));

  // prepare keyword args for snow_logger
  PyDict_SetItemString(keywords.get(), "level", Py_BuildValue("i", level));
  PyDict_SetItemString(keywords.get(), "path_name", Py_BuildValue("s", path_name));
  PyDict_SetItemString(keywords.get(), "func_name", Py_BuildValue("s", func_name));
  PyDict_SetItemString(keywords.get(), "line_num", Py_BuildValue("i", line_num));
  PyDict_SetItemString(keywords.get(), "msg", Py_BuildValue("s", msg));

  // call snow_logging.SnowLogger.log()
  PyObject_Call(call_log.get(), Py_BuildValue("()"), keywords.get());
}


void Logger::debug(const char *path_name, const char *func_name, int line_num, const char *format, ...)
{
  char msg[1000] = {0};
  va_list args;
  va_start(args, format);
  vsnprintf(msg, sizeof(msg), format, args);
  va_end(args);

  Logger::log(DEBUG, path_name, func_name, line_num, msg);
}

void Logger::info(const char *path_name, const char *func_name, int line_num, const char *format, ...)
{
  char msg[1000] = {0};
  va_list args;
  va_start(args, format);
  vsnprintf(msg, sizeof(msg), format, args);
  va_end(args);

  Logger::log(INFO, path_name, func_name, line_num, msg);
}

void Logger::warn(const char *path_name, const char *func_name, int line_num, const char *format, ...)
{
  char msg[1000] = {0};
  va_list args;
  va_start(args, format);
  vsnprintf(msg, sizeof(msg), format, args);
  va_end(args);

  Logger::log(WARN, path_name, func_name, line_num, msg);
}

void Logger::error(const char *path_name, const char *func_name, int line_num, const char *format, ...)
{
  char msg[1000] = {0};
  va_list args;
  va_start(args, format);
  vsnprintf(msg, sizeof(msg), format, args);
  va_end(args);


  Logger::log(ERROR, path_name, func_name, line_num, msg);
}

}
