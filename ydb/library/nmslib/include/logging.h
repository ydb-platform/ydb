/**
 * Non-metric Space Library
 *
 * Main developers: Bilegsaikhan Naidan, Leonid Boytsov, Yury Malkov, Ben Frederickson, David Novak
 *
 * For the complete list of contributors and further details see:
 * https://github.com/nmslib/nmslib
 *
 * Copyright (c) 2013-2018
 *
 * This code is released under the
 * Apache License Version 2.0 http://www.apache.org/licenses/.
 *
 */
#ifndef _LOGGING_H_
#define _LOGGING_H_

#include <fstream>
#include <iostream>
#include <string>
#include <sstream>
#include <stdexcept>

using std::ostream;
using std::ofstream;
using std::cerr;
using std::stringstream;
using std::runtime_error;
using std::string;

enum LogSeverity {LIB_DEBUG, LIB_INFO, LIB_WARNING, LIB_ERROR, LIB_FATAL};
enum LogChoice  {LIB_LOGNONE, LIB_LOGFILE, LIB_LOGSTDERR, LIB_LOGCUSTOM};

std::string LibGetCurrentTime();

// write log to file
void InitializeLogger(LogChoice choice = LIB_LOGNONE, const char* logfile = NULL);

// Abstract base class that all loggers must override
class Logger {
 public:
  virtual ~Logger();
  virtual void log(LogSeverity severity,
                   const char * file,
                   int line,
                   const char * function,
                   const std::string & message) = 0;
};

void setGlobalLogger(Logger * logger);
Logger * getGlobalLogger();

class StdErrLogger
  : public Logger {
 public:
  void log(LogSeverity severity,
           const char * file,
           int line,
           const char * function,
           const std::string & message);
};

class FileLogger
  : public Logger {
 public:
  FileLogger(const char * logfile);
  void log(LogSeverity severity,
           const char * file,
           int line,
           const char * function,
           const std::string & message);
 protected:
  std::ofstream logfile;
};

// A single entry in the log
class LogItem {
 public:
  LogItem(LogSeverity severity, const char * file, int line, const char * function,
          Logger * logger)
    : severity(severity), file(file), line(line), function(function), logger(logger) {
  }

  template <typename T>
  LogItem & operator << (T t) {
    message << t;
    return *this;
  }

  ~LogItem() {
    if (logger) {
        logger->log(severity, file, line, function, message.str());
    }
    // TODO: probably better to throw an exception here rather than die outright
    // but this matches previous behaviour
    if (severity == LIB_FATAL) {
      exit(1);
    }
  }

  LogSeverity severity;
  const char * file;
  int line;
  const char * function;
  Logger * logger;
  std::stringstream message;
};

class RuntimeErrorWrapper {
 public:
  RuntimeErrorWrapper(const std::string& file, int line, const char* function);

  stringstream& stream() { return currstrm_ ; }

 private:
  stringstream currstrm_;
};

// TODO: zero cost log abstraction
#define LOG(severity) \
  LogItem(severity, __FILE__, __LINE__, __FUNCTION__, getGlobalLogger())

#define CHECK(condition) \
  if (!(condition)) {\
    LOG(LIB_ERROR) << "Check failed: " << #condition;  \
    throw runtime_error("Check failed: it's either a bug or inconsistent data!"); \
  }

#define CHECK_MSG(condition,message) \
  if (!(condition)) {\
    LOG(LIB_ERROR) << "Check failed: " << #condition << " " << string(message);  \
    throw runtime_error("Check failed: " + string(message)); \
  }

// debug only check and log
#ifndef NDEBUG
#define DCHECK(condition) CHECK(condition)
#define DLOG(severity) LOG(severity)
#else
// NDEBUG disables standard C assertions
#define DCHECK(condition) while (0) CHECK(condition)
#define DLOG(severity) while (0) LOG(severity)
#endif

#define PREPARE_RUNTIME_ERR(var) \
  RuntimeErrorWrapper var(__FILE__, __LINE__, __FUNCTION__); var.stream()
#define THROW_RUNTIME_ERR(var) \
  throw runtime_error(var.stream().str())

#endif     // _LOGGING_H_

