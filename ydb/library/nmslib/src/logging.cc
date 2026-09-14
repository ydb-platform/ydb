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
 *///  To shutup Microsoft Compiler who complains about localtime being unsafe
#ifdef _MSC_VER
#define _CRT_SECURE_NO_WARNINGS
#endif

#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#include <iostream>
#include <memory>
#include <string>

#include "logging.h"

const char* log_severity[] = {"DEBUG", "INFO", "WARNING", "ERROR", "FATAL"};

using namespace std;

namespace {
  std::unique_ptr<Logger> global_log(new StdErrLogger());
}

void setGlobalLogger(Logger * logger) {
  global_log.reset(logger);
}

Logger * getGlobalLogger() {
  return global_log.get();
}

Logger::~Logger() {
}

std::string LibGetCurrentTime() {
  time_t now;
  time(&now);
  struct tm* timeinfo = localtime(&now);
  char time_string[50];
  strftime(time_string, sizeof(time_string), "%Y-%m-%d %H:%M:%S", timeinfo);
  return std::string(time_string);
}

template <typename T>
void defaultOutput(T & stream, LogSeverity severity,
                   const std::string& _file, int line, const char* function,
                   const std::string & message) {
  std::string file = _file;
  size_t n = file.rfind('/');
  if (n != std::string::npos) {
    file.erase(file.begin(), file.begin() + n + 1);
  }
  stream << LibGetCurrentTime() << " " << file << ":" << line
         << " (" << function << ") [" << log_severity[severity] << "] " << message << std::endl;
}

void StdErrLogger::log(LogSeverity severity,
                       const char * file,
                       int line,
                       const char * function,
                       const std::string & message) {
  defaultOutput(std::cerr, severity, file, line, function, message);
}

FileLogger::FileLogger(const char * logfilename)
  : logfile(logfilename) {
  if (!logfile) {
    LOG(LIB_FATAL) << "Can't open the logfile: '" << logfilename << "'";
  }
}

void FileLogger::log(LogSeverity severity,
                     const char * file,
                     int line,
                     const char * function,
                     const std::string & message) {
  defaultOutput(logfile, severity, file, line, function, message);
}

void InitializeLogger(LogChoice choice, const char* logfile) {
  switch (choice) {
    case LIB_LOGNONE:
      setGlobalLogger(NULL);
      break;
    case LIB_LOGSTDERR:
      setGlobalLogger(new StdErrLogger());
      break;
    case LIB_LOGFILE:
      setGlobalLogger(new FileLogger(logfile));
      break;
    case LIB_LOGCUSTOM:
      // don't set a logger here: setGlobalLogger will be called already
      break;
  }
}

RuntimeErrorWrapper::RuntimeErrorWrapper(const std::string& _file, int line, const char* function) {
  std::string file = _file;
  size_t n = file.rfind('/');
  if (n != std::string::npos) {
    file.erase(file.begin(), file.begin() + n + 1);
  }
  stream() << LibGetCurrentTime() << " " << file << ":" << line
           << " (" << function << ") ";
}
