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

#ifndef _CMD_OPTIONS_H_
#define _CMD_OPTIONS_H_

#include <iostream>
#include <iomanip>
#include <string>
#include <vector>
#include <exception>
#include <unordered_set>
#include <unordered_map>
#include <sstream>


namespace similarity {

#ifdef _MSC_VER
#define NOEXCEPT
#else
#define NOEXCEPT noexcept
#endif

class CmdParserException : public std::exception {
 public:
  CmdParserException(const std::string& msg) : msg_(msg) {}
  const char* what() const NOEXCEPT { return msg_.c_str(); }
 private:
  std::string msg_;
};

template <typename T>
class Value {
 public:
  static T get_default_value() { return T(); }
  static void convert(const std::string& arg, T* value) {}
};

template <>
class Value<bool> {
 public:
  static bool get_default_value() { return false; }
  static void convert(const std::string& arg, bool* value) {
    if (arg == "true") {
      *value = true;
    } else if (arg == "false") {
      *value = false;
    } else if (arg == "0") {
      *value = false;
    } else {
      *value = true;
    }
  }
};

template <>
class Value<int> {
 public:
  static int get_default_value() { return 0; }
  static void convert(const std::string& arg, int* value) {
    std::istringstream ss(arg);
    int n;
    ss >> n;
    *value = n;
  }
};

template <>
class Value<unsigned> {
 public:
  static unsigned get_default_value() { return 0; }
  static void convert(const std::string& arg, unsigned* value) {
    std::istringstream ss(arg);
    unsigned n;
    ss >> n;
    *value = n;
  }
};

template <>
class Value<float> {
 public:
  static float get_default_value() { return 0.0; }
  static void convert(const std::string& arg, float* value) {
    std::istringstream ss(arg);
    float n;
    ss >> n;
    *value = n;
  }
};

template <>
class Value<double> {
 public:
  static double get_default_value() { return 0.0; }
  static void convert(const std::string& arg, double* value) {
    std::istringstream ss(arg);
    double n;
    ss >> n;
    *value = n;
  }
};

template <>
class Value<std::string> {
 public:
  static std::string get_default_value() { return ""; }
  static void convert(const std::string& arg, std::string* value) {
    *value = arg;
  }
};

template <>
class Value<std::vector<std::string>> {
 public:
  static std::vector<std::string> get_default_value() {
    std::vector<std::string> value;
    return value;
  }
  static void convert(const std::string& arg,
                      std::vector<std::string>* value) {
    value->push_back(arg);
  }
};

template <>
class Value<std::vector<int>> {
 public:
  static std::vector<int> get_default_value() {
    std::vector<int> value;
    return value;
  }
  static void convert(const std::string& arg,
                      std::vector<int>* value) {
    std::istringstream ss(arg);
    int n;
    ss >> n;
    value->push_back(n);
  }
};

template <>
class Value<std::vector<double>> {
 public:
  static std::vector<double> get_default_value() {
    std::vector<double> value;
    return value;
  }
  static void convert(const std::string& arg,
                      std::vector<double>* value) {
    std::istringstream ss(arg);
    double n;
    ss >> n;
    value->push_back(n);
  }
};

template <typename T>
std::string Str(const T& value) {
  std::stringstream ss;
  ss << value;
  return ss.str();
}

template <typename T>
std::string Str(const std::vector<T>& value) {
  std::stringstream ss;
  for (auto& v : value) {
    ss << v << " ";
  }
  return ss.str();
}

class CmdParam {
 public:
  template <typename T>
  CmdParam(const std::string& names,
           const std::string& descr,
           T* value,
           bool required,
           typename std::remove_reference<T>::type default_value = Value<T>::get_default_value())
    : descr_(descr),
      ptr_(new Holder<T>(value, default_value)),
      required_(required),
      parsed_(false) {
    *value = default_value;
    auto pos = names.find(",");
    if (pos == std::string::npos) {
      long_name_ = names;
      short_name_ = "";
    } else {
      long_name_ = names.substr(0, pos);
      short_name_ = names.substr(pos + 1);
    }
    if (!long_name_.empty()) {
      long_name_ = "--" + long_name_;
    }
    if (!short_name_.empty()) {
      short_name_ = "-" + short_name_;
    }
  }

  const std::string& get_long_name() { return long_name_; }
  const std::string& get_short_name() { return short_name_; }
  bool is_required() { return required_; }
  bool is_parsed() { return parsed_; }
  bool is_allow_multiple() { return ptr_->is_allow_multiple(); }

  void Parse(const std::string& arg) {
    ptr_->Parse(arg);
    parsed_ = true;
  }

  std::string ParamOptStr() {
    return long_name_ +
           (long_name_.empty() || short_name_.empty() ? "" : ", ") +
           short_name_;
  }

  std::string ParamDesc(const string& addPadd) {
    std::stringstream ss;
    ss << descr_ << " " 
       << std::endl << addPadd << (required_ ? "" : ptr_->ToString());
    return ss.str();
  }

  std::string ToString() {
    std::stringstream ss;
    std::cout 
        << "\t" << ParamOptStr() << " : " << std::endl
        << "\t\t" << ParamDesc("\t\t"); 
    return ss.str();
  }

 private:

  class Base {
   public:
    virtual ~Base() {}
    virtual void Parse(const std::string& arg) = 0;
    virtual std::string ToString() = 0;
    virtual bool is_allow_multiple() = 0;
  };

  template <typename T>
  class Holder : public Base {
   public:
    Holder(T* value, const T defval)
        : value_(value), defval_(defval) {}

    void Parse(const std::string& arg) override {
      Value<type>::convert(arg, value_);
    }

    std::string ToString() override {
      std::stringstream ss;
      ss << "(default value: " << Str(defval_) << ")";
      return ss.str();
    }

    bool is_allow_multiple() override {
      return std::is_same<type, std::vector<std::string>>::value ||
          std::is_same<type, std::vector<int>>::value ||
          std::is_same<type, std::vector<double>>::value;
    }

   private:
    T* value_;
    const T defval_;
    using type = T;
  };

  std::string long_name_;
  std::string short_name_;
  std::string descr_;
  Base* ptr_;
  bool required_;
  bool parsed_;
};

class CmdOptions {
 public:
  ~CmdOptions() {
    for (auto* ptr : params_) {
      delete ptr;
    }
  }

  void Add(CmdParam* param) {
    params_.push_back(param);
    if (!param->get_long_name().empty()) {
      if (lookup_.count(param->get_long_name())) {
        std::stringstream ss;
        ss << "duplicate command line option "
            << param->get_long_name();
        throw CmdParserException(ss.str().c_str());
      }
      lookup_[param->get_long_name()] = param;
    }
    if (!param->get_short_name().empty()) {
      if (lookup_.count(param->get_short_name())) {
        std::stringstream ss;
        ss << "duplicate command line option "
            << param->get_short_name();
        throw CmdParserException(ss.str().c_str());
      }
      lookup_[param->get_short_name()] = param;
    }
  }

  void Parse(int argc, char* argv[]) {
    std::unordered_set<CmdParam*> processed;
    for (int i = 1; i < argc; i += 2) {
      std::string arg_name = argv[i];
      if (arg_name == "--help" || arg_name == "-h") {
        ToString();
        exit(0);
      }
      auto param = lookup_.find(arg_name);
      if (param == lookup_.end()) {
        std::stringstream ss;
        ss << "unknown argument " << arg_name;
        throw CmdParserException(ss.str().c_str());

      }
      if (!param->second->is_allow_multiple() && processed.count(param->second)) {
        std::stringstream ss;
        ss << "duplicate argument " << arg_name;
        throw CmdParserException(ss.str().c_str());
      }
      processed.insert(param->second);
      if (i + 1 < argc) {
        param->second->Parse(argv[i + 1]);
      } else {
        throw CmdParserException("Missing argument of the last parameter");
      }
    }
    for (auto* param : params_) {
      if (param->is_required() && !param->is_parsed()) {
        std::stringstream ss;
        ss << "missing required parameter "
            << param->get_long_name()
            << (param->get_short_name().empty() ? "" : ", ")
            << param->get_short_name();
        throw CmdParserException(ss.str().c_str());
      }
    }
  }

  void ToString() {
    std::cout << "Allowed options: " << std::endl;
    for (auto* param : params_) {
      std::cout << param->ToString() << std::endl;
    }
    std::cout.flush();
  }

 private:
  std::vector<CmdParam*> params_;
  std::unordered_map<std::string, CmdParam*> lookup_;
};

}             // namespace similarity

#endif        // _CMD_OPTIONS_H_
