/*
 * H.265 video codec.
 * Copyright (c) 2013-2014 struktur AG, Dirk Farin <farin@struktur.de>
 *
 * Authors: struktur AG, Dirk Farin <farin@struktur.de>
 *
 * This file is part of libde265.
 *
 * libde265 is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * libde265 is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with libde265.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef CONFIG_PARAM_H
#define CONFIG_PARAM_H

#include "en265.h"
#include "util.h"

#include <climits>
#include <vector>
#include <string>
#include <stddef.h>
#include <assert.h>


/* Notes: probably best to keep cmd-line-options here. So it will be:
   - automatically consistent even when having different combinations of algorithms
   - no other place to edit
   - if needed, one can still override it at another place
 */

// TODO: set a stack of default prefixes in config_parameters, such that all options added
// will receive this namespace prefix.

// TODO: add the possibility to remove long options again, i.e., not use the default id name
class option_base
{
 public:
 option_base() : mShortOption(0), mLongOption(NULL) { }
 option_base(const char* name) : mIDName(name), mShortOption(0), mLongOption(NULL) { }
  virtual ~option_base() { }


  // --- option identifier ---

  void set_ID(const char* name) { mIDName=name; }
  void add_namespace_prefix(std::string prefix) { mPrefix = prefix + ":" + mPrefix; }

  std::string get_name() const { return mPrefix + mIDName; }


  // --- description ---

  void set_description(std::string descr) { mDescription = descr; }
  std::string get_description() const { return mDescription; }
  bool has_description() const { return !mDescription.empty(); }


  // --- value ---

  virtual bool is_defined() const = 0;
  bool is_undefined() const { return !is_defined(); }

  virtual bool has_default() const  = 0;


  // --- command line options ----

  void set_cmd_line_options(const char* long_option, char short_option = 0)
  {
    mShortOption = short_option;
    mLongOption  = long_option;
  }

  void set_short_option(char short_option) { mShortOption=short_option; }

  void unsetCmdLineOption()
  {
    mShortOption = 0;
    mLongOption  = NULL;
  }

  bool hasShortOption() const { return mShortOption!=0; }
  char getShortOption() const { return mShortOption; }
  bool hasLongOption() const { return true; } //mLongOption!=NULL; }
  std::string getLongOption() const { return mLongOption ? std::string(mLongOption) : get_name(); }

  virtual LIBDE265_API bool processCmdLineArguments(char** argv, int* argc, int idx) { return false; }



  virtual std::string getTypeDescr() const = 0;

  virtual std::string get_default_string() const { return "N/A"; }

 private:
  std::string mPrefix;
  std::string mIDName;

  std::string mDescription;

  char mShortOption;
  const char* mLongOption;
};



class option_bool : public option_base
{
public:
  option_bool() : value_set(false), default_set(false) { }

  operator bool() const {
    assert(value_set || default_set);
    return value_set ? value : default_value;
  }

  virtual bool is_defined() const { return value_set || default_set; }
  virtual bool has_default() const { return default_set; }

  void set_default(bool v) { default_value=v; default_set=true; }
  virtual std::string get_default_string() const { return default_value ? "true":"false"; }

  virtual std::string getTypeDescr() const { return "(boolean)"; }
  virtual LIBDE265_API bool processCmdLineArguments(char** argv, int* argc, int idx) { set(true); return true; }

  bool set(bool v) { value_set=true; value=v; return true; }

 private:
  bool value_set;
  bool value;

  bool default_set;
  bool default_value;
};


class option_string : public option_base
{
public:
  option_string() : value_set(false), default_set(false) { }

  const option_string& operator=(std::string v) { value=v; value_set=true; return *this; }

  operator std::string() const { return get(); }
  std::string get() const {
    assert(value_set || default_set);
    return value_set ? value : default_value;
  }

  virtual bool is_defined() const { return value_set || default_set; }
  virtual bool has_default() const { return default_set; }

  void set_default(std::string v) { default_value=v; default_set=true; }
  virtual LIBDE265_API std::string get_default_string() const { return default_value; }

  virtual LIBDE265_API std::string getTypeDescr() const { return "(string)"; }
  virtual LIBDE265_API bool processCmdLineArguments(char** argv, int* argc, int idx);

  bool set(std::string v) { value_set=true; value=v; return true; }

 private:
  bool value_set;
  std::string value;

  bool default_set;
  std::string default_value;
};


class option_int : public option_base
{
public:
  option_int() : value_set(false), default_set(false),
    have_low_limit(false), have_high_limit(false) { }

  void set_minimum(int mini) { have_low_limit =true; low_limit =mini; }
  void set_maximum(int maxi) { have_high_limit=true; high_limit=maxi; }
  void set_range(int mini,int maxi);
  void set_valid_values(const std::vector<int>& v) { valid_values_set = v; }

  const option_int& operator=(int v) { value=v; value_set=true; return *this; }

  int operator() () const {
    assert(value_set || default_set);
    return value_set ? value : default_value;
  }
  operator int() const { return operator()(); }

  virtual bool is_defined() const { return value_set || default_set; }
  virtual bool has_default() const { return default_set; }

  void set_default(int v) { default_value=v; default_set=true; }
  virtual LIBDE265_API std::string get_default_string() const;

  virtual LIBDE265_API std::string getTypeDescr() const;
  virtual LIBDE265_API bool processCmdLineArguments(char** argv, int* argc, int idx);

  bool set(int v) {
    if (is_valid(v)) { value_set=true; value=v; return true; }
    else { return false; }
  }

 private:
  bool value_set;
  int value;

  bool default_set;
  int default_value;

  bool have_low_limit, have_high_limit;
  int  low_limit, high_limit;

  std::vector<int> valid_values_set;

  bool is_valid(int v) const;
};



class choice_option_base : public option_base
{
public:
  choice_option_base() : choice_string_table(NULL) { }
  ~choice_option_base() { delete[] choice_string_table; }

  bool set(std::string v) { return set_value(v); }
  virtual bool set_value(const std::string& val) = 0;
  virtual std::vector<std::string> get_choice_names() const = 0;

  virtual std::string getTypeDescr() const;
  virtual LIBDE265_API bool processCmdLineArguments(char** argv, int* argc, int idx);

  const char** get_choices_string_table() const;

 protected:
  void invalidate_choices_string_table() {
    delete[] choice_string_table;
    choice_string_table = NULL;
  }

 private:
  mutable char* choice_string_table;
};


template <class T> class choice_option : public choice_option_base
{
 public:
 choice_option() : default_set(false), value_set(false) { }

  // --- initialization ---

  void add_choice(const std::string& s, T id, bool default_value=false) {
    choices.push_back( std::make_pair(s,id) );
    if (default_value) {
      defaultID = id;
      defaultValue = s;
      default_set = true;
    }

    invalidate_choices_string_table();
  }

  void set_default(T val) {
#ifdef FOR_LOOP_AUTO_SUPPORT
    FOR_LOOP(auto, c, choices) {
#else
    for (typename std::vector< std::pair<std::string,T> >::const_iterator it=choices.begin(); it!=choices.end(); ++it) {
      const std::pair<std::string,T> & c = *it;
#endif
      if (c.second == val) {
        defaultID = val;
        defaultValue = c.first;
        default_set = true;
        return;
      }
    }

    assert(false); // value does not exist
  }


  // --- usage ---

  bool set_value(const std::string& val) // returns false if it is not a valid option
  {
    value_set = true;
    selectedValue=val;

    validValue = false;

#ifdef FOR_LOOP_AUTO_SUPPORT
    FOR_LOOP(auto, c, choices) {
#else
    for (typename std::vector< std::pair<std::string,T> >::const_iterator it=choices.begin(); it!=choices.end(); ++it) {
      const std::pair<std::string,T> & c = *it;
#endif
      if (val == c.first) {
        selectedID = c.second;
        validValue = true;
      }
    }

    return validValue;
  }

  bool isValidValue() const { return validValue; }

  const std::string& getValue() const {
    assert(value_set || default_set);
    return value_set ? selectedValue : defaultValue;
  }
  void setID(T id) { selectedID=id; validValue=true; }
  const T getID() const { return value_set ? selectedID : defaultID; }

  virtual bool is_defined() const { return value_set || default_set; }
  virtual bool has_default() const { return default_set; }

  std::vector<std::string> get_choice_names() const
  {
    std::vector<std::string> names;
#ifdef FOR_LOOP_AUTO_SUPPORT
    FOR_LOOP(auto, p, choices) {
#else
    for (typename std::vector< std::pair<std::string,T> >::const_iterator it=choices.begin(); it!=choices.end(); ++it) {
      const std::pair<std::string,T> & p = *it;
#endif
      names.push_back(p.first);
    }
    return names;
  }

  std::string get_default_string() const { return defaultValue; }

  T operator() () const { return (T)getID(); }

 private:
  std::vector< std::pair<std::string,T> > choices;

  bool default_set;
  std::string defaultValue;
  T defaultID;

  bool value_set;
  std::string selectedValue;
  T selectedID;

  bool validValue;
};




class config_parameters
{
 public:
 config_parameters() : param_string_table(NULL) { }
  ~config_parameters() { delete[] param_string_table; }

  void LIBDE265_API add_option(option_base* o);

  void LIBDE265_API print_params() const;
  bool LIBDE265_API parse_command_line_params(int* argc, char** argv, int* first_idx=NULL,
                                 bool ignore_unknown_options=false);


  // --- connection to C API ---

  std::vector<std::string> get_parameter_IDs() const;
  enum en265_parameter_type get_parameter_type(const char* param) const;

  std::vector<std::string> get_parameter_choices(const char* param) const;

  bool set_bool(const char* param, bool value);
  bool set_int(const char* param, int value);
  bool set_string(const char* param, const char* value);
  bool set_choice(const char* param, const char* value);

  const char** get_parameter_string_table() const;
  const char** get_parameter_choices_table(const char* param) const;

 private:
  std::vector<option_base*> mOptions;

  option_base* find_option(const char* param) const;

  mutable char* param_string_table;
};

#endif
