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

#include "configparam.h"

#include <string.h>
#include <ctype.h>
#include <sstream>
#include <iomanip>
#include <iostream>
#include <algorithm>
#include <typeinfo>

#ifndef RTTI_ENABLED
#error "Need to compile with RTTI enabled."
#endif

static void remove_option(int* argc,char** argv,int idx, int n=1)
{
  for (int i=idx+n;i<*argc;i++) {
    argv[i-n] = argv[i];
  }

  *argc-=n;
}


bool option_string::processCmdLineArguments(char** argv, int* argc, int idx)
{
  if (argv==NULL)   { return false; }
  if (idx >= *argc) { return false; }

  value = argv[idx];
  value_set = true;

  remove_option(argc,argv,idx,1);

  return true;
}


void option_int::set_range(int mini,int maxi)
{
  have_low_limit =true;
  have_high_limit=true;
  low_limit =mini;
  high_limit=maxi;
}

std::string option_int::getTypeDescr() const
{
  std::stringstream sstr;
  sstr << "(int)";

  if (have_low_limit || have_high_limit) { sstr << " "; }
  if (have_low_limit) { sstr << low_limit << " <= "; }
  if (have_low_limit || have_high_limit) { sstr << "x"; }
  if (have_high_limit) { sstr << " <= " << high_limit; }

  if (!valid_values_set.empty()) {
    sstr << " {";
    bool first=true;
    FOR_LOOP(int, v, valid_values_set) {
      if (!first) sstr << ","; else first=false;
      sstr << v;
    }
    sstr << "}";
  }

  return sstr.str();
}

bool option_int::processCmdLineArguments(char** argv, int* argc, int idx)
{
  if (argv==NULL)   { return false; }
  if (idx >= *argc) { return false; }

  int v = atoi(argv[idx]);
  if (!is_valid(v)) { return false; }

  value = v;
  value_set = true;

  remove_option(argc,argv,idx,1);

  return true;
}

bool option_int::is_valid(int v) const
{
  if (have_low_limit  && v<low_limit)  { return false; }
  if (have_high_limit && v>high_limit) { return false; }

  if (!valid_values_set.empty()) {
    auto iter = std::find(valid_values_set.begin(), valid_values_set.end(), v);
    if (iter==valid_values_set.end()) { return false; }
  }

  return true;
}

std::string option_int::get_default_string() const
{
  std::stringstream sstr;
  sstr << default_value;
  return sstr.str();
}


std::string choice_option_base::getTypeDescr() const
{
  std::vector<std::string> choices = get_choice_names();

  std::stringstream sstr;
  sstr << "{";

  bool first=true;
#ifdef FOR_LOOP_AUTO_SUPPORT
  FOR_LOOP(auto, c, choices) {
#else
  FOR_LOOP(std::string, c, choices) {
#endif
    if (first) { first=false; }
    else { sstr << ","; }

    sstr << c;
  }

  sstr << "}";
  return sstr.str();
}


bool choice_option_base::processCmdLineArguments(char** argv, int* argc, int idx)
{
  if (argv==NULL)   { return false; }
  if (idx >= *argc) { return false; }

  std::string value = argv[idx];

  std::cout << "set " << value << "\n";
  bool success = set_value(value);
  std::cout << "success " << success << "\n";

  remove_option(argc,argv,idx,1);

  return success;
}


static char* fill_strings_into_memory(const std::vector<std::string>& strings_list)
{
  // calculate memory requirement

  int totalStringLengths = 0;
#ifdef FOR_LOOP_AUTO_SUPPORT
  FOR_LOOP(auto, str, strings_list) {
#else
  FOR_LOOP(std::string, str, strings_list) {
#endif
    totalStringLengths += str.length() +1; // +1 for null termination
  }

  int numStrings = strings_list.size();

  int pointersSize = (numStrings+1) * sizeof(const char*);

  char* memory = new char[pointersSize + totalStringLengths];


  // copy strings to memory area

  char* stringPtr = memory + (numStrings+1) * sizeof(const char*);
  const char** tablePtr = (const char**)memory;

#ifdef FOR_LOOP_AUTO_SUPPORT
  FOR_LOOP(auto, str, strings_list) {
#else
  FOR_LOOP(std::string, str, strings_list) {
#endif
    *tablePtr++ = stringPtr;

    strcpy(stringPtr, str.c_str());
    stringPtr += str.length()+1;
  }

  *tablePtr = NULL;

  return memory;
}


const char** choice_option_base::get_choices_string_table() const
{
  if (choice_string_table==NULL) {
    choice_string_table = fill_strings_into_memory(get_choice_names());
  }

  return (const char**)choice_string_table;
}



bool config_parameters::parse_command_line_params(int* argc, char** argv, int* first_idx_ptr,
                                                  bool ignore_unknown_options)
{
  int first_idx=1;
  if (first_idx_ptr) { first_idx = *first_idx_ptr; }

  for (int i=first_idx;i < *argc;i++) {

    if (argv[i][0]=='-') {
      // option

      if (argv[i][1]=='-') {
        // long option

        bool option_found=false;

        for (size_t o=0;o<mOptions.size();o++) {
          if (mOptions[o]->hasLongOption() && strcmp(mOptions[o]->getLongOption().c_str(),
                                                     argv[i]+2)==0) {
            option_found=true;

            printf("FOUND %s\n",argv[i]);

            bool success = mOptions[o]->processCmdLineArguments(argv,argc, i+1);
            if (!success) {
              if (first_idx_ptr) { *first_idx_ptr = i; }
              return false;
            }

            remove_option(argc,argv,i);
            i--;

            break;
          }
        }

        if (option_found == false && !ignore_unknown_options) {
          return false;
        }
      }
      else {
        // short option

        bool is_single_option = (argv[i][1] != 0 && argv[i][2]==0);
        bool do_remove_option = true;

        for (int n=1; argv[i][n]; n++) {
          char option = argv[i][n];

          bool option_found=false;

          for (size_t o=0;o<mOptions.size();o++) {
            if (mOptions[o]->getShortOption() == option) {
              option_found=true;

              bool success;
              if (is_single_option) {
                success = mOptions[o]->processCmdLineArguments(argv,argc, i+1);
              }
              else {
                success = mOptions[o]->processCmdLineArguments(NULL,NULL, 0);
              }

              if (!success) {
                if (first_idx_ptr) { *first_idx_ptr = i; }
                return false;
              }

              break;
            }
          }

          if (!option_found) {
            if (!ignore_unknown_options) {
              fprintf(stderr, "unknown option -%c\n",option);
              return false;
            }
            else {
              do_remove_option=false;
            }
          }

        } // all short options

        if (do_remove_option) {
          remove_option(argc,argv,i);
          i--;
        }
      } // is short option
    } // is option
  } // all command line arguments

  return true;
}


void config_parameters::print_params() const
{
  for (size_t i=0;i<mOptions.size();i++) {
    const option_base* o = mOptions[i];

    std::stringstream sstr;
    sstr << "  ";
    if (o->hasShortOption()) {
      sstr << '-' << o->getShortOption();
    } else {
      sstr << "  ";
    }

    if (o->hasShortOption() && o->hasLongOption()) {
      sstr << ", ";
    } else {
      sstr << "  ";
    }

    if (o->hasLongOption()) {
      sstr << "--" << std::setw(12) << std::left << o->getLongOption();
    } else {
      sstr << "              ";
    }

    sstr << " ";
    sstr << o->getTypeDescr();

    if (o->has_default()) {
      sstr << ", default=" << o->get_default_string();
    }

    if (o->has_description()) {
      sstr << " : " << o->get_description();
    }

    sstr << "\n";

    std::cerr << sstr.str();
  }
}


void config_parameters::add_option(option_base* o)
{
  mOptions.push_back(o);
  delete[] param_string_table; // delete old table, since we got a new parameter
  param_string_table = NULL;
}


std::vector<std::string> config_parameters::get_parameter_IDs() const
{
  std::vector<std::string> ids;

#ifdef FOR_LOOP_AUTO_SUPPORT
  FOR_LOOP(auto, option, mOptions) {
#else
  FOR_LOOP(option_base*, option, mOptions) {
#endif
    ids.push_back(option->get_name());
  }

  return ids;
}


enum en265_parameter_type config_parameters::get_parameter_type(const char* param) const
{
  option_base* option = find_option(param);
  assert(option);

  if (dynamic_cast<option_int*>   (option)) { return en265_parameter_int; }
  if (dynamic_cast<option_bool*>  (option)) { return en265_parameter_bool; }
  if (dynamic_cast<option_string*>(option)) { return en265_parameter_string; }
  if (dynamic_cast<choice_option_base*>(option)) { return en265_parameter_choice; }

  assert(false);
  return en265_parameter_bool;
}


std::vector<std::string> config_parameters::get_parameter_choices(const char* param) const
{
  option_base* option = find_option(param);
  assert(option);

  choice_option_base* o = dynamic_cast<choice_option_base*>(option);
  assert(o);

  return o->get_choice_names();
}


option_base* config_parameters::find_option(const char* param) const
{
#ifdef FOR_LOOP_AUTO_SUPPORT
  FOR_LOOP(auto, o, mOptions) {
#else
  FOR_LOOP(option_base*, o, mOptions) {
#endif
    if (strcmp(o->get_name().c_str(), param)==0) { return o; }
  }

  return NULL;
}


bool config_parameters::set_bool(const char* param, bool value)
{
  option_base* option = find_option(param);
  assert(option);

  option_bool* o = dynamic_cast<option_bool*>(option);
  assert(o);

  return o->set(value);
}

bool config_parameters::set_int(const char* param, int value)
{
  option_base* option = find_option(param);
  assert(option);

  option_int* o = dynamic_cast<option_int*>(option);
  assert(o);

  return o->set(value);
}

bool config_parameters::set_string(const char* param, const char* value)
{
  option_base* option = find_option(param);
  assert(option);

  option_string* o = dynamic_cast<option_string*>(option);
  assert(o);

  return o->set(value);
}

bool config_parameters::set_choice(const char* param, const char* value)
{
  option_base* option = find_option(param);
  assert(option);

  choice_option_base* o = dynamic_cast<choice_option_base*>(option);
  assert(o);

  return o->set(value);
}



const char** config_parameters::get_parameter_choices_table(const char* param) const
{
  option_base* option = find_option(param);
  assert(option);

  choice_option_base* o = dynamic_cast<choice_option_base*>(option);
  assert(o);

  return o->get_choices_string_table();
}

const char** config_parameters::get_parameter_string_table() const
{
  if (param_string_table==NULL) {
    param_string_table = fill_strings_into_memory(get_parameter_IDs());
  }

  return (const char**)param_string_table;
}
