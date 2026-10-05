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
#ifndef PARAMS_H
#define PARAMS_H

#include <string>
#include <vector>
#include <limits>
#include <map>
#include <set>
#include <memory>
#include <sstream>
#include <algorithm>
#include <stdexcept>

#include "logging.h"
#include "utils.h"

namespace similarity {

using std::string;
using std::vector;
using std::multimap;
using std::set;
using std::stringstream;
using std::shared_ptr;
using std::unique_ptr;


#define FAKE_MAX_LEAVES_TO_VISIT std::numeric_limits<int>::max() 

class AnyParams {
public:
  /* 
   * Each element of the description array is in the form:
   * <param name>=<param value>
   */
  AnyParams(const vector<string>& Desc) :ParamNames(0), ParamValues(0) {
    set<string> seen;
    for (unsigned i = 0; i < Desc.size(); ++i) {
      vector<string>  OneParamPair;
      if (!SplitStr(Desc[i], OneParamPair, '=') ||
          OneParamPair.size() != 2) {
        stringstream err;
        err << "Wrong format of an argument: '" << Desc[i] << "' should be in the format: <Name>=<Value>";
        LOG(LIB_ERROR)  << err.str();
        throw runtime_error(err.str());
      }
      const string& Name = OneParamPair[0];
      const string& sVal = OneParamPair[1];

      if (seen.count(Name)) {
        string err = "Duplicate parameter: " + Name;
        LOG(LIB_ERROR)  << err;
        throw runtime_error(err);
      }
      seen.insert(Name);

      ParamNames.push_back(Name);
      ParamValues.push_back(sVal);
    }
  }
  
  /* 
   * Compare parameters against parameters in the other parameter container.
   * In doing so, IGNORE parameters from the exception list.
   */
  bool equalsIgnoreInList(const AnyParams& that, const vector<string>& ExceptList) {   
   /*
    * These loops are reasonably efficient, unless
    * we have thousands of parameters in the exception
    * list (which realistically won't happen)
    */
    
    vector<pair<string, string>> vals[2], inter;
    const AnyParams*             objRefs[2] = {this, &that};


    for (size_t objId = 0; objId < 2; ++objId) {
      const AnyParams& obj = *objRefs[objId];
 
      for (size_t i = 0; i < obj.ParamNames.size(); ++i) {
        const string& name = obj.ParamNames[i];
        if (find(ExceptList.begin(), ExceptList.end(), name) == ExceptList.end()) {
          vals[objId].push_back(make_pair(name, obj.ParamValues[i]));
        }
      }      
      sort(vals[objId].begin(), vals[objId].end());
    }
    
    inter.resize(ParamNames.size() + that.ParamNames.size());
    size_t qty = set_intersection(vals[0].begin(), vals[0].end(),
                     vals[1].begin(), vals[1].end(),
                     inter.begin()) - inter.begin();
    /*
     * We computed the size of the intersection.
     * If it is equal to the size of either of the original 
     * param lists (entries in the exception lists are excluded
     * at this point), then both sets are equal.
     */
    return qty == vals[0].size() && qty == vals[1].size();    
  }

  string ToString() const {
    stringstream res;
    for (unsigned i = 0; i < ParamNames.size(); ++i) {
      if (i) res << ",";
      res << ParamNames[i] << "=" << ParamValues[i];
    }
    return res.str();
  }

  template <typename ParamType> 
  void ChangeParam(const string& Name, const ParamType& Value) {
    for (unsigned i = 0; i < ParamNames.size(); ++i) 
    if (Name == ParamNames[i]) {
      stringstream str;

      str << Value;
      ParamValues[i] = str.str();
      return;
    }
    string err = "Parameter not found: " + Name;
    LOG(LIB_ERROR)  << err;
    throw runtime_error(err);
  } 

  template <typename ParamType> 
  void AddChangeParam(const string& Name, const ParamType& Value) {
    stringstream str;
    str << Value;
  
    for (unsigned i = 0; i < ParamNames.size(); ++i) 
    if (Name == ParamNames[i]) {
      ParamValues[i] = str.str();
      return;
    }

    ParamNames.push_back(Name);
    ParamValues.push_back(str.str());
  }

  AnyParams(){}
  AnyParams(const vector<string>& Names, const vector<string>& Values) 
            : ParamNames(Names), ParamValues(Values) {}
  AnyParams(const AnyParams& that)
            : ParamNames(that.ParamNames), ParamValues(that.ParamValues) {}

  vector<string>  ParamNames;
  vector<string>  ParamValues;

};

const inline AnyParams& getEmptyParams() {
  static AnyParams empty;
  return empty;
}

class AnyParamManager {
public:
  AnyParamManager(const AnyParams& params) : params(params), seen() {
    if (params.ParamNames.size() != params.ParamValues.size()) {
      string err = "Bug: different # of parameters and values";
      LOG(LIB_ERROR) << err;
      throw runtime_error(err);
    }
  }

  template <typename ParamType>
  void GetParamRequired(const string&  Name, ParamType& Value) {
    GetParam<ParamType>(Name, Value, true);
  }

  template <typename ParamType, typename DefaultType>
  void GetParamOptional(const string&  Name, ParamType& Value, const DefaultType& DefaultValue) {
    Value=DefaultValue;
    GetParam<ParamType>(Name, Value, false);
  }

  /*
   * Takes a list of exceptions and extracts all parameter values, 
   * except parameters from the exceptions' list. The extracted parameters
   * are added to the list of parameters already seen.
   */
  AnyParams ExtractParametersExcept(const vector<string>& ExceptList) {
    set<string> except(ExceptList.begin(), ExceptList.end());

    vector<string> names, values;

    for (size_t i = 0; i < params.ParamNames.size(); ++i) {
      const string& name = params.ParamNames[i];
      if (except.count(name) == 0) { // Not on the exception list
        names.push_back(name);
        values.push_back(params.ParamValues[i]);
        seen.insert(name);
      }
    }

    return AnyParams(names, values);
  }

  /*
   * Extract all parameter values, whose values are on the list.
   * The extracted parameters are added to the list of parameters already seen.
   */
  AnyParams ExtractParameters(const vector<string>& CheckList) {
    set<string> except(CheckList.begin(), CheckList.end());

    vector<string> names, values;

    for (size_t i = 0; i < params.ParamNames.size(); ++i) {
      const string& name = params.ParamNames[i];
      if (except.count(name)) { // On the list
        names.push_back(name);
        values.push_back(params.ParamValues[i]);
        seen.insert(name);
      }
    }

    return AnyParams(names, values);
  }

  bool hasParam(const string& name) {
    for (const string& s: params.ParamNames)
    if (s == name) return true;
    return false;
  };

  void CheckUnused() {
    bool bFail = false;
    for (size_t i = 0; i < params.ParamNames.size(); ++i) {
      const string& name = params.ParamNames[i];
      if (seen.count(name) == 0) {
        bFail = true;
        LOG(LIB_ERROR) << "Unknown parameter: '" << name << "'";
      }
    }
    if (bFail) throw runtime_error("Unknown parameters found!"); 
  }

  const AnyParams& GetAllParams() const { return params; }

  void copySeen(AnyParamManager& other) {
    for (const string& s: seen) other.seen.insert(s);
  }

private:
  const AnyParams&  params;
  set<string>       seen;

  template <typename ParamType>
  void GetParam(const string&  Name, ParamType& Value, bool bRequired) {
    bool bFound = false;
    /* 
     * This loop is reasonably efficient, unless
     * we have thousands of parameters (which realistically won't happen)
     */
    for (size_t i =0; i < params.ParamNames.size(); ++i) 
    if (Name == params.ParamNames[i]) {
      bFound = true; 
      ConvertStrToValue<ParamType>(params.ParamValues[i], Value);
    }

    if (bFound) {
      seen.insert(Name);
    }

    if (!bFound) {
      if (bRequired) {
        stringstream err;
        err <<  "Mandatory parameter: '" << Name << "' is missing!";
        LOG(LIB_ERROR) << err.str();
        throw runtime_error(err.str());
      }
    } 
  }

  template <typename ParamType>
  void ConvertStrToValue(const string& s, ParamType& Value);
};


template <typename ParamType>
inline void AnyParamManager::ConvertStrToValue(const string& s, ParamType& Value) {
  stringstream str(s);

  if (!(str>>Value) || !str.eof()) {
    stringstream err;
    err << "Failed to convert value '" << s << "' from type: " << typeid(Value).name();
    LOG(LIB_ERROR) << err.str();
    throw runtime_error(err.str());
  }
}

template <>
inline void AnyParamManager::ConvertStrToValue<string>(const string& str, string& Value) {
  Value = str;
}

// Parse space name/parameters
void ParseSpaceArg(const string& str, string& SpaceType, vector<string>& SpaceDesc);
// Take a comma-separated list of parameters and split them.
void ParseArg(const string& str, vector<string>& vDesc);

};

#endif
