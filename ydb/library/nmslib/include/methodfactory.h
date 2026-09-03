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
#ifndef METHOD_FACTORY_H
#define METHOD_FACTORY_H

#include <string>
#include <iostream>

#include "index.h"
#include "space.h"
#include "logging.h"

namespace similarity {

using std::string;

#define REGISTER_METHOD_CREATOR(type, name, func)\
      MethodFactoryRegistry<type>::Instance().Register(name, func); 
 

template <typename dist_t>
class MethodFactoryRegistry {
public:
  typedef Index<dist_t>* (*CreateFuncPtr)(
                           bool                 PrintProgress,
                           const string&        SpaceType,
                           Space<dist_t>&       space,
                           const ObjectVector&  DataObjects);

  static MethodFactoryRegistry& Instance() {
    static MethodFactoryRegistry elem;
    return elem;
  }

  void Register(const string& MethodName, CreateFuncPtr func) {
    LOG(LIB_INFO) << "Registering at the factory, method: " << MethodName << " distance type: " << DistTypeName<dist_t>();
    Creators_[MethodName] = func;
  }

  Index<dist_t>* CreateMethod(bool PrintProgress,
                            const string& MethName,
                            const string& SpaceType,
                            Space<dist_t>& space,
                            const ObjectVector& DataObjects) {
    if (Creators_.count(MethName)) {
      return Creators_[MethName](PrintProgress, SpaceType, space, DataObjects);
    } else {
      PREPARE_RUNTIME_ERR(err) << "It looks like the method " << MethName << 
                    " is not defined for the distance type : " << DistTypeName<dist_t>();
      THROW_RUNTIME_ERR(err);
    }
    return NULL;
  }
private:
  map<string, CreateFuncPtr>  Creators_;
};

}


#endif
