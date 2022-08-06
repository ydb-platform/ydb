
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "CustomFields.hh"
#include <map>
#include <memory>
#include "Exception.hh"

namespace avro {

using json::Entity;

Entity CustomFields::getField(const std::string &fieldName) const {
    std::map<std::string, Entity>::const_iterator iter =
        fields_.find(fieldName);
    if (iter == fields_.end()) {
      return Entity();
    }
    return iter->second;
}

void CustomFields::addField(const std::string& fieldName,
                            const std::string& fieldValue) {
  addField(fieldName,
           json::Entity(std::make_shared<std::string>(fieldValue)));
}

void CustomFields::addField(const std::string& fieldName,
                            const Entity& fieldValue) {
  auto iter_and_find =
      fields_.insert(std::pair<std::string, Entity>(fieldName, fieldValue));
  if (!iter_and_find.second) {
    throw Exception(fieldName + " already exists and cannot be added");
  }
}

void CustomFields::printJson(std::ostream& os,
                             const std::string& fieldName) const {
    if (fields_.find(fieldName) == fields_.end()) {
        throw Exception(fieldName + " doesn't exist");
    }
    os << "\"" << fieldName << "\": " << fields_.at(fieldName).toString();
}
}  // namespace avro
