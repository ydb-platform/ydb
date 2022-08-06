/*
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

#ifndef avro_CustomFields_hh__
#define avro_CustomFields_hh__

#include <iostream>

#include "../impl/json/JsonDom.hh"

namespace avro {

// CustomFields class stores avro custom attributes.
// Each field is represented by a unique name and value.
// User is supposed to create CustomFields object and then add it to Schema.
class AVRO_DECL CustomFields {
  public:
    // Retrieves the custom field json entity for that fieldName, returns an
    // null Entity if the field doesn't exist.
    json::Entity getField(const std::string &fieldName) const;

    // Adds a custom field. If the field already exists, throw an exception.
    void addField(const std::string &fieldName, const json::Entity &fieldValue);
    void addField(const std::string &fieldName, const std::string &fieldValue);

    // Provides a way to iterate over the custom fields or check field size.
    const std::map<std::string, json::Entity> &fields() const {
        return fields_;
    }

    // Prints the json string for the specific field.
    void printJson(std::ostream& os, const std::string &fieldName) const;

  private:
    std::map<std::string, json::Entity> fields_;
};

}  // namespace avro

#endif
