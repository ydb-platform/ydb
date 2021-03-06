/*
 *
 * Copyright 2016 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include "test/cpp/util/proto_file_parser.h"

#include <algorithm>
#include <iostream>
#include <sstream>
#include <unordered_set>

#include "y_absl/memory/memory.h"
#include "y_absl/strings/str_split.h"

#include <grpcpp/support/config.h>

namespace grpc {
namespace testing {
namespace {

// Match the user input method string to the full_name from method descriptor.
bool MethodNameMatch(const TString& full_name, const TString& input) {
  TString clean_input = input;
  std::replace(clean_input.begin(), clean_input.vend(), '/', '.');
  if (clean_input.size() > full_name.size()) {
    return false;
  }
  return full_name.compare(full_name.size() - clean_input.size(),
                           clean_input.size(), clean_input) == 0;
}
}  // namespace

class ErrorPrinter : public protobuf::compiler::MultiFileErrorCollector {
 public:
  explicit ErrorPrinter(ProtoFileParser* parser) : parser_(parser) {}

  void AddError(const google::protobuf::string& filename, int line, int column,
                const google::protobuf::string& message) override {
    std::ostringstream oss;
    oss << "error " << filename << " " << line << " " << column << " "
        << message << "\n";
    parser_->LogError(oss.str());
  }

  void AddWarning(const google::protobuf::string& filename, int line, int column,
                  const google::protobuf::string& message) override {
    std::cerr << "warning " << filename << " " << line << " " << column << " "
              << message << std::endl;
  }

 private:
  ProtoFileParser* parser_;  // not owned
};

ProtoFileParser::ProtoFileParser(const std::shared_ptr<grpc::Channel>& channel,
                                 const TString& proto_path,
                                 const TString& protofiles)
    : has_error_(false),
      dynamic_factory_(new protobuf::DynamicMessageFactory()) {
  std::vector<TString> service_list;
  if (channel) {
    reflection_db_ =
        y_absl::make_unique<grpc::ProtoReflectionDescriptorDatabase>(channel);
    reflection_db_->GetServices(&service_list);
  }

  std::unordered_set<TString> known_services;
  if (!protofiles.empty()) {
    for (const y_absl::string_view single_path : y_absl::StrSplit(
             proto_path, GRPC_CLI_PATH_SEPARATOR, y_absl::AllowEmpty())) {
      source_tree_.MapPath("", google::protobuf::string(single_path));
    }
    error_printer_ = y_absl::make_unique<ErrorPrinter>(this);
    importer_ = y_absl::make_unique<protobuf::compiler::Importer>(
        &source_tree_, error_printer_.get());

    std::string file_name;
    std::stringstream ss(protofiles);
    while (std::getline(ss, file_name, ',')) {
      const auto* file_desc = importer_->Import(google::protobuf::string(file_name));
      if (file_desc) {
        for (int i = 0; i < file_desc->service_count(); i++) {
          service_desc_list_.push_back(file_desc->service(i));
          known_services.insert(file_desc->service(i)->full_name());
        }
      } else {
        std::cerr << file_name << " not found" << std::endl;
      }
    }

    file_db_ =
        y_absl::make_unique<protobuf::DescriptorPoolDatabase>(*importer_->pool());
  }

  if (!reflection_db_ && !file_db_) {
    LogError("No available proto database");
    return;
  }

  if (!reflection_db_) {
    desc_db_ = std::move(file_db_);
  } else if (!file_db_) {
    desc_db_ = std::move(reflection_db_);
  } else {
    desc_db_ = y_absl::make_unique<protobuf::MergedDescriptorDatabase>(
        reflection_db_.get(), file_db_.get());
  }

  desc_pool_ = y_absl::make_unique<protobuf::DescriptorPool>(desc_db_.get());

  for (auto it = service_list.begin(); it != service_list.end(); it++) {
    if (known_services.find(*it) == known_services.end()) {
      if (const protobuf::ServiceDescriptor* service_desc =
              desc_pool_->FindServiceByName(google::protobuf::string(*it))) {
        service_desc_list_.push_back(service_desc);
        known_services.insert(*it);
      }
    }
  }
}

ProtoFileParser::~ProtoFileParser() {}

TString ProtoFileParser::GetFullMethodName(const TString& method) {
  has_error_ = false;

  if (known_methods_.find(method) != known_methods_.end()) {
    return known_methods_[method];
  }

  const protobuf::MethodDescriptor* method_descriptor = nullptr;
  for (auto it = service_desc_list_.begin(); it != service_desc_list_.end();
       it++) {
    const auto* service_desc = *it;
    for (int j = 0; j < service_desc->method_count(); j++) {
      const auto* method_desc = service_desc->method(j);
      if (MethodNameMatch(method_desc->full_name(), method)) {
        if (method_descriptor) {
          std::ostringstream error_stream;
          error_stream << "Ambiguous method names: ";
          error_stream << method_descriptor->full_name() << " ";
          error_stream << method_desc->full_name();
          LogError(error_stream.str());
        }
        method_descriptor = method_desc;
      }
    }
  }
  if (!method_descriptor) {
    LogError("Method name not found");
  }
  if (has_error_) {
    return "";
  }

  known_methods_[method] = method_descriptor->full_name();

  return method_descriptor->full_name();
}

TString ProtoFileParser::GetFormattedMethodName(const TString& method) {
  has_error_ = false;
  TString formatted_method_name = GetFullMethodName(method);
  if (has_error_) {
    return "";
  }
  size_t last_dot = formatted_method_name.find_last_of('.');
  if (last_dot != TString::npos) {
    formatted_method_name[last_dot] = '/';
  }
  formatted_method_name.insert(formatted_method_name.begin(), '/');
  return formatted_method_name;
}

TString ProtoFileParser::GetMessageTypeFromMethod(const TString& method,
                                                      bool is_request) {
  has_error_ = false;
  TString full_method_name = GetFullMethodName(method);
  if (has_error_) {
    return "";
  }
  const protobuf::MethodDescriptor* method_desc =
      desc_pool_->FindMethodByName(google::protobuf::string(full_method_name));
  if (!method_desc) {
    LogError("Method not found");
    return "";
  }

  return is_request ? method_desc->input_type()->full_name()
                    : method_desc->output_type()->full_name();
}

bool ProtoFileParser::IsStreaming(const TString& method, bool is_request) {
  has_error_ = false;

  TString full_method_name = GetFullMethodName(method);
  if (has_error_) {
    return false;
  }

  const protobuf::MethodDescriptor* method_desc =
      desc_pool_->FindMethodByName(google::protobuf::string(full_method_name));
  if (!method_desc) {
    LogError("Method not found");
    return false;
  }

  return is_request ? method_desc->client_streaming()
                    : method_desc->server_streaming();
}

TString ProtoFileParser::GetSerializedProtoFromMethod(
    const TString& method, const TString& formatted_proto,
    bool is_request, bool is_json_format) {
  has_error_ = false;
  TString message_type_name = GetMessageTypeFromMethod(method, is_request);
  if (has_error_) {
    return "";
  }
  return GetSerializedProtoFromMessageType(message_type_name, formatted_proto,
                                           is_json_format);
}

TString ProtoFileParser::GetFormattedStringFromMethod(
    const TString& method, const TString& serialized_proto,
    bool is_request, bool is_json_format) {
  has_error_ = false;
  TString message_type_name = GetMessageTypeFromMethod(method, is_request);
  if (has_error_) {
    return "";
  }
  return GetFormattedStringFromMessageType(message_type_name, serialized_proto,
                                           is_json_format);
}

TString ProtoFileParser::GetSerializedProtoFromMessageType(
    const TString& message_type_name, const TString& formatted_proto,
    bool is_json_format) {
  has_error_ = false;
  google::protobuf::string serialized;
  const protobuf::Descriptor* desc =
      desc_pool_->FindMessageTypeByName(google::protobuf::string(message_type_name));
  if (!desc) {
    LogError("Message type not found");
    return "";
  }
  std::unique_ptr<grpc::protobuf::Message> msg(
      dynamic_factory_->GetPrototype(desc)->New());
  bool ok;
  if (is_json_format) {
    ok = grpc::protobuf::json::JsonStringToMessage(google::protobuf::string(formatted_proto), msg.get())
             .ok();
    if (!ok) {
      LogError("Failed to convert json format to proto.");
      return "";
    }
  } else {
    ok = protobuf::TextFormat::ParseFromString(google::protobuf::string(formatted_proto), msg.get());
    if (!ok) {
      LogError("Failed to convert text format to proto.");
      return "";
    }
  }

  ok = msg->SerializeToString(&serialized);
  if (!ok) {
    LogError("Failed to serialize proto.");
    return "";
  }
  return serialized;
}

TString ProtoFileParser::GetFormattedStringFromMessageType(
    const TString& message_type_name, const TString& serialized_proto,
    bool is_json_format) {
  has_error_ = false;
  const protobuf::Descriptor* desc =
      desc_pool_->FindMessageTypeByName(google::protobuf::string(message_type_name));
  if (!desc) {
    LogError("Message type not found");
    return "";
  }
  std::unique_ptr<grpc::protobuf::Message> msg(
      dynamic_factory_->GetPrototype(desc)->New());
  if (!msg->ParseFromString(google::protobuf::string(serialized_proto))) {
    LogError("Failed to deserialize proto.");
    return "";
  }
  google::protobuf::string formatted_string;

  if (is_json_format) {
    grpc::protobuf::json::JsonPrintOptions jsonPrintOptions;
    jsonPrintOptions.add_whitespace = true;
    if (!grpc::protobuf::json::MessageToJsonString(*msg, &formatted_string,
                                                   jsonPrintOptions)
             .ok()) {
      LogError("Failed to print proto message to json format");
      return "";
    }
  } else {
    if (!protobuf::TextFormat::PrintToString(*msg, &formatted_string)) {
      LogError("Failed to print proto message to text format");
      return "";
    }
  }
  return formatted_string;
}

void ProtoFileParser::LogError(const TString& error_msg) {
  if (!error_msg.empty()) {
    std::cerr << error_msg << std::endl;
  }
  has_error_ = true;
}

}  // namespace testing
}  // namespace grpc
