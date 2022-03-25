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

#ifndef GRPC_TEST_CPP_UTIL_SERVICE_DESCRIBER_H
#define GRPC_TEST_CPP_UTIL_SERVICE_DESCRIBER_H

#include <grpcpp/support/config.h>

#include "test/cpp/util/config_grpc_cli.h"

namespace grpc {
namespace testing {

TString DescribeServiceList(std::vector<TString> service_list,
                                grpc::protobuf::DescriptorPool& desc_pool);

TString DescribeService(const grpc::protobuf::ServiceDescriptor* service);

TString DescribeMethod(const grpc::protobuf::MethodDescriptor* method);

TString SummarizeService(const grpc::protobuf::ServiceDescriptor* service);

TString SummarizeMethod(const grpc::protobuf::MethodDescriptor* method);

}  // namespace testing
}  // namespace grpc

#endif  // GRPC_TEST_CPP_UTIL_SERVICE_DESCRIBER_H
