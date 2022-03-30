
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(contrib-libs-googleapis-common-protos)
set_property(TARGET contrib-libs-googleapis-common-protos PROPERTY
  PROTOC_EXTRA_OUTS .grpc.pb.cc .grpc.pb.h
)
target_include_directories(contrib-libs-googleapis-common-protos PUBLIC
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos
)
target_include_directories(contrib-libs-googleapis-common-protos PRIVATE
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos
)
target_link_libraries(contrib-libs-googleapis-common-protos PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-grpc
  contrib-libs-protobuf
)
target_proto_messages(contrib-libs-googleapis-common-protos PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/api/annotations.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/api/auth.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/api/backend.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/api/billing.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/api/client.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/api/config_change.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/api/consumer.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/api/context.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/api/control.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/api/distribution.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/api/documentation.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/api/endpoint.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/api/field_behavior.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/api/http.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/api/httpbody.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/api/label.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/api/launch_stage.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/api/log.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/api/logging.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/api/metric.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/api/monitored_resource.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/api/monitoring.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/api/quota.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/api/resource.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/api/routing.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/api/service.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/api/source_info.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/api/system_parameter.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/api/usage.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/cloud/extended_operations.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/cloud/location/locations.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/gapic/metadata/gapic_metadata.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/logging/type/http_request.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/logging/type/log_severity.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/longrunning/operations.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/rpc/code.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/rpc/context/attribute_context.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/rpc/error_details.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/rpc/status.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/type/calendar_period.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/type/color.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/type/date.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/type/datetime.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/type/dayofweek.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/type/expr.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/type/fraction.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/type/latlng.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/type/money.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/type/month.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/type/postal_address.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/type/quaternion.proto
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos/google/type/timeofday.proto
)
target_sources(contrib-libs-googleapis-common-protos PRIVATE
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/api/annotations.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/api/auth.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/api/backend.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/api/billing.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/api/client.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/api/config_change.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/api/consumer.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/api/context.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/api/control.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/api/distribution.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/api/documentation.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/api/endpoint.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/api/field_behavior.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/api/http.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/api/httpbody.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/api/label.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/api/launch_stage.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/api/log.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/api/logging.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/api/metric.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/api/monitored_resource.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/api/monitoring.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/api/quota.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/api/resource.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/api/routing.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/api/service.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/api/source_info.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/api/system_parameter.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/api/usage.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/cloud/extended_operations.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/cloud/location/locations.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/gapic/metadata/gapic_metadata.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/logging/type/http_request.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/logging/type/log_severity.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/longrunning/operations.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/rpc/code.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/rpc/context/attribute_context.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/rpc/error_details.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/rpc/status.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/type/calendar_period.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/type/color.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/type/date.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/type/datetime.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/type/dayofweek.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/type/expr.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/type/fraction.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/type/latlng.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/type/money.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/type/month.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/type/postal_address.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/type/quaternion.pb.cc
  ${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos/google/type/timeofday.pb.cc
)
target_proto_addincls(contrib-libs-googleapis-common-protos
  ./contrib/libs/googleapis-common-protos
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos
  ${CMAKE_BINARY_DIR}
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}/contrib/libs/googleapis-common-protos
  ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf/src
  ${CMAKE_BINARY_DIR}
  ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf/src
)
target_proto_outs(contrib-libs-googleapis-common-protos
  --cpp_out=${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos
  --cpp_styleguide_out=${CMAKE_BINARY_DIR}/contrib/libs/googleapis-common-protos
)
target_proto_plugin(contrib-libs-googleapis-common-protos
  grpc_cpp
  grpc_cpp
)
