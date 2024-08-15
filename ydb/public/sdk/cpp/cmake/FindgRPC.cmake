if (NOT Threads_FOUND)
  find_package(Threads REQUIRED)
endif()

if (NOT Protobuf_FOUND)
  find_package(Protobuf REQUIRED)
endif()

find_library(gRPC_grpc_LIBRARY
  grpc
  HINTS $ENV{gRPC_ROOT}/lib
)

find_library(gRPC_grpc++_LIBRARY
  grpc++
  HINTS $ENV{gRPC_ROOT}/lib
)

find_path(gRPC_INCLUDE_DIR
  grpcpp/grpcpp.h
  HINTS $ENV{gRPC_ROOT}/include
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(gRPC DEFAULT_MSG gRPC_grpc++_LIBRARY gRPC_grpc_LIBRARY gRPC_INCLUDE_DIR)

mark_as_advanced(gRPC_grcp++_LIBRARIES gRPC_grpc_LIBRARIES gRPC_INCLUDE_DIR)

if (gRPC_FOUND)
  if (NOT TARGET gRPC::grpc)
    add_library(gRPC::grpc UNKNOWN IMPORTED)
    set_target_properties(gRPC::grpc PROPERTIES INTERFACE_INCLUDE_DIRECTORIES "${gRPC_INCLUDE_DIR}")
    set_target_properties(gRPC::grpc PROPERTIES IMPORTED_LOCATION "${gRPC_grpc_LIBRARY}")
    set_property(TARGET gRPC::grpc APPEND PROPERTY INTERFACE_LINK_LIBRARIES 
      protobuf::libprotobuf
      Threads::Threads
    )
  endif()

  if (NOT TARGET gRPC::grpc++)
    add_library(gRPC::grpc++ UNKNOWN IMPORTED)
    set_target_properties(gRPC::grpc++ PROPERTIES INTERFACE_INCLUDE_DIRECTORIES "${gRPC_INCLUDE_DIR}")
    set_target_properties(gRPC::grpc++ PROPERTIES IMPORTED_LOCATION "${gRPC_grpc++_LIBRARY}")
    set_property(TARGET gRPC::grpc++ APPEND PROPERTY INTERFACE_LINK_LIBRARIES 
      gRPC::grpc
      protobuf::libprotobuf
      Threads::Threads
    )
  endif()

  if (NOT TARGET gRPC::grpc_cpp_plugin)
    add_executable(gRPC::grpc_cpp_plugin IMPORTED)
  endif()

  get_target_property(_gRPC_CPP_PLUGIN_EXECUTABLE gRPC::grpc_cpp_plugin IMPORTED_LOCATION)

  if (NOT _gRPC_CPP_PLUGIN_EXECUTABLE)
    find_program(_gRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin DOC "The gRPC C++ plugin for protoc")
    mark_as_advanced(_gRPC_CPP_PLUGIN_EXECUTABLE)
    if (_gRPC_CPP_PLUGIN_EXECUTABLE)
      set_property(TARGET gRPC::grpc_cpp_plugin PROPERTY IMPORTED_LOCATION ${_gRPC_CPP_PLUGIN_EXECUTABLE})
    else()
      set(gRPC_FOUND "grpc_cpp_plugin-NOTFOUND")
    endif()
  endif()
endif()
