find . -name '*.py' -exec sed -e 's/grpc_reflection.v1alpha import reflection_pb2/src.proto.grpc.reflection.v1alpha import reflection_pb2/g' --in-place '{}' ';'
find . -name '*.py' -exec sed -e 's/grpc_reflection.v1alpha.reflection_pb2/src.proto.grpc.reflection.v1alpha.reflection_pb2/g' --in-place '{}' ';'
