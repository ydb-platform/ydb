JAVA_CONTRIB()

LICENSE(Apache-2.0)

PEERDIR(
    contrib/java/io/grpc/grpc-api/1.51.0
    contrib/java/com/google/code/findbugs/jsr305/3.0.2
    contrib/java/com/google/protobuf/protobuf-java/3.21.7
    contrib/java/com/google/api/grpc/proto-google-common-protos/2.9.0
    contrib/java/io/grpc/grpc-protobuf-lite/1.51.0
    contrib/java/com/google/guava/guava/31.1-android
)

JAR_RESOURCE(4628902851)

SRC_RESOURCE(4628902574)

EXCLUDE(contrib/java/com/google/protobuf/protobuf-javalite)

END()
