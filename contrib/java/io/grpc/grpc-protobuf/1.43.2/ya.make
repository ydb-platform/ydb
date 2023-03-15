JAVA_CONTRIB()

LICENSE(Apache-2.0)

PEERDIR(
    contrib/java/io/grpc/grpc-api/1.43.2
    contrib/java/com/google/code/findbugs/jsr305/3.0.2
    contrib/java/com/google/protobuf/protobuf-java/3.19.2
    contrib/java/com/google/api/grpc/proto-google-common-protos/2.0.1
    contrib/java/io/grpc/grpc-protobuf-lite/1.43.2
    contrib/java/com/google/guava/guava/30.1.1-android
)

JAR_RESOURCE(2954038238)

SRC_RESOURCE(2954032072)

EXCLUDE(contrib/java/com/google/protobuf/protobuf-javalite/3.19.2)

END()
