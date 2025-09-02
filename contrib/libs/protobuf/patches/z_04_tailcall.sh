set -xue

cat << EOF >> src/google/protobuf/port_def.inc

#if defined(__clang__) && defined(_WIN32)
#undef PROTOBUF_MUSTTAIL
#undef PROTOBUF_TAILCALL
#define PROTOBUF_MUSTTAIL
#define PROTOBUF_TAILCALL false
#endif
EOF
