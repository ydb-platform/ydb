mv src thrift
sed '/#include "ext\/protocol.tcc"/ r thrift/ext/protocol.tcc' --in-place thrift/ext/protocol.h
sed 's|#include "ext\/protocol.tcc"||' --in-place thrift/ext/protocol.h
rm thrift/ext/protocol.tcc
sed -E 's|(\s+)src/|\1thrift/|g' --in-place ya.make
