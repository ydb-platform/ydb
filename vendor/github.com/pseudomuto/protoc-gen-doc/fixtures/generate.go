package fixtures

//go:generate protoc --descriptor_set_out=fileset.pb --include_imports --include_source_info -I. -I../thirdparty Booking.proto Vehicle.proto nested/Book.proto

// Compiling proto3 optional fields requires using protoc >=3.12.x and passing the --experimental_allow_proto3_optional flag.
// Rather than use this flag to compile all of the protocol buffers (which would eliminate test coverage for descriptors
// compiled without the flag), only pass this flag when compiling the one message explicitly testing proto3 optional fields.
// Once this feature is no longer behind an experimental flag, compilation of Cookie.proto can be moved to the above protoc command.
//go:generate protoc --experimental_allow_proto3_optional --descriptor_set_out=cookie.pb --include_imports --include_source_info -I. -I../thirdparty Cookie.proto
