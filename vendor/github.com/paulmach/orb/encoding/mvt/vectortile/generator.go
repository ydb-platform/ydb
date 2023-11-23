package vectortile

//-----go:generate protoc --go_out=.,import_path=vectortile:. vector_tile.proto
//go:generate protoc --proto_path=../../../../..:../../../../gogo/protobuf/protobuf:. --gogofast_out=.,import_path=vectortile:. vector_tile.proto
