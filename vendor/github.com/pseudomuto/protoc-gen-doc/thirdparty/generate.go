package thirdparty

//go:generate mkdir -p github.com/envoyproxy/protoc-gen-validate/validate
//go:generate curl -fsSL https://github.com/envoyproxy/protoc-gen-validate/raw/master/validate/validate.proto -o github.com/envoyproxy/protoc-gen-validate/validate/validate.proto

//go:generate mkdir -p github.com/mwitkow/go-proto-validators
//go:generate curl -fsSL https://github.com/mwitkow/go-proto-validators/raw/master/validator.proto -o github.com/mwitkow/go-proto-validators/validator.proto

//go:generate mkdir -p github.com/pseudomuto/protokit/fixtures
//go:generate curl -fsSL https://github.com/pseudomuto/protokit/raw/master/fixtures/extend.proto -o github.com/pseudomuto/protokit/fixtures/extend.proto
