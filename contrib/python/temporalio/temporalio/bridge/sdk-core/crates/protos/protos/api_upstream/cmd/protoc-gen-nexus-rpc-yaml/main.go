// protoc-gen-nexus-rpc-yaml is a protoc plugin that generates nexus/temporal-proto-models-nexusrpc.yaml
// from proto service methods annotated with option (nexusannotations.v1.operation).tags = "exposed".
package main

import (
	"google.golang.org/protobuf/compiler/protogen"
)

func main() {
	protogen.Options{}.Run(func(gen *protogen.Plugin) error {
		return generate(gen)
	})
}
