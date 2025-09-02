package protokit_test

import (
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/protoc-gen-go/plugin"
	"github.com/pseudomuto/protokit"

	"log"
)

type plugin struct{}

func (p *plugin) Generate(r *plugin_go.CodeGeneratorRequest) (*plugin_go.CodeGeneratorResponse, error) {
	descriptors := protokit.ParseCodeGenRequest(r)
	resp := new(plugin_go.CodeGeneratorResponse)

	for _, desc := range descriptors {
		resp.File = append(resp.File, &plugin_go.CodeGeneratorResponse_File{
			Name:    proto.String(desc.GetName() + ".out"),
			Content: proto.String("Some relevant output"),
		})
	}

	return resp, nil
}

// An example of running a custom plugin. This would be in your main.go file.
func ExampleRunPlugin() {
	// in func main() {}
	if err := protokit.RunPlugin(new(plugin)); err != nil {
		log.Fatal(err)
	}
}
