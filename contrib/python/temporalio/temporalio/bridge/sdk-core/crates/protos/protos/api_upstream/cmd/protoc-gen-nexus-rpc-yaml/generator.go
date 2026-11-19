package main

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	nexusannotationsv1 "github.com/nexus-rpc/nexus-proto-annotations/go/nexusannotations/v1"
	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"gopkg.in/yaml.v3"
)

// params holds the parsed protoc plugin options.
// Passed via --nexus-rpc-yaml_opt=key=value (multiple opts are comma-joined by protoc).
//
//   - nexus-rpc_langs_out: optional. Output path for the langs YAML.
//     If empty, nothing is written.
//     Example: "nexus/temporal-proto-models-nexusrpc.yaml"
//
//   - python_package_prefix: optional. Dot-separated package prefix for $pythonRef.
//     The last two path segments of the go_package ({service}/v{n}) are appended.
//     Example: "temporalio.api" → "temporalio.api.workflowservice.v1.TypeName"
//     If empty, $pythonRef is omitted.
//
//   - typescript_package_prefix: optional. Scoped package prefix for $typescriptRef.
//     The last two path segments of the go_package ({service}/v{n}) are appended.
//     Example: "@temporalio/api" → "@temporalio/api/workflowservice/v1.TypeName"
//     If empty, $typescriptRef is omitted.
//
//   - include_operation_tags: optional, repeatable. Only include operations whose tags
//     contain at least one of these values. If empty, all annotated operations are included
//     (subject to exclude_operation_tags). Specify multiple times for multiple tags.
//     Example: include_operation_tags=exposed
//
//   - exclude_operation_tags: optional, repeatable. Exclude operations whose tags contain
//     any of these values. Applied after include_operation_tags.
//     Example: exclude_operation_tags=internal
type params struct {
	nexusRpcLangsOut        string
	pythonPackagePrefix     string
	typescriptPackagePrefix string
	includeOperationTags    []string
	excludeOperationTags    []string
}

// parseParams parses the comma-separated key=value parameter string provided by protoc.
func parseParams(raw string) (params, error) {
	var p params
	if raw == "" {
		return p, nil
	}
	for kv := range strings.SplitSeq(raw, ",") {
		key, value, ok := strings.Cut(kv, "=")
		if !ok {
			return p, fmt.Errorf("invalid parameter %q: expected key=value", kv)
		}
		switch key {
		case "nexus-rpc_langs_out":
			p.nexusRpcLangsOut = value
		case "python_package_prefix":
			p.pythonPackagePrefix = value
		case "typescript_package_prefix":
			p.typescriptPackagePrefix = value
		case "include_operation_tags":
			p.includeOperationTags = append(p.includeOperationTags, value)
		case "exclude_operation_tags":
			p.excludeOperationTags = append(p.excludeOperationTags, value)
		default:
			return p, fmt.Errorf("unknown parameter %q", key)
		}
	}
	return p, nil
}

// shouldIncludeOperation returns true if the method's nexus operation tags pass
// the include/exclude filters. Mirrors the logic from protoc-gen-go-nexus:
//  1. Method must have the nexus operation extension set.
//  2. If includeOperationTags is non-empty, at least one of the method's tags must match.
//  3. If excludeOperationTags is non-empty, none of the method's tags may match.
func shouldIncludeOperation(p params, m *protogen.Method) bool {
	opts, ok := m.Desc.Options().(*descriptorpb.MethodOptions)
	if !ok || opts == nil {
		return false
	}
	if !proto.HasExtension(opts, nexusannotationsv1.E_Operation) {
		return false
	}
	tags := proto.GetExtension(opts, nexusannotationsv1.E_Operation).(*nexusannotationsv1.OperationOptions).GetTags()
	if len(p.includeOperationTags) > 0 && !slices.ContainsFunc(p.includeOperationTags, func(t string) bool {
		return slices.Contains(tags, t)
	}) {
		return false
	}
	return !slices.ContainsFunc(p.excludeOperationTags, func(t string) bool {
		return slices.Contains(tags, t)
	})
}

func generate(gen *protogen.Plugin) error {
	p, err := parseParams(gen.Request.GetParameter())
	if err != nil {
		return err
	}

	langsDoc := newDoc()
	hasOps := false

	for _, f := range gen.Files {
		if !f.Generate {
			continue
		}
		for _, svc := range f.Services {
			for _, m := range svc.Methods {
				if !shouldIncludeOperation(p, m) {
					continue
				}
				svcName := string(svc.Desc.FullName())
				methodName := string(m.Desc.Name())
				hasOps = true
				addOperation(langsDoc, svcName, methodName,
					langRefs(p, f.Desc, m.Input.Desc),
					langRefs(p, f.Desc, m.Output.Desc),
				)
			}
		}
	}

	if !hasOps {
		return nil
	}
	if p.nexusRpcLangsOut != "" {
		return writeFile(gen, p.nexusRpcLangsOut, langsDoc)
	}
	return nil
}

// langRefs builds the map of language-specific type refs for a message.
//
// Go, Java, dotnet, and Ruby refs are derived from proto file-level package options.
// Python and TypeScript refs require the corresponding prefix params to be set; if
// empty they are omitted. Both use the last two path segments of go_package
// ({service}/v{n}), dropping any intermediate grouping directory.
func langRefs(p params, file protoreflect.FileDescriptor, msg protoreflect.MessageDescriptor) map[string]string {
	opts, ok := file.Options().(*descriptorpb.FileOptions)
	if !ok || opts == nil {
		return nil
	}
	name := string(msg.Name())
	refs := make(map[string]string)

	if pkg := opts.GetGoPackage(); pkg != "" {
		// strip the ";alias" suffix (e.g. "go.temporal.io/api/workflowservice/v1;workflowservice")
		pkg = strings.SplitN(pkg, ";", 2)[0]
		refs["$goRef"] = pkg + "." + name

		segments := strings.Split(pkg, "/")
		if len(segments) >= 2 {
			tail := segments[len(segments)-2] + "/" + segments[len(segments)-1]
			if p.pythonPackagePrefix != "" {
				dotTail := strings.ReplaceAll(tail, "/", ".")
				refs["$pythonRef"] = p.pythonPackagePrefix + "." + dotTail + "." + name
			}
			if p.typescriptPackagePrefix != "" {
				refs["$typescriptRef"] = p.typescriptPackagePrefix + "/" + tail + "." + name
			}
		}
	}
	if pkg := opts.GetJavaPackage(); pkg != "" {
		refs["$javaRef"] = pkg + "." + name
	}
	if pkg := opts.GetRubyPackage(); pkg != "" {
		refs["$rubyRef"] = pkg + "::" + name
	}
	if pkg := opts.GetCsharpNamespace(); pkg != "" {
		refs["$dotnetRef"] = pkg + "." + name
	}
	if len(refs) == 0 {
		return nil
	}
	return refs
}

// newDoc creates a yaml.Node document with the "nexusrpc: 1.0.0" header
// and an empty "services" mapping node.
func newDoc() *yaml.Node {
	doc := &yaml.Node{Kind: yaml.DocumentNode}
	root := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	doc.Content = []*yaml.Node{root}
	root.Content = append(root.Content,
		scalarNode("nexusrpc"),
		scalarNode("1.0.0"),
		scalarNode("services"),
		&yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"},
	)
	return doc
}

// servicesNode returns the "services" mapping node from a doc created by newDoc.
func servicesNode(doc *yaml.Node) *yaml.Node {
	root := doc.Content[0]
	for i := 0; i < len(root.Content)-1; i += 2 {
		if root.Content[i].Value == "services" {
			return root.Content[i+1]
		}
	}
	panic("services node not found")
}

// addOperation inserts a service → operation → {input, output} entry into doc.
// Services and operations are inserted in the order first encountered.
func addOperation(doc *yaml.Node, svcName, methodName string, input, output map[string]string) {
	svcs := servicesNode(doc)

	var svcOps *yaml.Node
	for i := 0; i < len(svcs.Content)-1; i += 2 {
		if svcs.Content[i].Value == svcName {
			svcMap := svcs.Content[i+1]
			for j := 0; j < len(svcMap.Content)-1; j += 2 {
				if svcMap.Content[j].Value == "operations" {
					svcOps = svcMap.Content[j+1]
				}
			}
		}
	}
	if svcOps == nil {
		svcMap := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
		svcOps = &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
		svcMap.Content = append(svcMap.Content, scalarNode("operations"), svcOps)
		svcs.Content = append(svcs.Content, scalarNode(svcName), svcMap)
	}

	opNode := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	if len(input) > 0 {
		opNode.Content = append(opNode.Content, scalarNode("input"), mapNode(input))
	}
	if len(output) > 0 {
		opNode.Content = append(opNode.Content, scalarNode("output"), mapNode(output))
	}
	svcOps.Content = append(svcOps.Content, scalarNode(methodName), opNode)
}

// mapNode serializes a map[string]string as a yaml mapping node with keys in sorted order.
func mapNode(m map[string]string) *yaml.Node {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	node := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	for _, k := range keys {
		node.Content = append(node.Content, scalarNode(k), scalarNode(m[k]))
	}
	return node
}

func scalarNode(value string) *yaml.Node {
	return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: value}
}

func writeFile(gen *protogen.Plugin, name string, doc *yaml.Node) error {
	f := gen.NewGeneratedFile(name, "")
	enc := yaml.NewEncoder(f)
	enc.SetIndent(2)
	if err := enc.Encode(doc); err != nil {
		return err
	}
	return enc.Close()
}
