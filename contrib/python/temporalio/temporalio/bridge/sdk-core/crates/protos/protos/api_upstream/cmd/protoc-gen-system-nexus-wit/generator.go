package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"

	nexusannotationsv1 "github.com/nexus-rpc/nexus-proto-annotations/go/nexusannotations/v1"
	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

type params struct {
	nexGen       string
	output       string
	input        string
	linkedInputs []string
}

// parseParams parses the comma-separated key=value parameter string provided by protoc.
//
//   - output: required. Path to the WIT file to generate, relative to the
//     --system-nexus-wit_out directory. Example: "nexus/temporal-system.wit".
//
//   - nex_gen: optional. Path to the nex-gen binary. Defaults to "nex-gen".
//
//   - linked_input: optional, repeatable. Additional WIT input passed to
//     nex-gen after the main input.
//
//   - input: optional. Existing WIT file to update. Defaults to output, so
//     existing handwritten annotations and type refinements are preserved when
//     regenerating in place.
func parseParams(raw string) (params, error) {
	p := params{
		nexGen: "nex-gen",
	}
	if raw == "" {
		return p, nil
	}
	for kv := range strings.SplitSeq(raw, ",") {
		key, value, ok := strings.Cut(kv, "=")
		if !ok {
			return p, fmt.Errorf("invalid parameter %q: expected key=value", kv)
		}
		switch key {
		case "nex_gen":
			p.nexGen = value
		case "output":
			p.output = value
		case "input":
			p.input = value
		case "linked_input":
			p.linkedInputs = append(p.linkedInputs, value)
		default:
			return p, fmt.Errorf("unknown parameter %q", key)
		}
	}
	return p, nil
}

func generate(gen *protogen.Plugin) error {
	p, err := parseParams(gen.Request.GetParameter())
	if err != nil {
		return err
	}
	if p.output == "" {
		return fmt.Errorf("missing required output parameter")
	}
	if p.input == "" {
		p.input = p.output
	}

	rpcs := exposedRPCs(gen)
	if len(rpcs) == 0 {
		return fmt.Errorf("no proto RPCs are marked as exposed Nexus operations")
	}

	tempDir, err := os.MkdirTemp("", "system-nexus-wit-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tempDir)

	descriptorPath := filepath.Join(tempDir, "temporal_api.bin")
	if err := writeDescriptorSet(gen, descriptorPath); err != nil {
		return err
	}

	tempOutput := filepath.Join(tempDir, "system-nexus.wit")
	input := ""
	if _, err := os.Stat(p.input); err == nil {
		if err := copyFile(p.input, tempOutput); err != nil {
			return err
		}
		input = tempOutput
	} else if !os.IsNotExist(err) {
		return err
	}

	for _, rpc := range rpcs {
		if err := runAddRPC(p.nexGen, descriptorPath, rpc, tempOutput, input, p.linkedInputs); err != nil {
			return err
		}
		input = tempOutput
	}

	content, err := os.ReadFile(tempOutput)
	if err != nil {
		return err
	}
	_, err = gen.NewGeneratedFile(p.output, "").Write(content)
	return err
}

func exposedRPCs(gen *protogen.Plugin) []string {
	var rpcs []string
	for _, f := range gen.Files {
		if !f.Generate {
			continue
		}
		for _, svc := range f.Services {
			for _, m := range svc.Methods {
				if isExposedOperation(m) {
					rpcs = append(rpcs, string(m.Desc.FullName()))
				}
			}
		}
	}
	return rpcs
}

func isExposedOperation(m *protogen.Method) bool {
	opts, ok := m.Desc.Options().(*descriptorpb.MethodOptions)
	if !ok || opts == nil {
		return false
	}
	if !proto.HasExtension(opts, nexusannotationsv1.E_Operation) {
		return false
	}
	tags := proto.GetExtension(opts, nexusannotationsv1.E_Operation).(*nexusannotationsv1.OperationOptions).GetTags()
	return slices.Contains(tags, "exposed")
}

func writeDescriptorSet(gen *protogen.Plugin, descriptorPath string) error {
	data, err := proto.Marshal(&descriptorpb.FileDescriptorSet{
		File: gen.Request.GetProtoFile(),
	})
	if err != nil {
		return err
	}
	return os.WriteFile(descriptorPath, data, 0o644)
}

func runAddRPC(nexGen string, descriptors string, rpc string, output string, input string, linkedInputs []string) error {
	args := []string{
		"add-rpc",
		"--descriptors", descriptors,
		"--rpc", rpc,
		"--output", output,
	}
	if input != "" {
		args = append(args, "--input", input)
	}
	for _, linkedInput := range linkedInputs {
		args = append(args, "--input", linkedInput)
	}

	command := exec.Command(nexGen, args...)
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if err := command.Run(); err != nil {
		return fmt.Errorf("%s %s: %w", nexGen, strings.Join(args, " "), err)
	}
	return nil
}

func copyFile(source string, destination string) error {
	content, err := os.ReadFile(source)
	if err != nil {
		return err
	}
	return os.WriteFile(destination, content, 0o644)
}
