package gendoc_test

import (
	"regexp"
	"testing"

	"github.com/golang/protobuf/proto"
	plugin_go "github.com/golang/protobuf/protoc-gen-go/plugin"
	. "github.com/pseudomuto/protoc-gen-doc"
	"github.com/pseudomuto/protokit/utils"
	"github.com/stretchr/testify/require"
)

func TestParseOptionsForBuiltinTemplates(t *testing.T) {
	results := map[string]string{
		"docbook":  "output.xml",
		"html":     "output.html",
		"json":     "output.json",
		"markdown": "output.md",
	}

	for kind, file := range results {
		req := new(plugin_go.CodeGeneratorRequest)
		req.Parameter = proto.String(kind + "," + file)

		options, err := ParseOptions(req)
		require.NoError(t, err)

		renderType, err := NewRenderType(kind)
		require.NoError(t, err)

		require.Equal(t, renderType, options.Type)
		require.Equal(t, file, options.OutputFile)
		require.Empty(t, options.TemplateFile)
	}
}

func TestParseOptionsForSourceRelative(t *testing.T) {
	req := new(plugin_go.CodeGeneratorRequest)
	req.Parameter = proto.String("markdown,index.md,source_relative")
	options, err := ParseOptions(req)
	require.NoError(t, err)
	require.Equal(t, options.SourceRelative, true)

	req.Parameter = proto.String("markdown,index.md,default")
	options, err = ParseOptions(req)
	require.NoError(t, err)
	require.Equal(t, options.SourceRelative, false)

	req.Parameter = proto.String("markdown,index.md")
	options, err = ParseOptions(req)
	require.NoError(t, err)
	require.Equal(t, options.SourceRelative, false)
}

func TestParseOptionsForCustomTemplate(t *testing.T) {
	req := new(plugin_go.CodeGeneratorRequest)
	req.Parameter = proto.String("/path/to/template.tmpl,/base/name/only/output.md")

	options, err := ParseOptions(req)
	require.NoError(t, err)

	require.Equal(t, RenderTypeHTML, options.Type)
	require.Equal(t, "/path/to/template.tmpl", options.TemplateFile)
	require.Equal(t, "output.md", options.OutputFile)
}

func TestParseOptionsForExcludePatterns(t *testing.T) {
	req := new(plugin_go.CodeGeneratorRequest)
	req.Parameter = proto.String(":google/*,notgoogle/*")

	options, err := ParseOptions(req)
	require.NoError(t, err)
	require.Len(t, options.ExcludePatterns, 2)

	pattern0, _ := regexp.Compile("google/*")
	pattern1, _ := regexp.Compile("notgoogle/*")
	require.Equal(t, pattern0.String(), options.ExcludePatterns[0].String())
	require.Equal(t, pattern1.String(), options.ExcludePatterns[1].String())
}

func TestParseOptionsWithInvalidValues(t *testing.T) {
	badValues := []string{
		"markdown",
		"html",
		"/some/path.tmpl",
		"more,than,1,comma",
		"markdown,index.md,unknown",
	}

	for _, value := range badValues {
		req := new(plugin_go.CodeGeneratorRequest)
		req.Parameter = proto.String(value)

		_, err := ParseOptions(req)
		require.Error(t, err)
	}
}

func TestRunPluginForBuiltinTemplate(t *testing.T) {
	set, _ := utils.LoadDescriptorSet("fixtures", "fileset.pb")
	req := utils.CreateGenRequest(set, "Booking.proto", "Vehicle.proto", "nested/Book.proto")
	req.Parameter = proto.String("markdown,/base/name/only/output.md")

	plugin := new(Plugin)
	resp, err := plugin.Generate(req)
	require.NoError(t, err)
	require.Len(t, resp.File, 1)
	require.Equal(t, "output.md", resp.File[0].GetName())
	require.NotEmpty(t, resp.File[0].GetContent())
}

func TestRunPluginForCustomTemplate(t *testing.T) {
	set, _ := utils.LoadDescriptorSet("fixtures", "fileset.pb")
	req := utils.CreateGenRequest(set, "Booking.proto", "Vehicle.proto", "nested/Book.proto")
	req.Parameter = proto.String("resources/html.tmpl,/base/name/only/output.html")

	plugin := new(Plugin)
	resp, err := plugin.Generate(req)
	require.NoError(t, err)
	require.Len(t, resp.File, 1)
	require.Equal(t, "output.html", resp.File[0].GetName())
	require.NotEmpty(t, resp.File[0].GetContent())
}

func TestRunPluginWithInvalidOptions(t *testing.T) {
	req := new(plugin_go.CodeGeneratorRequest)
	req.Parameter = proto.String("html")

	plugin := new(Plugin)
	_, err := plugin.Generate(req)
	require.Error(t, err)
}

func TestRunPluginForSourceRelative(t *testing.T) {
	set, _ := utils.LoadDescriptorSet("fixtures", "fileset.pb")
	req := utils.CreateGenRequest(set, "Booking.proto", "Vehicle.proto", "nested/Book.proto")
	req.Parameter = proto.String("markdown,index.md,source_relative")

	plugin := new(Plugin)
	resp, err := plugin.Generate(req)
	require.NoError(t, err)
	require.Len(t, resp.File, 2)
	expected := map[string]int{"index.md": 1, "nested/index.md": 1}
	require.Contains(t, expected, resp.File[0].GetName())
	delete(expected, resp.File[0].GetName())
	require.Contains(t, expected, resp.File[1].GetName())
	delete(expected, resp.File[1].GetName())

	require.NotEmpty(t, resp.File[0].GetContent())
	require.NotEmpty(t, resp.File[1].GetContent())
}
