package gendoc_test

import (
	"os"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	. "github.com/pseudomuto/protoc-gen-doc"
	"github.com/pseudomuto/protoc-gen-doc/extensions"
	"github.com/pseudomuto/protokit"
	"github.com/pseudomuto/protokit/utils"
	"github.com/stretchr/testify/require"
)

var (
	template    *Template
	bookingFile *File
	vehicleFile *File

	cookieTemplate *Template
	cookieFile     *File
)

func TestMain(m *testing.M) {
	registerTestExtensions()

	set, _ := utils.LoadDescriptorSet("fixtures", "fileset.pb")
	req := utils.CreateGenRequest(set, "Booking.proto", "Vehicle.proto")
	result := protokit.ParseCodeGenRequest(req)

	template = NewTemplate(result)
	bookingFile = template.Files[0]
	vehicleFile = template.Files[1]

	set, _ = utils.LoadDescriptorSet("fixtures", "cookie.pb")
	req = utils.CreateGenRequest(set, "Cookie.proto")
	result = protokit.ParseCodeGenRequest(req)
	cookieTemplate = NewTemplate(result)
	cookieFile = cookieTemplate.Files[0]

	os.Exit(m.Run())
}

func identity(payload interface{}) interface{} { return payload }

var E_ExtendFile = &proto.ExtensionDesc{
	ExtendedType:  (*descriptor.FileOptions)(nil),
	ExtensionType: (*bool)(nil),
	Field:         20000,
	Name:          "com.pseudomuto.protokit.v1.extend_file",
	Tag:           "varint,20000,opt,name=extend_file,json=extendFile",
	Filename:      "extend.proto",
}

var E_ExtendService = &proto.ExtensionDesc{
	ExtendedType:  (*descriptor.ServiceOptions)(nil),
	ExtensionType: (*bool)(nil),
	Field:         20000,
	Name:          "com.pseudomuto.protokit.v1.extend_service",
	Tag:           "varint,20000,opt,name=extend_service,json=extendService",
	Filename:      "extend.proto",
}

var E_ExtendMethod = &proto.ExtensionDesc{
	ExtendedType:  (*descriptor.MethodOptions)(nil),
	ExtensionType: (*bool)(nil),
	Field:         20000,
	Name:          "com.pseudomuto.protokit.v1.extend_method",
	Tag:           "varint,20000,opt,name=extend_method,json=extendMethod",
	Filename:      "extend.proto",
}

var E_ExtendEnum = &proto.ExtensionDesc{
	ExtendedType:  (*descriptor.EnumOptions)(nil),
	ExtensionType: (*bool)(nil),
	Field:         20000,
	Name:          "com.pseudomuto.protokit.v1.extend_enum",
	Tag:           "varint,20000,opt,name=extend_enum,json=extendEnum",
	Filename:      "extend.proto",
}

var E_ExtendEnumValue = &proto.ExtensionDesc{
	ExtendedType:  (*descriptor.EnumValueOptions)(nil),
	ExtensionType: (*bool)(nil),
	Field:         20000,
	Name:          "com.pseudomuto.protokit.v1.extend_enum_value",
	Tag:           "varint,20000,opt,name=extend_enum_value,json=extendEnumValue",
	Filename:      "extend.proto",
}

var E_ExtendMessage = &proto.ExtensionDesc{
	ExtendedType:  (*descriptor.MessageOptions)(nil),
	ExtensionType: (*bool)(nil),
	Field:         20000,
	Name:          "com.pseudomuto.protokit.v1.extend_message",
	Tag:           "varint,20000,opt,name=extend_message,json=extendMessage",
	Filename:      "extend.proto",
}

var E_ExtendField = &proto.ExtensionDesc{
	ExtendedType:  (*descriptor.FieldOptions)(nil),
	ExtensionType: (*bool)(nil),
	Field:         20000,
	Name:          "com.pseudomuto.protokit.v1.extend_field",
	Tag:           "varint,20000,opt,name=extend_field,json=extendField",
	Filename:      "extend.proto",
}

func registerTestExtensions() {
	proto.RegisterExtension(E_ExtendFile)
	extensions.SetTransformer(E_ExtendFile.Name, identity)
	proto.RegisterExtension(E_ExtendService)
	extensions.SetTransformer(E_ExtendService.Name, identity)
	proto.RegisterExtension(E_ExtendMethod)
	extensions.SetTransformer(E_ExtendMethod.Name, identity)
	proto.RegisterExtension(E_ExtendEnum)
	extensions.SetTransformer(E_ExtendEnum.Name, identity)
	proto.RegisterExtension(E_ExtendEnumValue)
	extensions.SetTransformer(E_ExtendEnumValue.Name, identity)
	proto.RegisterExtension(E_ExtendMessage)
	extensions.SetTransformer(E_ExtendMessage.Name, identity)
	proto.RegisterExtension(E_ExtendField)
	extensions.SetTransformer(E_ExtendField.Name, identity)
}

func TestTemplateProperties(t *testing.T) {
	require.Len(t, template.Files, 2)
}

func TestFileProperties(t *testing.T) {
	require.Equal(t, "Booking.proto", bookingFile.Name)
	require.Equal(t, "Booking related messages.\n\nThis file is really just an example. The data model is completely\nfictional.", bookingFile.Description)
	require.Equal(t, "com.example", bookingFile.Package)
	require.True(t, bookingFile.HasEnums)
	require.True(t, bookingFile.HasExtensions)
	require.True(t, bookingFile.HasMessages)
	require.True(t, bookingFile.HasServices)
	require.NotEmpty(t, bookingFile.Options)
	require.True(t, *bookingFile.Option(E_ExtendFile.Name).(*bool))
}

func TestFileEnumProperties(t *testing.T) {
	enum := findEnum("BookingStatus.StatusCode", bookingFile)
	require.Equal(t, "StatusCode", enum.Name)
	require.Equal(t, "BookingStatus.StatusCode", enum.LongName)
	require.Equal(t, "com.example.BookingStatus.StatusCode", enum.FullName)
	require.Equal(t, "A flag for the status result.", enum.Description)
	require.Len(t, enum.Values, 2)

	expectedValues := []*EnumValue{
		{Name: "OK", Number: "200", Description: "OK result."},
		{Name: "BAD_REQUEST", Number: "400", Description: "BAD result."},
	}

	for idx, value := range enum.Values {
		require.Equal(t, expectedValues[idx], value)
	}

	enum = findEnum("BookingType", bookingFile)
	require.NotEmpty(t, enum.Options)
	require.True(t, *enum.Option(E_ExtendEnum.Name).(*bool))
	require.Contains(t, enum.ValueOptions(), E_ExtendEnumValue.Name)
	require.NotEmpty(t, enum.ValuesWithOption(E_ExtendEnumValue.Name))

	for _, value := range enum.Values {
		if value.Name == "FUTURE" {
			require.NotEmpty(t, value.Options)
			require.True(t, *value.Option(E_ExtendEnumValue.Name).(*bool))
		}
	}
}

func TestFileExtensionProperties(t *testing.T) {
	ext := findExtension("BookingStatus.country", bookingFile)
	require.Equal(t, "country", ext.Name)
	require.Equal(t, "BookingStatus.country", ext.LongName)
	require.Equal(t, "com.example.BookingStatus.country", ext.FullName)
	require.Equal(t, "The country the booking occurred in.", ext.Description)
	require.Equal(t, "optional", ext.Label)
	require.Equal(t, "string", ext.Type)
	require.Equal(t, "string", ext.LongType)
	require.Equal(t, "string", ext.FullType)
	require.Equal(t, 100, ext.Number)
	require.Equal(t, "china", ext.DefaultValue)
	require.Equal(t, "BookingStatus", ext.ContainingType)
	require.Equal(t, "BookingStatus", ext.ContainingLongType)
	require.Equal(t, "com.example.BookingStatus", ext.ContainingFullType)
}

func TestMessageProperties(t *testing.T) {
	msg := findMessage("Vehicle", vehicleFile)
	require.Equal(t, "Vehicle", msg.Name)
	require.Equal(t, "Vehicle", msg.LongName)
	require.Equal(t, "com.example.Vehicle", msg.FullName)
	require.Equal(t, "Represents a vehicle that can be hired.", msg.Description)
	require.False(t, msg.HasExtensions)
	require.True(t, msg.HasFields)
	require.NotEmpty(t, msg.Options)
	require.True(t, *msg.Option(E_ExtendMessage.Name).(*bool))
	require.Contains(t, msg.FieldOptions(), E_ExtendField.Name)
	require.NotEmpty(t, msg.FieldsWithOption(E_ExtendField.Name))
}

func TestNestedMessageProperties(t *testing.T) {
	msg := findMessage("Vehicle.Category", vehicleFile)
	require.Equal(t, "Category", msg.Name)
	require.Equal(t, "Vehicle.Category", msg.LongName)
	require.Equal(t, "com.example.Vehicle.Category", msg.FullName)
	require.Equal(t, "Represents a vehicle category. E.g. \"Sedan\" or \"Truck\".", msg.Description)
	require.False(t, msg.HasExtensions)
	require.True(t, msg.HasFields)
}

func TestMultiplyNestedMessages(t *testing.T) {
	require.NotNil(t, findEnum("Vehicle.Engine.FuelType", vehicleFile))
	require.NotNil(t, findMessage("Vehicle.Engine.Stats", vehicleFile))
}

func TestMessageExtensionProperties(t *testing.T) {
	msg := findMessage("Booking", bookingFile)
	require.Len(t, msg.Extensions, 1)

	ext := msg.Extensions[0]
	require.Equal(t, "optional_field_1", ext.Name)
	require.Equal(t, "BookingStatus.optional_field_1", ext.LongName)
	require.Equal(t, "com.example.BookingStatus.optional_field_1", ext.FullName)
	require.Equal(t, "An optional field to be used however you please.", ext.Description)
	require.Equal(t, "optional", ext.Label)
	require.Equal(t, "string", ext.Type)
	require.Equal(t, "string", ext.LongType)
	require.Equal(t, "string", ext.FullType)
	require.Equal(t, 101, ext.Number)
	require.Empty(t, ext.DefaultValue)
	require.Equal(t, "BookingStatus", ext.ContainingType)
	require.Equal(t, "BookingStatus", ext.ContainingLongType)
	require.Equal(t, "com.example.BookingStatus", ext.ContainingFullType)
	require.Equal(t, "Booking", ext.ScopeType)
	require.Equal(t, "Booking", ext.ScopeLongType)
	require.Equal(t, "com.example.Booking", ext.ScopeFullType)
}

func TestFieldProperties(t *testing.T) {
	msg := findMessage("BookingStatus", bookingFile)

	field := findField("id", msg)
	require.Equal(t, "id", field.Name)
	require.Equal(t, "Unique booking status ID.", field.Description)
	require.Equal(t, "required", field.Label)
	require.Equal(t, "int32", field.Type)
	require.Equal(t, "int32", field.LongType)
	require.Equal(t, "int32", field.FullType)
	require.Empty(t, field.DefaultValue)
	require.False(t, field.IsOneof)
	require.NotEmpty(t, field.Options)
	require.True(t, *field.Option(E_ExtendField.Name).(*bool))

	field = findField("status_code", msg)
	require.Equal(t, "status_code", field.Name)
	require.Equal(t, "The status of this status?", field.Description)
	require.Equal(t, "optional", field.Label)
	require.Equal(t, "StatusCode", field.Type)
	require.Equal(t, "BookingStatus.StatusCode", field.LongType)
	require.Equal(t, "com.example.BookingStatus.StatusCode", field.FullType)
	require.Empty(t, field.DefaultValue)
	require.False(t, field.IsOneof)

	field = findField("category", findMessage("Vehicle", vehicleFile))
	require.Equal(t, "category", field.Name)
	require.Equal(t, "Vehicle category.", field.Description)
	require.Empty(t, field.Label) // proto3, neither required, nor optional are valid
	require.Equal(t, "Category", field.Type)
	require.Equal(t, "Vehicle.Category", field.LongType)
	require.Equal(t, "com.example.Vehicle.Category", field.FullType)
	require.Empty(t, field.DefaultValue)
	require.False(t, field.IsOneof)

	field = findField("properties", findMessage("Vehicle", vehicleFile))
	require.Equal(t, "properties", field.Name)
	require.Equal(t, "repeated", field.Label)
	require.Equal(t, "PropertiesEntry", field.Type)
	require.Equal(t, "Vehicle.PropertiesEntry", field.LongType)
	require.Equal(t, "com.example.Vehicle.PropertiesEntry", field.FullType)
	require.Empty(t, field.DefaultValue)
	require.True(t, field.IsMap)
	require.False(t, field.IsOneof)

	field = findField("rates", findMessage("Vehicle", vehicleFile))
	require.Equal(t, "rates", field.Name)
	require.Equal(t, "repeated", field.Label)
	require.Equal(t, "sint32", field.Type)
	require.Equal(t, "sint32", field.LongType)
	require.Equal(t, "sint32", field.FullType)
	require.False(t, field.IsMap)
	require.False(t, field.IsOneof)

	field = findField("kilometers", findMessage("Vehicle", vehicleFile))
	require.Equal(t, "kilometers", field.Name)
	require.Equal(t, "", field.Label)
	require.Equal(t, "int32", field.Type)
	require.Equal(t, "int32", field.LongType)
	require.Equal(t, "int32", field.FullType)
	require.False(t, field.IsMap)
	require.True(t, field.IsOneof)
	require.Equal(t, "travel", field.OneofDecl)

	field = findField("human_name", findMessage("Vehicle", vehicleFile))
	require.Equal(t, "human_name", field.Name)
	require.Equal(t, "", field.Label)
	require.Equal(t, "string", field.Type)
	require.Equal(t, "string", field.LongType)
	require.Equal(t, "string", field.FullType)
	require.False(t, field.IsMap)
	require.True(t, field.IsOneof)
	require.Equal(t, "drivers", field.OneofDecl)
}

func TestFieldPropertiesProto3(t *testing.T) {
	msg := findMessage("Model", vehicleFile)

	field := findField("id", msg)
	require.Equal(t, "id", field.Name)
	require.Equal(t, "The unique model ID.", field.Description)
	require.Equal(t, "", field.Label)
	require.Equal(t, "string", field.Type)
	require.Equal(t, "string", field.LongType)
	require.Equal(t, "string", field.FullType)
	require.Empty(t, field.DefaultValue)
	require.Empty(t, field.Options)

	field = findField("model_code", msg)
	require.Equal(t, "model_code", field.Name)
	require.Equal(t, "The car model code, e.g. \"PZ003\".", field.Description)
	require.Equal(t, "", field.Label)
	require.Equal(t, "string", field.Type)
	require.Equal(t, "string", field.LongType)
	require.Equal(t, "string", field.FullType)
	require.Empty(t, field.DefaultValue)
	require.Empty(t, field.Options)

	field = findField("daily_hire_rate_dollars", msg)
	require.Equal(t, "daily_hire_rate_dollars", field.Name)
	require.Equal(t, "Dollars per day.", field.Description)
	require.Equal(t, "", field.Label)
	require.Equal(t, "sint32", field.Type)
	require.Equal(t, "sint32", field.LongType)
	require.Equal(t, "sint32", field.FullType)
	require.Empty(t, field.DefaultValue)
	require.Empty(t, field.Options)
}

func TestFieldPropertiesProto3Optional(t *testing.T) {
	msg := findMessage("Cookie", cookieFile)

	field := findField("id", msg)
	require.Equal(t, "id", field.Name)
	require.Equal(t, "The id of the cookie.", field.Description)
	require.Equal(t, "", field.Label)
	require.Equal(t, "string", field.Type)
	require.Equal(t, "string", field.LongType)
	require.Equal(t, "string", field.FullType)
	require.Empty(t, field.DefaultValue)
	require.Empty(t, field.Options)

	field = findField("name", msg)
	require.Equal(t, "name", field.Name)
	require.Equal(t, "The name of the cookie.", field.Description)
	require.Equal(t, "optional", field.Label)
	require.Equal(t, "string", field.Type)
	require.Equal(t, "string", field.LongType)
	require.Equal(t, "string", field.FullType)
	require.Empty(t, field.DefaultValue)
	require.Empty(t, field.Options)

	field = findField("ingredients", msg)
	require.Equal(t, "ingredients", field.Name)
	require.Equal(t, "Ingredients in the cookie.", field.Description)
	require.Equal(t, "repeated", field.Label)
	require.Equal(t, "string", field.Type)
	require.Equal(t, "string", field.LongType)
	require.Equal(t, "string", field.FullType)
	require.Empty(t, field.DefaultValue)
	require.Empty(t, field.Options)
}

func TestServiceProperties(t *testing.T) {
	service := findService("VehicleService", vehicleFile)
	require.Equal(t, "VehicleService", service.Name)
	require.Equal(t, "VehicleService", service.LongName)
	require.Equal(t, "com.example.VehicleService", service.FullName)
	require.Equal(t, "The vehicle service.\n\nManages vehicles and such...", service.Description)
	require.Len(t, service.Methods, 3)
	require.NotEmpty(t, service.Options)
	require.True(t, *service.Option(E_ExtendService.Name).(*bool))
	require.Contains(t, service.MethodOptions(), E_ExtendMethod.Name)
	require.NotEmpty(t, service.MethodsWithOption(E_ExtendMethod.Name))
}

func TestServiceMethodProperties(t *testing.T) {
	service := findService("VehicleService", vehicleFile)

	method := findServiceMethod("AddModels", service)
	require.Equal(t, "AddModels", method.Name)
	require.Equal(t, "creates models", method.Description)
	require.Equal(t, "Model", method.RequestType)
	require.Equal(t, "Model", method.RequestLongType)
	require.Equal(t, "com.example.Model", method.RequestFullType)
	require.True(t, method.RequestStreaming)
	require.Equal(t, "Model", method.ResponseType)
	require.Equal(t, "Model", method.ResponseLongType)
	require.Equal(t, "com.example.Model", method.ResponseFullType)
	require.True(t, method.ResponseStreaming)

	method = findServiceMethod("GetVehicle", service)
	require.Equal(t, "GetVehicle", method.Name)
	require.Equal(t, "Looks up a vehicle by id.", method.Description)
	require.Equal(t, "FindVehicleById", method.RequestType)
	require.Equal(t, "FindVehicleById", method.RequestLongType)
	require.Equal(t, "com.example.FindVehicleById", method.RequestFullType)
	require.False(t, method.RequestStreaming)
	require.Equal(t, "Vehicle", method.ResponseType)
	require.Equal(t, "Vehicle", method.ResponseLongType)
	require.Equal(t, "com.example.Vehicle", method.ResponseFullType)
	require.False(t, method.ResponseStreaming)
	require.NotEmpty(t, method.Options)
	require.True(t, *method.Option(E_ExtendMethod.Name).(*bool))
}

func TestExcludedComments(t *testing.T) {
	message := findMessage("ExcludedMessage", vehicleFile)
	require.Empty(t, message.Description)
	require.Empty(t, findField("name", message).Description)
	require.Empty(t, findField("value", message).Description)

	// just checking that it doesn't exclude everything
	require.Equal(t, "the id of this message.", findField("id", message).Description)
}

func findService(name string, f *File) *Service {
	for _, s := range f.Services {
		if s.Name == name {
			return s
		}
	}

	return nil
}

func findServiceMethod(name string, s *Service) *ServiceMethod {
	for _, m := range s.Methods {
		if m.Name == name {
			return m
		}
	}

	return nil
}

func findEnum(name string, f *File) *Enum {
	for _, enum := range f.Enums {
		if enum.LongName == name {
			return enum
		}
	}

	return nil
}

func findExtension(name string, f *File) *FileExtension {
	for _, ext := range f.Extensions {
		if ext.LongName == name {
			return ext
		}
	}

	return nil
}

func findMessage(name string, f *File) *Message {
	for _, m := range f.Messages {
		if m.LongName == name {
			return m
		}
	}

	return nil
}

func findField(name string, m *Message) *MessageField {
	for _, f := range m.Fields {
		if f.Name == name {
			return f
		}
	}

	return nil
}
