GO_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(v1.36.6)

SRCS(
    api_export.go
    api_export_opaque.go
    bitmap.go
    checkinit.go
    codec_extension.go
    codec_field.go
    codec_field_opaque.go
    codec_gen.go
    codec_map.go
    codec_message.go
    codec_message_opaque.go
    codec_messageset.go
    codec_tables.go
    codec_unsafe.go
    convert.go
    convert_list.go
    convert_map.go
    decode.go
    encode.go
    enum.go
    equal.go
    extension.go
    lazy.go
    legacy_enum.go
    legacy_export.go
    legacy_extension.go
    legacy_file.go
    legacy_message.go
    merge.go
    merge_gen.go
    message.go
    message_opaque.go
    message_opaque_gen.go
    message_reflect.go
    message_reflect_field.go
    message_reflect_field_gen.go
    message_reflect_gen.go
    pointer_unsafe.go
    pointer_unsafe_opaque.go
    presence.go
    validate.go
)

END()

RECURSE(
    # gotest
)
