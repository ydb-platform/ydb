GO_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(v1.34.2)

SRCS(
    api_export.go
    checkinit.go
    codec_extension.go
    codec_field.go
    codec_gen.go
    codec_map.go
    codec_map_go112.go
    codec_message.go
    codec_messageset.go
    codec_tables.go
    codec_unsafe.go
    convert.go
    convert_list.go
    convert_map.go
    decode.go
    encode.go
    enum.go
    extension.go
    legacy_enum.go
    legacy_export.go
    legacy_extension.go
    legacy_file.go
    legacy_message.go
    merge.go
    merge_gen.go
    message.go
    message_reflect.go
    message_reflect_field.go
    message_reflect_gen.go
    pointer_unsafe.go
    validate.go
    weak.go
)

END()

RECURSE(
    # gotest
)
