package headers

type ContentType string

// String implements stringer interface
func (ct ContentType) String() string {
	return string(ct)
}

type ContentEncoding string

// String implements stringer interface
func (ce ContentEncoding) String() string {
	return string(ce)
}

const (
	ContentTypeKey     = "Content-Type"
	ContentLength      = "Content-Length"
	ContentEncodingKey = "Content-Encoding"

	ContentTypeAny ContentType = "*/*"

	TypeApplicationJSON          ContentType = "application/json"
	TypeApplicationXML           ContentType = "application/xml"
	TypeApplicationOctetStream   ContentType = "application/octet-stream"
	TypeApplicationProtobuf      ContentType = "application/protobuf"
	TypeApplicationMsgpack       ContentType = "application/msgpack"
	TypeApplicationXSolomonSpack ContentType = "application/x-solomon-spack"

	EncodingAny     ContentEncoding = "*"
	EncodingZSTD    ContentEncoding = "zstd"
	EncodingLZ4     ContentEncoding = "lz4"
	EncodingGZIP    ContentEncoding = "gzip"
	EncodingDeflate ContentEncoding = "deflate"

	TypeTextPlain    ContentType = "text/plain"
	TypeTextHTML     ContentType = "text/html"
	TypeTextCSV      ContentType = "text/csv"
	TypeTextCmd      ContentType = "text/cmd"
	TypeTextCSS      ContentType = "text/css"
	TypeTextXML      ContentType = "text/xml"
	TypeTextMarkdown ContentType = "text/markdown"

	TypeImageAny  ContentType = "image/*"
	TypeImageJPEG ContentType = "image/jpeg"
	TypeImageGIF  ContentType = "image/gif"
	TypeImagePNG  ContentType = "image/png"
	TypeImageSVG  ContentType = "image/svg+xml"
	TypeImageTIFF ContentType = "image/tiff"
	TypeImageWebP ContentType = "image/webp"

	TypeVideoMPEG ContentType = "video/mpeg"
	TypeVideoMP4  ContentType = "video/mp4"
	TypeVideoOgg  ContentType = "video/ogg"
	TypeVideoWebM ContentType = "video/webm"
)
