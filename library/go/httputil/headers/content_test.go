package headers_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/ydb-platform/ydb/library/go/httputil/headers"
)

func TestContentTypeConsts(t *testing.T) {
	assert.Equal(t, headers.ContentTypeKey, "Content-Type")

	assert.Equal(t, headers.ContentTypeAny, headers.ContentType("*/*"))

	assert.Equal(t, headers.TypeApplicationJSON, headers.ContentType("application/json"))
	assert.Equal(t, headers.TypeApplicationXML, headers.ContentType("application/xml"))
	assert.Equal(t, headers.TypeApplicationOctetStream, headers.ContentType("application/octet-stream"))
	assert.Equal(t, headers.TypeApplicationProtobuf, headers.ContentType("application/protobuf"))
	assert.Equal(t, headers.TypeApplicationMsgpack, headers.ContentType("application/msgpack"))

	assert.Equal(t, headers.TypeTextPlain, headers.ContentType("text/plain"))
	assert.Equal(t, headers.TypeTextHTML, headers.ContentType("text/html"))
	assert.Equal(t, headers.TypeTextCSV, headers.ContentType("text/csv"))
	assert.Equal(t, headers.TypeTextCmd, headers.ContentType("text/cmd"))
	assert.Equal(t, headers.TypeTextCSS, headers.ContentType("text/css"))
	assert.Equal(t, headers.TypeTextXML, headers.ContentType("text/xml"))
	assert.Equal(t, headers.TypeTextMarkdown, headers.ContentType("text/markdown"))

	assert.Equal(t, headers.TypeImageAny, headers.ContentType("image/*"))
	assert.Equal(t, headers.TypeImageJPEG, headers.ContentType("image/jpeg"))
	assert.Equal(t, headers.TypeImageGIF, headers.ContentType("image/gif"))
	assert.Equal(t, headers.TypeImagePNG, headers.ContentType("image/png"))
	assert.Equal(t, headers.TypeImageSVG, headers.ContentType("image/svg+xml"))
	assert.Equal(t, headers.TypeImageTIFF, headers.ContentType("image/tiff"))
	assert.Equal(t, headers.TypeImageWebP, headers.ContentType("image/webp"))

	assert.Equal(t, headers.TypeVideoMPEG, headers.ContentType("video/mpeg"))
	assert.Equal(t, headers.TypeVideoMP4, headers.ContentType("video/mp4"))
	assert.Equal(t, headers.TypeVideoOgg, headers.ContentType("video/ogg"))
	assert.Equal(t, headers.TypeVideoWebM, headers.ContentType("video/webm"))
}
