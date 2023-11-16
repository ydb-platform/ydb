// Package wkb is for decoding ESRI's Well Known Binary (WKB) format
// sepcification at https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry#Well-known_binary
package wkb

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/encoding/internal/wkbcommon"
)

var (
	// ErrUnsupportedDataType is returned by Scan methods when asked to scan
	// non []byte data from the database. This should never happen
	// if the driver is acting appropriately.
	ErrUnsupportedDataType = errors.New("wkb: scan value must be []byte")

	// ErrNotWKB is returned when unmarshalling WKB and the data is not valid.
	ErrNotWKB = errors.New("wkb: invalid data")

	// ErrIncorrectGeometry is returned when unmarshalling WKB data into the wrong type.
	// For example, unmarshaling linestring data into a point.
	ErrIncorrectGeometry = errors.New("wkb: incorrect geometry")

	// ErrUnsupportedGeometry is returned when geometry type is not supported by this lib.
	ErrUnsupportedGeometry = errors.New("wkb: unsupported geometry")
)

var commonErrorMap = map[error]error{
	wkbcommon.ErrUnsupportedDataType: ErrUnsupportedDataType,
	wkbcommon.ErrNotWKB:              ErrNotWKB,
	wkbcommon.ErrNotWKBHeader:        ErrNotWKB,
	wkbcommon.ErrIncorrectGeometry:   ErrIncorrectGeometry,
	wkbcommon.ErrUnsupportedGeometry: ErrUnsupportedGeometry,
}

func mapCommonError(err error) error {
	e, ok := commonErrorMap[err]
	if ok {
		return e
	}

	return err
}

// DefaultByteOrder is the order used for marshalling or encoding
// is none is specified.
var DefaultByteOrder binary.ByteOrder = binary.LittleEndian

// An Encoder will encode a geometry as WKB to the writer given at
// creation time.
type Encoder struct {
	e *wkbcommon.Encoder
}

// MustMarshal will encode the geometry and panic on error.
// Currently there is no reason to error during geometry marshalling.
func MustMarshal(geom orb.Geometry, byteOrder ...binary.ByteOrder) []byte {
	d, err := Marshal(geom, byteOrder...)
	if err != nil {
		panic(err)
	}

	return d
}

// Marshal encodes the geometry with the given byte order.
func Marshal(geom orb.Geometry, byteOrder ...binary.ByteOrder) ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, wkbcommon.GeomLength(geom, false)))

	e := NewEncoder(buf)
	if len(byteOrder) > 0 {
		e.SetByteOrder(byteOrder[0])
	}

	err := e.Encode(geom)
	if err != nil {
		return nil, err
	}

	if buf.Len() == 0 {
		return nil, nil
	}

	return buf.Bytes(), nil
}

// MarshalToHex will encode the geometry into a hex string representation of the binary wkb.
func MarshalToHex(geom orb.Geometry, byteOrder ...binary.ByteOrder) (string, error) {
	data, err := Marshal(geom, byteOrder...)
	if err != nil {
		return "", err
	}

	return hex.EncodeToString(data), nil
}

// MustMarshalToHex will encode the geometry and panic on error.
// Currently there is no reason to error during geometry marshalling.
func MustMarshalToHex(geom orb.Geometry, byteOrder ...binary.ByteOrder) string {
	d, err := MarshalToHex(geom, byteOrder...)
	if err != nil {
		panic(err)
	}

	return d
}

// NewEncoder creates a new Encoder for the given writer.
func NewEncoder(w io.Writer) *Encoder {
	e := wkbcommon.NewEncoder(w)
	e.SetByteOrder(DefaultByteOrder)
	return &Encoder{e: e}
}

// SetByteOrder will override the default byte order set when
// the encoder was created.
func (e *Encoder) SetByteOrder(bo binary.ByteOrder) *Encoder {
	e.e.SetByteOrder(bo)
	return e
}

// Encode will write the geometry encoded as WKB to the given writer.
func (e *Encoder) Encode(geom orb.Geometry) error {
	return e.e.Encode(geom, 0)
}

// Decoder can decoder WKB geometry off of the stream.
type Decoder struct {
	d *wkbcommon.Decoder
}

// Unmarshal will decode the type into a Geometry.
func Unmarshal(data []byte) (orb.Geometry, error) {
	g, _, err := wkbcommon.Unmarshal(data)
	if err != nil {
		return nil, mapCommonError(err)
	}

	return g, nil
}

// NewDecoder will create a new WKB decoder.
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{
		d: wkbcommon.NewDecoder(r),
	}
}

// Decode will decode the next geometry off of the stream.
func (d *Decoder) Decode() (orb.Geometry, error) {
	g, _, err := d.d.Decode()
	if err != nil {
		return nil, mapCommonError(err)
	}

	return g, nil
}
