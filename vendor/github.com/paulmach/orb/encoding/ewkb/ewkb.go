package ewkb

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

	// ErrNotEWKB is returned when unmarshalling EWKB and the data is not valid.
	ErrNotEWKB = errors.New("wkb: invalid data")

	// ErrIncorrectGeometry is returned when unmarshalling EWKB data into the wrong type.
	// For example, unmarshaling linestring data into a point.
	ErrIncorrectGeometry = errors.New("wkb: incorrect geometry")

	// ErrUnsupportedGeometry is returned when geometry type is not supported by this lib.
	ErrUnsupportedGeometry = errors.New("wkb: unsupported geometry")
)

var commonErrorMap = map[error]error{
	wkbcommon.ErrUnsupportedDataType: ErrUnsupportedDataType,
	wkbcommon.ErrNotWKB:              ErrNotEWKB,
	wkbcommon.ErrNotWKBHeader:        ErrNotEWKB,
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

// DefaultByteOrder is the order used for marshalling or encoding is none is specified.
var DefaultByteOrder binary.ByteOrder = binary.LittleEndian

// DefaultSRID is set to 4326, a common SRID, which represents spatial data using
// longitude and latitude coordinates on the Earth's surface as defined in the WGS84 standard,
// which is also used for the Global Positioning System (GPS).
// This will be used by the encoder if non is specified.
var DefaultSRID int = 4326

// An Encoder will encode a geometry as EWKB to the writer given at creation time.
type Encoder struct {
	srid int
	e    *wkbcommon.Encoder
}

// MustMarshal will encode the geometry and panic on error.
// Currently there is no reason to error during geometry marshalling.
func MustMarshal(geom orb.Geometry, srid int, byteOrder ...binary.ByteOrder) []byte {
	d, err := Marshal(geom, srid, byteOrder...)
	if err != nil {
		panic(err)
	}

	return d
}

// Marshal encodes the geometry with the given byte order.
// An SRID of 0 will not be included in the encoding and the result will be a wkb encoding of the geometry.
func Marshal(geom orb.Geometry, srid int, byteOrder ...binary.ByteOrder) ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, wkbcommon.GeomLength(geom, srid != 0)))

	e := NewEncoder(buf)
	e.SetSRID(srid)

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

// MarshalToHex will encode the geometry into a hex string representation of the binary ewkb.
func MarshalToHex(geom orb.Geometry, srid int, byteOrder ...binary.ByteOrder) (string, error) {
	data, err := Marshal(geom, srid, byteOrder...)
	if err != nil {
		return "", err
	}

	return hex.EncodeToString(data), nil
}

// MustMarshalToHex will encode the geometry and panic on error.
// Currently there is no reason to error during geometry marshalling.
func MustMarshalToHex(geom orb.Geometry, srid int, byteOrder ...binary.ByteOrder) string {
	d, err := MarshalToHex(geom, srid, byteOrder...)
	if err != nil {
		panic(err)
	}

	return d
}

// NewEncoder creates a new Encoder for the given writer.
func NewEncoder(w io.Writer) *Encoder {
	e := wkbcommon.NewEncoder(w)
	e.SetByteOrder(DefaultByteOrder)
	return &Encoder{e: e, srid: DefaultSRID}
}

// SetByteOrder will override the default byte order set when
// the encoder was created.
func (e *Encoder) SetByteOrder(bo binary.ByteOrder) *Encoder {
	e.e.SetByteOrder(bo)
	return e
}

// SetSRID will override the default srid.
func (e *Encoder) SetSRID(srid int) *Encoder {
	e.srid = srid
	return e
}

// Encode will write the geometry encoded as EWKB to the given writer.
func (e *Encoder) Encode(geom orb.Geometry, srid ...int) error {
	s := e.srid
	if len(srid) > 0 {
		s = srid[0]
	}

	return e.e.Encode(geom, s)
}

// Decoder can decoder WKB geometry off of the stream.
type Decoder struct {
	d *wkbcommon.Decoder
}

// Unmarshal will decode the type into a Geometry.
func Unmarshal(data []byte) (orb.Geometry, int, error) {
	g, srid, err := wkbcommon.Unmarshal(data)
	if err != nil {
		return nil, 0, mapCommonError(err)
	}

	return g, srid, nil
}

// NewDecoder will create a new EWKB decoder.
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{
		d: wkbcommon.NewDecoder(r),
	}
}

// Decode will decode the next geometry off of the stream.
func (d *Decoder) Decode() (orb.Geometry, int, error) {
	g, srid, err := d.d.Decode()
	if err != nil {
		return nil, 0, mapCommonError(err)
	}

	return g, srid, nil
}
