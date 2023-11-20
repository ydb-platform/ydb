package wkbcommon

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/paulmach/orb"
)

// byteOrder represents little or big endian encoding.
// We don't use binary.ByteOrder because that is an interface
// that leaks to the heap all over the place.
type byteOrder int

const bigEndian byteOrder = 0
const littleEndian byteOrder = 1

const (
	pointType              uint32 = 1
	lineStringType         uint32 = 2
	polygonType            uint32 = 3
	multiPointType         uint32 = 4
	multiLineStringType    uint32 = 5
	multiPolygonType       uint32 = 6
	geometryCollectionType uint32 = 7

	ewkbType uint32 = 0x20000000
)

const (
	// limits so that bad data can't come in and preallocate tons of memory.
	// Well formed data with less elements will allocate the correct amount just fine.
	MaxPointsAlloc = 10000
	MaxMultiAlloc  = 100
)

// DefaultByteOrder is the order used for marshalling or encoding
// is none is specified.
var DefaultByteOrder binary.ByteOrder = binary.LittleEndian

// An Encoder will encode a geometry as (E)WKB to the writer given at
// creation time.
type Encoder struct {
	buf []byte

	w     io.Writer
	order binary.ByteOrder
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
func Marshal(geom orb.Geometry, srid int, byteOrder ...binary.ByteOrder) ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, GeomLength(geom, srid != 0)))

	e := NewEncoder(buf)
	if len(byteOrder) > 0 {
		e.SetByteOrder(byteOrder[0])
	}

	err := e.Encode(geom, srid)
	if err != nil {
		return nil, err
	}

	if buf.Len() == 0 {
		return nil, nil
	}

	return buf.Bytes(), nil
}

// NewEncoder creates a new Encoder for the given writer.
func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{
		w:     w,
		order: DefaultByteOrder,
	}
}

// SetByteOrder will override the default byte order set when
// the encoder was created.
func (e *Encoder) SetByteOrder(bo binary.ByteOrder) {
	e.order = bo
}

// Encode will write the geometry encoded as (E)WKB to the given writer.
func (e *Encoder) Encode(geom orb.Geometry, srid int) error {
	if geom == nil {
		return nil
	}

	switch g := geom.(type) {
	// nil values should not write any data. Empty sizes will still
	// write an empty version of that type.
	case orb.MultiPoint:
		if g == nil {
			return nil
		}
	case orb.LineString:
		if g == nil {
			return nil
		}
	case orb.MultiLineString:
		if g == nil {
			return nil
		}
	case orb.Polygon:
		if g == nil {
			return nil
		}
	case orb.MultiPolygon:
		if g == nil {
			return nil
		}
	case orb.Collection:
		if g == nil {
			return nil
		}
	// deal with types that are not supported by wkb
	case orb.Ring:
		if g == nil {
			return nil
		}
		geom = orb.Polygon{g}
	case orb.Bound:
		geom = g.ToPolygon()
	}

	var b []byte
	if e.order == binary.LittleEndian {
		b = []byte{1}
	} else {
		b = []byte{0}
	}

	_, err := e.w.Write(b)
	if err != nil {
		return err
	}

	if e.buf == nil {
		e.buf = make([]byte, 16)
	}

	switch g := geom.(type) {
	case orb.Point:
		return e.writePoint(g, srid)
	case orb.MultiPoint:
		return e.writeMultiPoint(g, srid)
	case orb.LineString:
		return e.writeLineString(g, srid)
	case orb.MultiLineString:
		return e.writeMultiLineString(g, srid)
	case orb.Polygon:
		return e.writePolygon(g, srid)
	case orb.MultiPolygon:
		return e.writeMultiPolygon(g, srid)
	case orb.Collection:
		return e.writeCollection(g, srid)
	}

	panic("unsupported type")
}

func (e *Encoder) writeTypePrefix(t uint32, l int, srid int) error {
	if srid == 0 {
		e.order.PutUint32(e.buf, t)
		e.order.PutUint32(e.buf[4:], uint32(l))

		_, err := e.w.Write(e.buf[:8])
		return err
	}

	e.order.PutUint32(e.buf, t|ewkbType)
	e.order.PutUint32(e.buf[4:], uint32(srid))
	e.order.PutUint32(e.buf[8:], uint32(l))

	_, err := e.w.Write(e.buf[:12])
	return err
}

// Decoder can decoder (E)WKB geometry off of the stream.
type Decoder struct {
	r io.Reader
}

// Unmarshal will decode the type into a Geometry.
func Unmarshal(data []byte) (orb.Geometry, int, error) {
	order, typ, srid, geomData, err := unmarshalByteOrderType(data)
	if err != nil {
		return nil, 0, err
	}

	var g orb.Geometry

	switch typ {
	case pointType:
		g, err = unmarshalPoint(order, geomData)
	case multiPointType:
		g, err = unmarshalMultiPoint(order, geomData)
	case lineStringType:
		g, err = unmarshalLineString(order, geomData)
	case multiLineStringType:
		g, err = unmarshalMultiLineString(order, geomData)
	case polygonType:
		g, err = unmarshalPolygon(order, geomData)
	case multiPolygonType:
		g, err = unmarshalMultiPolygon(order, geomData)
	case geometryCollectionType:
		g, _, err := NewDecoder(bytes.NewReader(data)).Decode()
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, 0, ErrNotWKB
		}

		return g, srid, err
	default:
		return nil, 0, ErrUnsupportedGeometry
	}

	if err != nil {
		return nil, 0, err
	}

	return g, srid, nil
}

// NewDecoder will create a new (E)WKB decoder.
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{
		r: r,
	}
}

// Decode will decode the next geometry off of the stream.
func (d *Decoder) Decode() (orb.Geometry, int, error) {
	buf := make([]byte, 8)
	order, typ, srid, err := readByteOrderType(d.r, buf)
	if err != nil {
		return nil, 0, err
	}

	var g orb.Geometry
	switch typ {
	case pointType:
		g, err = readPoint(d.r, order, buf)
	case multiPointType:
		g, err = readMultiPoint(d.r, order, buf)
	case lineStringType:
		g, err = readLineString(d.r, order, buf)
	case multiLineStringType:
		g, err = readMultiLineString(d.r, order, buf)
	case polygonType:
		g, err = readPolygon(d.r, order, buf)
	case multiPolygonType:
		g, err = readMultiPolygon(d.r, order, buf)
	case geometryCollectionType:
		g, err = readCollection(d.r, order, buf)
	default:
		return nil, 0, ErrUnsupportedGeometry
	}

	if err != nil {
		return nil, 0, err
	}

	return g, srid, nil
}

func readByteOrderType(r io.Reader, buf []byte) (byteOrder, uint32, int, error) {
	// the byte order is the first byte
	if _, err := r.Read(buf[:1]); err != nil {
		return 0, 0, 0, err
	}

	var order byteOrder
	if buf[0] == 0 {
		order = bigEndian
	} else if buf[0] == 1 {
		order = littleEndian
	} else {
		return 0, 0, 0, ErrNotWKB
	}

	// the type which is 4 bytes
	typ, err := readUint32(r, order, buf[:4])
	if err != nil {
		return 0, 0, 0, err
	}

	if typ&ewkbType == 0 {
		return order, typ, 0, nil
	}

	srid, err := readUint32(r, order, buf[:4])
	if err != nil {
		return 0, 0, 0, err
	}

	return order, typ & 0x0ff, int(srid), nil
}

func readUint32(r io.Reader, order byteOrder, buf []byte) (uint32, error) {
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, err
	}
	return unmarshalUint32(order, buf), nil
}

func unmarshalByteOrderType(buf []byte) (byteOrder, uint32, int, []byte, error) {
	order, typ, err := byteOrderType(buf)
	if err != nil {
		return 0, 0, 0, nil, err
	}

	if typ&ewkbType == 0 {
		// regular wkb, no srid
		return order, typ & 0x0F, 0, buf[5:], nil
	}

	if len(buf) < 10 {
		return 0, 0, 0, nil, ErrNotWKB
	}

	srid := unmarshalUint32(order, buf[5:])
	return order, typ & 0x0F, int(srid), buf[9:], nil
}

func byteOrderType(buf []byte) (byteOrder, uint32, error) {
	if len(buf) < 6 {
		return 0, 0, ErrNotWKB
	}

	var order byteOrder
	switch buf[0] {
	case 0:
		order = bigEndian
	case 1:
		order = littleEndian
	default:
		return 0, 0, ErrNotWKBHeader
	}

	// the type which is 4 bytes
	typ := unmarshalUint32(order, buf[1:])
	return order, typ, nil
}

func unmarshalUint32(order byteOrder, buf []byte) uint32 {
	if order == littleEndian {
		return binary.LittleEndian.Uint32(buf)
	}
	return binary.BigEndian.Uint32(buf)
}

// GeomLength helps to do preallocation during a marshal.
func GeomLength(geom orb.Geometry, ewkb bool) int {
	ewkbExtra := 0
	if ewkb {
		ewkbExtra = 4
	}

	switch g := geom.(type) {
	case orb.Point:
		return 21 + ewkbExtra
	case orb.MultiPoint:
		return 9 + 21*len(g) + ewkbExtra
	case orb.LineString:
		return 9 + 16*len(g) + ewkbExtra
	case orb.MultiLineString:
		sum := 0
		for _, ls := range g {
			sum += 9 + 16*len(ls)
		}

		return 9 + sum + ewkbExtra
	case orb.Polygon:
		sum := 0
		for _, r := range g {
			sum += 4 + 16*len(r)
		}

		return 9 + sum + ewkbExtra
	case orb.MultiPolygon:
		sum := 0
		for _, c := range g {
			sum += GeomLength(c, false)
		}

		return 9 + sum + ewkbExtra
	case orb.Collection:
		sum := 0
		for _, c := range g {
			sum += GeomLength(c, false)
		}

		return 9 + sum + ewkbExtra
	}

	return 0
}
