package wkt

import (
	"bytes"
	"fmt"

	"github.com/paulmach/orb"
)

// Marshal returns a WKT representation of the geometry.
func Marshal(g orb.Geometry) []byte {
	buf := bytes.NewBuffer(nil)

	wkt(buf, g)
	return buf.Bytes()
}

// MarshalString returns a WKT representation of the geometry as a string.
func MarshalString(g orb.Geometry) string {
	buf := bytes.NewBuffer(nil)

	wkt(buf, g)
	return buf.String()
}

func wkt(buf *bytes.Buffer, geom orb.Geometry) {
	switch g := geom.(type) {
	case orb.Point:
		fmt.Fprintf(buf, "POINT(%g %g)", g[0], g[1])
	case orb.MultiPoint:
		if len(g) == 0 {
			buf.Write([]byte(`MULTIPOINT EMPTY`))
			return
		}

		buf.Write([]byte(`MULTIPOINT(`))
		for i, p := range g {
			if i != 0 {
				buf.WriteByte(',')
			}

			fmt.Fprintf(buf, "(%g %g)", p[0], p[1])
		}
		buf.WriteByte(')')
	case orb.LineString:
		if len(g) == 0 {
			buf.Write([]byte(`LINESTRING EMPTY`))
			return
		}

		buf.Write([]byte(`LINESTRING`))
		writeLineString(buf, g)
	case orb.MultiLineString:
		if len(g) == 0 {
			buf.Write([]byte(`MULTILINESTRING EMPTY`))
			return
		}

		buf.Write([]byte(`MULTILINESTRING(`))
		for i, ls := range g {
			if i != 0 {
				buf.WriteByte(',')
			}
			writeLineString(buf, ls)
		}
		buf.WriteByte(')')
	case orb.Ring:
		wkt(buf, orb.Polygon{g})
	case orb.Polygon:
		if len(g) == 0 {
			buf.Write([]byte(`POLYGON EMPTY`))
			return
		}

		buf.Write([]byte(`POLYGON(`))
		for i, r := range g {
			if i != 0 {
				buf.WriteByte(',')
			}
			writeLineString(buf, orb.LineString(r))
		}
		buf.WriteByte(')')
	case orb.MultiPolygon:
		if len(g) == 0 {
			buf.Write([]byte(`MULTIPOLYGON EMPTY`))
			return
		}

		buf.Write([]byte(`MULTIPOLYGON(`))
		for i, p := range g {
			if i != 0 {
				buf.WriteByte(',')
			}
			buf.WriteByte('(')
			for j, r := range p {
				if j != 0 {
					buf.WriteByte(',')
				}
				writeLineString(buf, orb.LineString(r))
			}
			buf.WriteByte(')')
		}
		buf.WriteByte(')')
	case orb.Collection:
		if len(g) == 0 {
			buf.Write([]byte(`GEOMETRYCOLLECTION EMPTY`))
			return
		}
		buf.Write([]byte(`GEOMETRYCOLLECTION(`))
		for i, c := range g {
			if i != 0 {
				buf.WriteByte(',')
			}
			wkt(buf, c)
		}
		buf.WriteByte(')')
	case orb.Bound:
		wkt(buf, g.ToPolygon())
	default:
		panic("unsupported type")
	}
}

func writeLineString(buf *bytes.Buffer, ls orb.LineString) {
	buf.WriteByte('(')
	for i, p := range ls {
		if i != 0 {
			buf.WriteByte(',')
		}

		fmt.Fprintf(buf, "%g %g", p[0], p[1])
	}
	buf.WriteByte(')')
}
