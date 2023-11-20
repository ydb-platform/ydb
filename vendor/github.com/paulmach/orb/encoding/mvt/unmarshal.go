package mvt

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io/ioutil"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/encoding/mvt/vectortile"
	"github.com/paulmach/orb/geojson"
	"github.com/paulmach/protoscan"
)

var ErrDataIsGZipped = errors.New("failed to unmarshal, data possibly gzipped")

// UnmarshalGzipped takes gzipped Mapbox Vector Tile (MVT) data and unzips it
// before decoding it into a set of layers, It does not project the coordinates.
func UnmarshalGzipped(data []byte) (Layers, error) {
	gzreader, err := gzip.NewReader(bytes.NewBuffer(data))
	if err != nil {
		return nil, fmt.Errorf("failed to create gzreader: %v", err)
	}

	decoded, err := ioutil.ReadAll(gzreader)
	if err != nil {
		return nil, fmt.Errorf("failed to unzip: %v", err)
	}

	return Unmarshal(decoded)
}

// Unmarshal takes Mapbox Vector Tile (MVT) data and converts into a
// set of layers, It does not project the coordinates.
func Unmarshal(data []byte) (Layers, error) {
	layers, err := unmarshalTile(data)
	if err != nil && dataIsGZipped(data) {
		return nil, ErrDataIsGZipped
	}

	return layers, err
}

// decoder is here to reuse objects to save allocations/memory.
type decoder struct {
	keys     []string
	values   []interface{}
	features [][]byte

	valMsg *protoscan.Message

	tags *protoscan.Iterator
	geom *protoscan.Iterator
}

func (d *decoder) Reset() {
	d.keys = d.keys[:0]
	d.values = d.values[:0]
	d.features = d.features[:0]
}

func unmarshalTile(data []byte) (Layers, error) {
	d := &decoder{}

	var (
		layers Layers
		m      *protoscan.Message
		err    error
	)

	msg := protoscan.New(data)
	for msg.Next() {
		switch msg.FieldNumber() {
		case 3:
			m, err = msg.Message(m)
			if err != nil {
				return nil, err
			}

			layer, err := d.Layer(m)
			if err != nil {
				return nil, err
			}

			layers = append(layers, layer)
		default:
			msg.Skip()
		}
	}

	if msg.Err() != nil {
		return nil, msg.Err()
	}

	return layers, nil
}

func (d *decoder) Layer(msg *protoscan.Message) (*Layer, error) {
	var err error

	d.Reset()
	layer := &Layer{
		Version: vectortile.Default_Tile_Layer_Version,
		Extent:  vectortile.Default_Tile_Layer_Extent,
	}

	for msg.Next() {
		switch msg.FieldNumber() {
		case 15: // version
			v, err := msg.Uint32()
			if err != nil {
				return nil, err
			}
			layer.Version = v
		case 1: // name
			s, err := msg.String()
			if err != nil {
				return nil, err
			}
			layer.Name = s
		case 2: // feature
			data, err := msg.MessageData()
			if err != nil {
				return nil, err
			}
			d.features = append(d.features, data)
		case 3: // keys
			s, err := msg.String()
			if err != nil {
				return nil, err
			}
			d.keys = append(d.keys, s)
		case 4: // values
			d.valMsg, err = msg.Message(d.valMsg)
			if err != nil {
				return nil, err
			}

			v, err := decodeValueMsg(d.valMsg)
			if err != nil {
				return nil, err
			}
			d.values = append(d.values, v)
		case 5: // extent
			e, err := msg.Uint32()
			if err != nil {
				return nil, err
			}
			layer.Extent = e
		default:
			msg.Skip()
		}
	}

	if msg.Err() != nil {
		return nil, msg.Err()
	}

	layer.Features = make([]*geojson.Feature, len(d.features))
	for i, data := range d.features {
		msg.Reset(data)
		f, err := d.Feature(msg)
		if err != nil {
			return nil, err
		}
		layer.Features[i] = f
	}

	return layer, nil
}

func (d *decoder) Feature(msg *protoscan.Message) (*geojson.Feature, error) {
	feature := &geojson.Feature{Type: "Feature"}
	var geomType vectortile.Tile_GeomType

	for msg.Next() {
		switch msg.FieldNumber() {
		case 1: // id
			id, err := msg.Uint64()
			if err != nil {
				return nil, err
			}
			feature.ID = float64(id)
		case 2: //tags, repeated packed
			var err error
			d.tags, err = msg.Iterator(d.tags)
			if err != nil {
				return nil, err
			}

			count := d.tags.Count(protoscan.WireTypeVarint)
			feature.Properties = make(geojson.Properties, count/2)

			for d.tags.HasNext() {
				k, err := d.tags.Uint32()
				if err != nil {
					return nil, err
				}

				v, err := d.tags.Uint32()
				if err != nil {
					return nil, err
				}

				if len(d.keys) <= int(k) || len(d.values) <= int(v) {
					continue
				}
				feature.Properties[d.keys[k]] = d.values[v]
			}
		case 3: // geomtype
			t, err := msg.Int32()
			if err != nil {
				return nil, err
			}

			geomType = vectortile.Tile_GeomType(t)
		case 4: // geometry
			var err error
			d.geom, err = msg.Iterator(d.geom)
			if err != nil {
				return nil, err
			}
		default:
			msg.Skip()
		}
	}

	if msg.Err() != nil {
		return nil, msg.Err()
	}

	geo, err := d.Geometry(geomType)
	if err != nil {
		return nil, err
	}
	feature.Geometry = geo

	return feature, nil
}

func (d *decoder) Geometry(geomType vectortile.Tile_GeomType) (orb.Geometry, error) {
	gd := &geomDecoder{iter: d.geom, count: d.geom.Count(protoscan.WireTypeVarint)}

	if gd.count < 2 {
		return nil, fmt.Errorf("geom is not long enough: %v", gd.count)
	}

	switch geomType {
	case vectortile.Tile_POINT:
		return gd.decodePoint()
	case vectortile.Tile_LINESTRING:
		return gd.decodeLineString()
	case vectortile.Tile_POLYGON:
		return gd.decodePolygon()
	}

	return nil, fmt.Errorf("unknown geometry type: %v", geomType)
}

// A geomDecoder holds state for geometry decoding.
type geomDecoder struct {
	iter  *protoscan.Iterator
	count int
	used  int

	prev orb.Point
}

func (gd *geomDecoder) decodePoint() (orb.Geometry, error) {
	_, count, err := gd.cmdAndCount()
	if err != nil {
		return nil, err
	}

	if count == 1 {
		return gd.NextPoint()
	}

	mp := make(orb.MultiPoint, 0, count)
	for i := uint32(0); i < count; i++ {
		p, err := gd.NextPoint()
		if err != nil {
			return nil, err
		}
		mp = append(mp, p)
	}

	return mp, nil
}

func (gd *geomDecoder) decodeLine() (orb.LineString, error) {
	cmd, count, err := gd.cmdAndCount()
	if err != nil {
		return nil, err
	}

	if cmd != moveTo || count != 1 {
		return nil, errors.New("first command not one moveTo")
	}

	first, err := gd.NextPoint()
	if err != nil {
		return nil, err
	}
	cmd, count, err = gd.cmdAndCount()
	if err != nil {
		return nil, err
	}

	if cmd != lineTo {
		return nil, errors.New("second command not a lineTo")
	}

	ls := make(orb.LineString, 0, count+1)
	ls = append(ls, first)

	for i := uint32(0); i < count; i++ {
		p, err := gd.NextPoint()
		if err != nil {
			return nil, err
		}
		ls = append(ls, p)
	}

	return ls, nil
}

func (gd *geomDecoder) decodeLineString() (orb.Geometry, error) {
	var mls orb.MultiLineString
	for !gd.done() {
		ls, err := gd.decodeLine()
		if err != nil {
			return nil, err
		}

		if gd.done() && len(mls) == 0 {
			return ls, nil
		}

		mls = append(mls, ls)
	}

	return mls, nil
}

func (gd *geomDecoder) decodePolygon() (orb.Geometry, error) {
	var mp orb.MultiPolygon
	var p orb.Polygon
	for !gd.done() {
		ls, err := gd.decodeLine()
		if err != nil {
			return nil, err
		}

		r := orb.Ring(ls)

		cmd, _, err := gd.cmdAndCount()
		if err != nil {
			return nil, err
		}

		if cmd == closePath && !r.Closed() {
			r = append(r, r[0])
		}

		// figure out if new polygon
		if len(mp) == 0 && len(p) == 0 {
			p = append(p, r)
		} else {
			if r.Orientation() == orb.CCW {
				mp = append(mp, p)
				p = orb.Polygon{r}
			} else {
				p = append(p, r)
			}
		}
	}

	if len(mp) == 0 {
		return p, nil
	}

	return append(mp, p), nil
}

func (gd *geomDecoder) cmdAndCount() (uint32, uint32, error) {
	if gd.done() {
		return 0, 0, errors.New("no more data")
	}

	v, err := gd.iter.Uint32()
	if err != nil {
		return 0, 0, err
	}
	gd.used++

	cmd := v & 0x07
	count := v >> 3

	if cmd != closePath {
		if v := gd.used + int(2*count); gd.count < v {
			return 0, 0, fmt.Errorf("data cut short: needed %d, have %d", v, gd.count)
		}
	}

	return cmd, count, nil
}

func (gd *geomDecoder) NextPoint() (orb.Point, error) {
	gd.used += 2

	v, err := gd.iter.Uint32()
	if err != nil {
		return orb.Point{}, err
	}
	gd.prev[0] += unzigzag(v)

	v, err = gd.iter.Uint32()
	if err != nil {
		return orb.Point{}, err
	}
	gd.prev[1] += unzigzag(v)

	return gd.prev, nil
}

func (gd *geomDecoder) done() bool {
	return !gd.iter.HasNext()
}

func decodeValueMsg(msg *protoscan.Message) (interface{}, error) {
	for msg.Next() {
		switch msg.FieldNumber() {
		case 1:
			return msg.String()
		case 2:
			v, err := msg.Float()
			return float64(v), err
		case 3:
			return msg.Double()
		case 4:
			v, err := msg.Int64()
			return float64(v), err
		case 5:
			v, err := msg.Uint64()
			return float64(v), err
		case 6:
			v, err := msg.Sint64()
			return float64(v), err
		case 7:
			return msg.Bool()
		default:
			msg.Skip()
		}
	}

	return nil, msg.Err()
}

// Check if data is GZipped by reading the "magic bytes"
// Rarely this method can result in false positives
func dataIsGZipped(data []byte) bool {
	return (data[0] == 0x1F && data[1] == 0x8B)
}
