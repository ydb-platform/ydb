package wkt

import (
	"errors"
	"regexp"
	"strconv"
	"strings"

	"github.com/paulmach/orb"
)

var (
	// ErrNotWKT is returned when unmarshalling WKT and the data is not valid.
	ErrNotWKT = errors.New("wkt: invalid data")

	// ErrIncorrectGeometry is returned when unmarshalling WKT data into the wrong type.
	// For example, unmarshaling linestring data into a point.
	ErrIncorrectGeometry = errors.New("wkt: incorrect geometry")

	// ErrUnsupportedGeometry is returned when geometry type is not supported by this lib.
	ErrUnsupportedGeometry = errors.New("wkt: unsupported geometry")

	doubleParen = regexp.MustCompile(`\)[\s|\t]*\)([\s|\t]*,[\s|\t]*)\([\s|\t]*\(`)
	singleParen = regexp.MustCompile(`\)([\s|\t]*,[\s|\t]*)\(`)
	noParen     = regexp.MustCompile(`([\s|\t]*,[\s|\t]*)`)
)

// UnmarshalPoint returns the point represented by the wkt string.
// Will return ErrIncorrectGeometry if the wkt is not a point.
func UnmarshalPoint(s string) (p orb.Point, err error) {
	geom, err := Unmarshal(s)
	if err != nil {
		return orb.Point{}, err
	}
	g, ok := geom.(orb.Point)
	if !ok {
		return orb.Point{}, ErrIncorrectGeometry
	}
	return g, nil
}

// UnmarshalMultiPoint returns the multi-point represented by the wkt string.
// Will return ErrIncorrectGeometry if the wkt is not a multi-point.
func UnmarshalMultiPoint(s string) (p orb.MultiPoint, err error) {
	geom, err := Unmarshal(s)
	if err != nil {
		return nil, err
	}

	g, ok := geom.(orb.MultiPoint)
	if !ok {
		return nil, ErrIncorrectGeometry
	}
	return g, nil
}

// UnmarshalLineString returns the linestring represented by the wkt string.
// Will return ErrIncorrectGeometry if the wkt is not a linestring.
func UnmarshalLineString(s string) (p orb.LineString, err error) {
	geom, err := Unmarshal(s)
	if err != nil {
		return nil, err
	}
	g, ok := geom.(orb.LineString)
	if !ok {
		return nil, ErrIncorrectGeometry
	}
	return g, nil
}

// UnmarshalMultiLineString returns the multi-linestring represented by the wkt string.
// Will return ErrIncorrectGeometry if the wkt is not a multi-linestring.
func UnmarshalMultiLineString(s string) (p orb.MultiLineString, err error) {
	geom, err := Unmarshal(s)
	if err != nil {
		return nil, err
	}
	g, ok := geom.(orb.MultiLineString)
	if !ok {
		return nil, ErrIncorrectGeometry
	}
	return g, nil
}

// UnmarshalPolygon returns the polygon represented by the wkt string.
// Will return ErrIncorrectGeometry if the wkt is not a polygon.
func UnmarshalPolygon(s string) (p orb.Polygon, err error) {
	geom, err := Unmarshal(s)
	if err != nil {
		return nil, err
	}
	g, ok := geom.(orb.Polygon)
	if !ok {
		return nil, ErrIncorrectGeometry
	}
	return g, nil
}

// UnmarshalMultiPolygon returns the multi-polygon represented by the wkt string.
// Will return ErrIncorrectGeometry if the wkt is not a multi-polygon.
func UnmarshalMultiPolygon(s string) (p orb.MultiPolygon, err error) {
	geom, err := Unmarshal(s)
	if err != nil {
		return nil, err
	}
	g, ok := geom.(orb.MultiPolygon)
	if !ok {
		return nil, ErrIncorrectGeometry
	}
	return g, nil
}

// UnmarshalCollection returns the geometry collection represented by the wkt string.
// Will return ErrIncorrectGeometry if the wkt is not a geometry collection.
func UnmarshalCollection(s string) (p orb.Collection, err error) {
	geom, err := Unmarshal(s)
	if err != nil {
		return orb.Collection{}, err
	}
	g, ok := geom.(orb.Collection)
	if !ok {
		return nil, ErrIncorrectGeometry
	}
	return g, nil
}

// trimSpaceBrackets trim space and brackets
func trimSpaceBrackets(s string) (string, error) {
	s = strings.Trim(s, " ")
	if len(s) == 0 {
		return "", nil
	}

	if s[0] == '(' {
		s = s[1:]
	} else {
		return "", ErrNotWKT
	}

	if s[len(s)-1] == ')' {
		s = s[:len(s)-1]
	} else {
		return "", ErrNotWKT
	}
	return strings.Trim(s, " "), nil
}

// parsePoint pase point by (x y)
func parsePoint(s string) (p orb.Point, err error) {
	ps := strings.Split(s, " ")
	if len(ps) != 2 {
		return orb.Point{}, ErrNotWKT
	}

	x, err := strconv.ParseFloat(ps[0], 64)
	if err != nil {
		return orb.Point{}, err
	}

	y, err := strconv.ParseFloat(ps[1], 64)
	if err != nil {
		return orb.Point{}, err
	}

	return orb.Point{x, y}, nil
}

// splitGeometryCollection split GEOMETRYCOLLECTION to more geometry
func splitGeometryCollection(s string) (r []string) {
	r = make([]string, 0)
	stack := make([]rune, 0)
	l := len(s)
	for i, v := range s {
		if !strings.Contains(string(stack), "(") {
			stack = append(stack, v)
			continue
		}
		if v >= 'A' && v < 'Z' {
			t := string(stack)
			r = append(r, t[:len(t)-1])
			stack = make([]rune, 0)
			stack = append(stack, v)
			continue
		}
		if i == l-1 {
			r = append(r, string(stack))
			continue
		}
		stack = append(stack, v)
	}
	return
}

// Unmarshal return a geometry by parsing the WKT string.
func Unmarshal(s string) (geom orb.Geometry, err error) {
	s = strings.ToUpper(strings.Trim(s, " "))
	switch {
	case strings.Contains(s, "GEOMETRYCOLLECTION"):
		if s == "GEOMETRYCOLLECTION EMPTY" {
			return orb.Collection{}, nil
		}

		s = strings.ReplaceAll(s, "GEOMETRYCOLLECTION", "")
		if len(s) == 0 {
			return nil, ErrNotWKT
		}

		tc := orb.Collection{}
		geometries := splitGeometryCollection(s)
		if len(geometries) == 0 {
			return nil, err
		}

		for _, g := range geometries {
			if len(g) == 0 {
				continue
			}

			tg, err := Unmarshal(g)
			if err != nil {
				return nil, err
			}

			tc = append(tc, tg)
		}

		geom = tc

	case strings.Contains(s, "MULTIPOINT"):
		if s == "MULTIPOINT EMPTY" {
			return orb.MultiPoint{}, nil
		}

		s, err := trimSpaceBrackets(strings.ReplaceAll(s, "MULTIPOINT", ""))
		if err != nil {
			return nil, err
		}

		ps := splitByRegexp(s, noParen)
		tmp := orb.MultiPoint{}
		for _, p := range ps {
			p, err := trimSpaceBrackets(p)
			if err != nil {
				return nil, err
			}

			tp, err := parsePoint(p)
			if err != nil {
				return nil, err
			}

			tmp = append(tmp, tp)
		}

		geom = tmp

	case strings.Contains(s, "POINT"):
		s, err := trimSpaceBrackets(strings.ReplaceAll(s, "POINT", ""))
		if err != nil {
			return nil, err
		}

		tp, err := parsePoint(s)
		if err != nil {
			return nil, err
		}

		geom = tp

	case strings.Contains(s, "MULTILINESTRING"):
		if s == "MULTILINESTRING EMPTY" {
			return orb.MultiLineString{}, nil
		}

		s, err := trimSpaceBrackets(strings.ReplaceAll(s, "MULTILINESTRING", ""))
		if err != nil {
			return nil, err
		}

		tmls := orb.MultiLineString{}
		for _, ls := range splitByRegexp(s, singleParen) {
			ls, err := trimSpaceBrackets(ls)
			if err != nil {
				return nil, err
			}

			tls := orb.LineString{}
			for _, p := range splitByRegexp(ls, noParen) {
				tp, err := parsePoint(p)
				if err != nil {
					return nil, err
				}
				tls = append(tls, tp)
			}
			tmls = append(tmls, tls)
		}

		geom = tmls

	case strings.Contains(s, "LINESTRING"):
		if s == "LINESTRING EMPTY" {
			return orb.LineString{}, nil
		}

		s, err := trimSpaceBrackets(strings.ReplaceAll(s, "LINESTRING", ""))
		if err != nil {
			return nil, err
		}

		ls := splitByRegexp(s, noParen)
		tls := orb.LineString{}
		for _, p := range ls {
			tp, err := parsePoint(p)
			if err != nil {
				return nil, err
			}

			tls = append(tls, tp)
		}

		geom = tls

	case strings.Contains(s, "MULTIPOLYGON"):
		if s == "MULTIPOLYGON EMPTY" {
			return orb.MultiPolygon{}, nil
		}

		s, err := trimSpaceBrackets(strings.ReplaceAll(s, "MULTIPOLYGON", ""))
		if err != nil {
			return nil, err
		}

		tmpoly := orb.MultiPolygon{}
		for _, poly := range splitByRegexp(s, doubleParen) {
			poly, err := trimSpaceBrackets(poly)
			if err != nil {
				return nil, err
			}

			tpoly := orb.Polygon{}
			for _, r := range splitByRegexp(poly, singleParen) {
				r, err := trimSpaceBrackets(r)
				if err != nil {
					return nil, err
				}

				tr := orb.Ring{}
				for _, p := range splitByRegexp(r, noParen) {
					tp, err := parsePoint(p)
					if err != nil {
						return nil, err
					}

					tr = append(tr, tp)
				}

				tpoly = append(tpoly, tr)
			}

			tmpoly = append(tmpoly, tpoly)
		}

		geom = tmpoly

	case strings.Contains(s, "POLYGON"):
		if s == "POLYGON EMPTY" {
			return orb.Polygon{}, nil
		}

		s, err := trimSpaceBrackets(strings.ReplaceAll(s, "POLYGON", ""))
		if err != nil {
			return nil, err
		}

		rings := splitByRegexp(s, singleParen)
		tpoly := make(orb.Polygon, 0, len(rings))
		for _, r := range rings {
			r, err := trimSpaceBrackets(r)
			if err != nil {
				return nil, err
			}

			ps := splitByRegexp(r, noParen)
			tring := orb.Ring{}
			for _, p := range ps {
				tp, err := parsePoint(p)
				if err != nil {
					return nil, err
				}
				tring = append(tring, tp)
			}
			tpoly = append(tpoly, tring)
		}
		geom = tpoly
	default:
		return nil, ErrUnsupportedGeometry
	}

	return
}

func splitByRegexp(s string, re *regexp.Regexp) []string {
	indexes := re.FindAllStringSubmatchIndex(s, -1)
	start := 0
	result := make([]string, len(indexes)+1)
	for i, element := range indexes {
		result[i] = s[start:element[2]]
		start = element[3]
	}
	result[len(indexes)] = s[start:]
	return result
}
