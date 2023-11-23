/*
Package geojson is a library for encoding and decoding GeoJSON into Go structs
using the geometries in the orb package. Supports both the json.Marshaler and
json.Unmarshaler interfaces as well as helper functions such as
`UnmarshalFeatureCollection` and `UnmarshalFeature`.
*/
package geojson

import (
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
)

const featureCollection = "FeatureCollection"

// A FeatureCollection correlates to a GeoJSON feature collection.
type FeatureCollection struct {
	Type     string     `json:"type"`
	BBox     BBox       `json:"bbox,omitempty"`
	Features []*Feature `json:"features"`

	// ExtraMembers can be used to encoded/decode extra key/members in
	// the base of the feature collection. Note that keys of "type", "bbox"
	// and "features" will not work as those are reserved by the GeoJSON spec.
	ExtraMembers Properties `json:"-"`
}

// NewFeatureCollection creates and initializes a new feature collection.
func NewFeatureCollection() *FeatureCollection {
	return &FeatureCollection{
		Type:     featureCollection,
		Features: []*Feature{},
	}
}

// Append appends a feature to the collection.
func (fc *FeatureCollection) Append(feature *Feature) *FeatureCollection {
	fc.Features = append(fc.Features, feature)
	return fc
}

// MarshalJSON converts the feature collection object into the proper JSON.
// It will handle the encoding of all the child features and geometries.
// Alternately one can call json.Marshal(fc) directly for the same result.
// Items in the ExtraMembers map will be included in the base of the
// feature collection object.
func (fc FeatureCollection) MarshalJSON() ([]byte, error) {
	m := newFeatureCollectionDoc(fc)
	return marshalJSON(m)
}

// MarshalBSON converts the feature collection object into a BSON document
// represented by bytes. It will handle the encoding of all the child features
// and geometries.
// Items in the ExtraMembers map will be included in the base of the
// feature collection object.
func (fc FeatureCollection) MarshalBSON() ([]byte, error) {
	m := newFeatureCollectionDoc(fc)
	return bson.Marshal(m)
}

func newFeatureCollectionDoc(fc FeatureCollection) map[string]interface{} {
	var tmp map[string]interface{}
	if fc.ExtraMembers != nil {
		tmp = fc.ExtraMembers.Clone()
	} else {
		tmp = make(map[string]interface{}, 3)
	}

	tmp["type"] = featureCollection
	delete(tmp, "bbox")
	if fc.BBox != nil {
		tmp["bbox"] = fc.BBox
	}
	if fc.Features == nil {
		tmp["features"] = []*Feature{}
	} else {
		tmp["features"] = fc.Features
	}

	return tmp
}

// UnmarshalJSON decodes the data into a GeoJSON feature collection.
// Extra/foreign members will be put into the `ExtraMembers` attribute.
func (fc *FeatureCollection) UnmarshalJSON(data []byte) error {
	tmp := make(map[string]nocopyRawMessage, 4)

	err := unmarshalJSON(data, &tmp)
	if err != nil {
		return err
	}

	*fc = FeatureCollection{}
	for key, value := range tmp {
		switch key {
		case "type":
			err := unmarshalJSON(value, &fc.Type)
			if err != nil {
				return err
			}
		case "bbox":
			err := unmarshalJSON(value, &fc.BBox)
			if err != nil {
				return err
			}
		case "features":
			err := unmarshalJSON(value, &fc.Features)
			if err != nil {
				return err
			}
		default:
			if fc.ExtraMembers == nil {
				fc.ExtraMembers = Properties{}
			}

			var val interface{}
			err := unmarshalJSON(value, &val)
			if err != nil {
				return err
			}
			fc.ExtraMembers[key] = val
		}
	}

	if fc.Type != featureCollection {
		return fmt.Errorf("geojson: not a feature collection: type=%s", fc.Type)
	}

	return nil
}

// UnmarshalBSON will unmarshal a BSON document created with bson.Marshal.
// Extra/foreign members will be put into the `ExtraMembers` attribute.
func (fc *FeatureCollection) UnmarshalBSON(data []byte) error {
	tmp := make(map[string]bson.RawValue, 4)

	err := bson.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}

	*fc = FeatureCollection{}
	for key, value := range tmp {
		switch key {
		case "type":
			fc.Type, _ = bson.RawValue(value).StringValueOK()
		case "bbox":
			err := value.Unmarshal(&fc.BBox)
			if err != nil {
				return err
			}
		case "features":
			err := value.Unmarshal(&fc.Features)
			if err != nil {
				return err
			}
		default:
			if fc.ExtraMembers == nil {
				fc.ExtraMembers = Properties{}
			}

			var val interface{}
			err := value.Unmarshal(&val)
			if err != nil {
				return err
			}
			fc.ExtraMembers[key] = val
		}
	}

	if fc.Type != featureCollection {
		return fmt.Errorf("geojson: not a feature collection: type=%s", fc.Type)
	}

	return nil
}

// UnmarshalFeatureCollection decodes the data into a GeoJSON feature collection.
// Alternately one can call json.Unmarshal(fc) directly for the same result.
func UnmarshalFeatureCollection(data []byte) (*FeatureCollection, error) {
	fc := &FeatureCollection{}

	err := fc.UnmarshalJSON(data)
	if err != nil {
		return nil, err
	}

	return fc, nil
}
