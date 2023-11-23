package geojson

import "fmt"

// Properties defines the feature properties with some helper methods.
type Properties map[string]interface{}

// MustBool guarantees the return of a `bool` (with optional default).
// This function useful when you explicitly want a `bool` in a single
// value return context, for example:
//     myFunc(f.Properties.MustBool("param1"), f.Properties.MustBool("optional_param", true))
// This function will panic if the value is present but not a bool.
func (p Properties) MustBool(key string, def ...bool) bool {
	v := p[key]
	if b, ok := v.(bool); ok {
		return b
	}

	if v != nil {
		panic(fmt.Sprintf("not a bool, but a %T: %v", v, v))
	}

	if len(def) > 0 {
		return def[0]
	}

	panic("property not found")
}

// MustInt guarantees the return of an `int` (with optional default).
// This function useful when you explicitly want a `int` in a single
// value return context, for example:
//     myFunc(f.Properties.MustInt("param1"), f.Properties.MustInt("optional_param", 123))
// This function will panic if the value is present but not a number.
func (p Properties) MustInt(key string, def ...int) int {
	v := p[key]
	if i, ok := v.(int); ok {
		return i
	}

	if f, ok := v.(float64); ok {
		return int(f)
	}

	if v != nil {
		panic(fmt.Sprintf("not a number, but a %T: %v", v, v))
	}

	if len(def) > 0 {
		return def[0]
	}

	panic("property not found")
}

// MustFloat64 guarantees the return of a `float64` (with optional default)
// This function useful when you explicitly want a `float64` in a single
// value return context, for example:
//     myFunc(f.Properties.MustFloat64("param1"), f.Properties.MustFloat64("optional_param", 10.1))
// This function will panic if the value is present but not a number.
func (p Properties) MustFloat64(key string, def ...float64) float64 {
	v := p[key]
	if f, ok := v.(float64); ok {
		return f
	}

	if i, ok := v.(int); ok {
		return float64(i)
	}

	if v != nil {
		panic(fmt.Sprintf("not a number, but a %T: %v", v, v))
	}

	if len(def) > 0 {
		return def[0]
	}

	panic("property not found")
}

// MustString guarantees the return of a `string` (with optional default)
// This function useful when you explicitly want a `string` in a single
// value return context, for example:
//     myFunc(f.Properties.MustString("param1"), f.Properties.MustString("optional_param", "default"))
// This function will panic if the value is present but not a string.
func (p Properties) MustString(key string, def ...string) string {
	v := p[key]
	if s, ok := v.(string); ok {
		return s
	}

	if v != nil {
		panic(fmt.Sprintf("not a string, but a %T: %v", v, v))
	}

	if len(def) > 0 {
		return def[0]
	}

	panic("property not found")
}

// Clone returns a shallow copy of the properties.
func (p Properties) Clone() Properties {
	n := make(Properties, len(p)+3)
	for k, v := range p {
		n[k] = v
	}

	return n
}
