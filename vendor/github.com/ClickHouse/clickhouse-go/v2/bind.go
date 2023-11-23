// Licensed to ClickHouse, Inc. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. ClickHouse, Inc. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package clickhouse

import (
	std_driver "database/sql/driver"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func Named(name string, value interface{}) driver.NamedValue {
	return driver.NamedValue{
		Name:  name,
		Value: value,
	}
}

type TimeUnit uint8

const (
	Seconds TimeUnit = iota
	MilliSeconds
	MicroSeconds
	NanoSeconds
)

type GroupSet struct {
	Value []interface{}
}

type ArraySet []interface{}

func DateNamed(name string, value time.Time, scale TimeUnit) driver.NamedDateValue {
	return driver.NamedDateValue{
		Name:  name,
		Value: value,
		Scale: uint8(scale),
	}
}

var bindNumericRe = regexp.MustCompile(`\$[0-9]+`)
var bindPositionalRe = regexp.MustCompile(`[^\\][?]`)

func bind(tz *time.Location, query string, args ...interface{}) (string, error) {
	if len(args) == 0 {
		return query, nil
	}
	var (
		haveNamed      bool
		haveNumeric    bool
		havePositional bool
	)
	haveNumeric = bindNumericRe.MatchString(query)
	havePositional = bindPositionalRe.MatchString(query)
	if haveNumeric && havePositional {
		return "", ErrBindMixedParamsFormats
	}
	for _, v := range args {
		switch v.(type) {
		case driver.NamedValue, driver.NamedDateValue:
			haveNamed = true
		default:
		}
		if haveNamed && (haveNumeric || havePositional) {
			return "", ErrBindMixedParamsFormats
		}
	}
	if haveNamed {
		return bindNamed(tz, query, args...)
	}
	if haveNumeric {
		return bindNumeric(tz, query, args...)
	}
	return bindPositional(tz, query, args...)
}

var bindPositionCharRe = regexp.MustCompile(`[?]`)

func bindPositional(tz *time.Location, query string, args ...interface{}) (_ string, err error) {
	var (
		unbind = make(map[int]struct{})
		params = make([]string, len(args))
	)
	for i, v := range args {
		if fn, ok := v.(std_driver.Valuer); ok {
			if v, err = fn.Value(); err != nil {
				return "", nil
			}
		}
		params[i], err = format(tz, Seconds, v)
		if err != nil {
			return "", err
		}
	}
	i := 0
	query = bindPositionalRe.ReplaceAllStringFunc(query, func(n string) string {
		if i >= len(params) {
			unbind[i] = struct{}{}
			return ""
		}
		val := params[i]
		i++
		return bindPositionCharRe.ReplaceAllStringFunc(n, func(m string) string {
			return val
		})
	})
	for param := range unbind {
		return "", fmt.Errorf("have no arg for param ? at position %d", param)
	}
	// replace \? escape sequence
	return strings.ReplaceAll(query, "\\?", "?"), nil
}

func bindNumeric(tz *time.Location, query string, args ...interface{}) (_ string, err error) {
	var (
		unbind = make(map[string]struct{})
		params = make(map[string]string)
	)
	for i, v := range args {
		if fn, ok := v.(std_driver.Valuer); ok {
			if v, err = fn.Value(); err != nil {
				return "", nil
			}
		}
		val, err := format(tz, Seconds, v)
		if err != nil {
			return "", err
		}
		params[fmt.Sprintf("$%d", i+1)] = val
	}
	query = bindNumericRe.ReplaceAllStringFunc(query, func(n string) string {
		if _, found := params[n]; !found {
			unbind[n] = struct{}{}
			return ""
		}
		return params[n]
	})
	for param := range unbind {
		return "", fmt.Errorf("have no arg for %s param", param)
	}
	return query, nil
}

var bindNamedRe = regexp.MustCompile(`@[a-zA-Z0-9\_]+`)

func bindNamed(tz *time.Location, query string, args ...interface{}) (_ string, err error) {
	var (
		unbind = make(map[string]struct{})
		params = make(map[string]string)
	)
	for _, v := range args {
		switch v := v.(type) {
		case driver.NamedValue:
			value := v.Value
			if fn, ok := v.Value.(std_driver.Valuer); ok {
				if value, err = fn.Value(); err != nil {
					return "", err
				}
			}
			val, err := format(tz, Seconds, value)
			if err != nil {
				return "", err
			}
			params["@"+v.Name] = val
		case driver.NamedDateValue:
			val, err := format(tz, TimeUnit(v.Scale), v.Value)
			if err != nil {
				return "", err
			}
			params["@"+v.Name] = val
		}
	}
	query = bindNamedRe.ReplaceAllStringFunc(query, func(n string) string {
		if _, found := params[n]; !found {
			unbind[n] = struct{}{}
			return ""
		}
		return params[n]
	})
	for param := range unbind {
		return "", fmt.Errorf("have no arg for %q param", param)
	}
	return query, nil
}

func formatTime(tz *time.Location, scale TimeUnit, value time.Time) (string, error) {
	switch value.Location().String() {
	case "Local", "":
		switch scale {
		case Seconds:
			return fmt.Sprintf("toDateTime('%d')", value.Unix()), nil
		case MilliSeconds:
			return fmt.Sprintf("toDateTime64('%d', 3)", value.UnixMilli()), nil
		case MicroSeconds:
			return fmt.Sprintf("toDateTime64('%d', 6)", value.UnixMicro()), nil
		case NanoSeconds:
			return fmt.Sprintf("toDateTime64('%d', 9)", value.UnixNano()), nil
		}
	case tz.String():
		if scale == Seconds {
			return value.Format("toDateTime('2006-01-02 15:04:05')"), nil
		}
		return fmt.Sprintf("toDateTime64('%s', %d)", value.Format(fmt.Sprintf("2006-01-02 15:04:05.%0*d", int(scale*3), 0)), int(scale*3)), nil
	}
	if scale == Seconds {
		return value.Format(fmt.Sprintf("toDateTime('2006-01-02 15:04:05', '%s')", value.Location().String())), nil
	}
	return fmt.Sprintf("toDateTime64('%s', %d, '%s')", value.Format(fmt.Sprintf("2006-01-02 15:04:05.%0*d", int(scale*3), 0)), int(scale*3), value.Location().String()), nil
}

func format(tz *time.Location, scale TimeUnit, v interface{}) (string, error) {
	quote := func(v string) string {
		return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(v) + "'"
	}
	switch v := v.(type) {
	case nil:
		return "NULL", nil
	case string:
		return quote(v), nil
	case time.Time:
		return formatTime(tz, scale, v)
	case GroupSet:
		val, err := join(tz, scale, v.Value)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("(%s)", val), nil
	case []GroupSet:
		val, err := join(tz, scale, v)
		if err != nil {
			return "", err
		}
		return val, err
	case ArraySet:
		val, err := join(tz, scale, v)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("[%s]", val), nil
	case fmt.Stringer:
		return quote(v.String()), nil
	}
	switch v := reflect.ValueOf(v); v.Kind() {
	case reflect.String:
		return quote(v.String()), nil
	case reflect.Slice, reflect.Array:
		values := make([]string, 0, v.Len())
		for i := 0; i < v.Len(); i++ {
			val, err := format(tz, scale, v.Index(i).Interface())
			if err != nil {
				return "", err
			}
			values = append(values, val)
		}
		return strings.Join(values, ", "), nil
	case reflect.Map: // map
		values := make([]string, 0, len(v.MapKeys()))
		for _, key := range v.MapKeys() {
			name := fmt.Sprint(key.Interface())
			if key.Kind() == reflect.String {
				name = fmt.Sprintf("'%s'", name)
			}
			val, err := format(tz, scale, v.MapIndex(key).Interface())
			if err != nil {
				return "", err
			}
			if v.MapIndex(key).Kind() == reflect.Slice || v.MapIndex(key).Kind() == reflect.Array {
				// assume slices in maps are arrays
				val = fmt.Sprintf("[%s]", val)
			}
			values = append(values, fmt.Sprintf("%s, %s", name, val))
		}
		return "map(" + strings.Join(values, ", ") + ")", nil

	}
	return fmt.Sprint(v), nil
}

func join[E any](tz *time.Location, scale TimeUnit, values []E) (string, error) {
	items := make([]string, len(values), len(values))
	for i := range values {
		val, err := format(tz, scale, values[i])
		if err != nil {
			return "", err
		}
		items[i] = val
	}
	return strings.Join(items, ", "), nil
}

func rebind(in []std_driver.NamedValue) []interface{} {
	args := make([]interface{}, 0, len(in))
	for _, v := range in {
		switch {
		case len(v.Name) != 0:
			args = append(args, driver.NamedValue{
				Name:  v.Name,
				Value: v.Value,
			})

		default:
			args = append(args, v.Value)
		}
	}
	return args
}
