package xreflect

import "reflect"

// Assign source's value to target's value it points to. Source must be value, target must be pointer to existing value.
// Source must be assignable to target's value it points to.
func Assign(source interface{}, target interface{}) bool {
	val := reflect.ValueOf(target)
	typ := val.Type()
	targetType := typ.Elem()
	if reflect.TypeOf(source).AssignableTo(targetType) {
		val.Elem().Set(reflect.ValueOf(source))
		return true
	}

	return false
}
