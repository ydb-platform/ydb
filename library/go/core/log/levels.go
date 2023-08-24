package log

import (
	"fmt"
	"strings"
)

// Level of logging
type Level int

// MarshalText marshals level to text
func (l Level) MarshalText() ([]byte, error) {
	if l >= maxLevel || l < 0 {
		return nil, fmt.Errorf("failed to marshal log level: level value (%d) is not in the allowed range (0-%d)", l, maxLevel-1)
	}
	return []byte(l.String()), nil
}

// UnmarshalText unmarshals level from text
func (l *Level) UnmarshalText(text []byte) error {
	level, err := ParseLevel(string(text))
	if err != nil {
		return err
	}

	*l = level
	return nil
}

// Standard log levels
const (
	TraceLevel Level = iota
	DebugLevel
	InfoLevel
	WarnLevel
	ErrorLevel
	FatalLevel
	maxLevel
)

func Levels() (l []Level) {
	for i := 0; i < int(maxLevel); i++ {
		l = append(l, Level(i))
	}
	return
}

// String values for standard log levels
const (
	TraceString = "trace"
	DebugString = "debug"
	InfoString  = "info"
	WarnString  = "warn"
	ErrorString = "error"
	FatalString = "fatal"
)

// String implements Stringer interface for Level
func (l Level) String() string {
	switch l {
	case TraceLevel:
		return TraceString
	case DebugLevel:
		return DebugString
	case InfoLevel:
		return InfoString
	case WarnLevel:
		return WarnString
	case ErrorLevel:
		return ErrorString
	case FatalLevel:
		return FatalString
	default:
		// For when new log level is not added to this func (most likely never).
		panic(fmt.Sprintf("unknown log level: %d", l))
	}
}

// Set implements flag.Value interface
func (l *Level) Set(v string) error {
	lvl, err := ParseLevel(v)
	if err != nil {
		return err
	}

	*l = lvl
	return nil
}

// ParseLevel parses log level from string. Returns ErrUnknownLevel for unknown log level.
func ParseLevel(l string) (Level, error) {
	switch strings.ToLower(l) {
	case TraceString:
		return TraceLevel, nil
	case DebugString:
		return DebugLevel, nil
	case InfoString:
		return InfoLevel, nil
	case WarnString:
		return WarnLevel, nil
	case ErrorString:
		return ErrorLevel, nil
	case FatalString:
		return FatalLevel, nil
	default:
		return FatalLevel, fmt.Errorf("unknown log level: %s", l)
	}
}
