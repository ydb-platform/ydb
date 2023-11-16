package types

import (
	"time"

	"google.golang.org/protobuf/proto"
)

// Resource is the base interface for the xDS payload.
type Resource interface {
	proto.Message
}

// ResourceWithTTL is a Resource with an optional TTL.
type ResourceWithTTL struct {
	Resource Resource
	TTL      *time.Duration
}

// ResourceWithName provides a name for out-of-tree resources.
type ResourceWithName interface {
	proto.Message
	GetName() string
}

// MarshaledResource is an alias for the serialized binary array.
type MarshaledResource = []byte

// SkipFetchError is the error returned when the cache fetch is short
// circuited due to the client's version already being up-to-date.
type SkipFetchError struct{}

// Error satisfies the error interface
func (e SkipFetchError) Error() string {
	return "skip fetch: version up to date"
}

// ResponseType enumeration of supported response types
type ResponseType int

const (
	Endpoint ResponseType = iota
	Cluster
	Route
	ScopedRoute
	VirtualHost
	Listener
	Secret
	Runtime
	ExtensionConfig
	RateLimitConfig
	UnknownType // token to count the total number of supported types
)
