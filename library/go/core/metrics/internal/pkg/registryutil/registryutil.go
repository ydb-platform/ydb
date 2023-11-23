package registryutil

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/OneOfOne/xxhash"
)

// BuildRegistryKey creates registry name based on given prefix and tags
func BuildRegistryKey(prefix string, tags map[string]string) string {
	var builder strings.Builder

	builder.WriteString(strconv.Quote(prefix))
	builder.WriteRune('{')
	builder.WriteString(StringifyTags(tags))
	builder.WriteByte('}')

	return builder.String()
}

// BuildFQName returns name parts joined by given separator.
// Mainly used to append prefix to registry
func BuildFQName(separator string, parts ...string) (name string) {
	var b strings.Builder
	for _, p := range parts {
		if p == "" {
			continue
		}
		if b.Len() > 0 {
			b.WriteString(separator)
		}
		b.WriteString(strings.Trim(p, separator))
	}
	return b.String()
}

// MergeTags merges 2 sets of tags with the tags from tagsRight overriding values from tagsLeft
func MergeTags(leftTags map[string]string, rightTags map[string]string) map[string]string {
	if leftTags == nil && rightTags == nil {
		return nil
	}

	if len(leftTags) == 0 {
		return rightTags
	}

	if len(rightTags) == 0 {
		return leftTags
	}

	newTags := make(map[string]string)
	for key, value := range leftTags {
		newTags[key] = value
	}
	for key, value := range rightTags {
		newTags[key] = value
	}
	return newTags
}

// StringifyTags returns string representation of given tags map.
// It is guaranteed that equal sets of tags will produce equal strings.
func StringifyTags(tags map[string]string) string {
	keys := make([]string, 0, len(tags))
	for key := range tags {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var builder strings.Builder
	for i, key := range keys {
		if i > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString(key + "=" + tags[key])
	}

	return builder.String()
}

// VectorHash computes hash of metrics vector element
func VectorHash(tags map[string]string, labels []string) (uint64, error) {
	if len(tags) != len(labels) {
		return 0, errors.New("inconsistent tags and labels sets")
	}

	h := xxhash.New64()

	for _, label := range labels {
		v, ok := tags[label]
		if !ok {
			return 0, fmt.Errorf("label '%s' not found in tags", label)
		}
		_, _ = h.WriteString(label)
		_, _ = h.WriteString(v)
		_, _ = h.WriteString(",")
	}

	return h.Sum64(), nil
}
