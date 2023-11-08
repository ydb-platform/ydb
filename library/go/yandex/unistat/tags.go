package unistat

import (
	"sort"
	"strings"
)

type Tag struct {
	Name  string
	Value string
}

func formatTags(tags []Tag) string {
	if len(tags) == 0 {
		return ""
	}

	sort.Slice(tags, func(i, j int) bool {
		return tags[i].Name < tags[j].Name
	})

	var result strings.Builder
	for i := range tags {
		value := tags[i].Name + "=" + tags[i].Value + ";"

		result.WriteString(value)
	}

	return result.String()
}
