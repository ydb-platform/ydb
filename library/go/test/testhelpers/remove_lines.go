package testhelpers

import (
	"fmt"
	"sort"
	"strings"
)

func RemoveLines(str string, lines ...int) (string, error) {
	if len(lines) == 0 {
		return str, nil
	}

	sort.Ints(lines)

	var b strings.Builder
	b.Grow(len(str))

	var count int
	var start int
	var lineID int
	for i, s := range str {
		if s != '\n' {
			continue
		}

		if lines[lineID] != count {
			b.WriteString(str[start:i])
			b.WriteString("\n")
		} else {
			lineID++
			if len(lines) <= lineID {
				b.WriteString(str[i+1:])
				break
			}
		}

		count++
		start = i + 1
	}

	if len(lines) > lineID {
		return str, fmt.Errorf("not all lines were removed: processed line ids before %d for lines %d", lineID, lines)
	}

	return b.String(), nil
}
