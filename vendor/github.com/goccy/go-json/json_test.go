package json_test

import (
	"bytes"
	stdjson "encoding/json"
	"math"
	"math/rand"
	"reflect"
	"testing"

	"github.com/goccy/go-json"
)

var validTests = []struct {
	data string
	ok   bool
}{
	{`foo`, false},
	{`}{`, false},
	{`{]`, false},
	{`{}`, true},
	{`{"foo":"bar"}`, true},
	{`{"foo":"bar","bar":{"baz":["qux"]}}`, true},
	{`[""],`, false},
}

func TestValid(t *testing.T) {
	for _, tt := range validTests {
		if ok := json.Valid([]byte(tt.data)); ok != tt.ok {
			t.Errorf("Valid(%#q) = %v, want %v", tt.data, ok, tt.ok)
		}
	}
}

func TestValidWithComplexData(t *testing.T) {
	data := []byte(`{"ABCDEFGHIJKL":[{"MNOPQRSTUVWX":[{"YABC":{"DEFG":[{"HIJKLMNO":{"PQRS":"TUVWXYABCDEFGHIJKLMNOPQRSTUVWXYABCDEFGHIJKLMNOPQRSTUVWXYABCDE","FGHIJKLM":"NOPQRSTUVW"},"XYABCDEFGH":[{"IJKLMNOP":"!=","Q":{"RSTU":"V","WXYABCDE":"FGHIJKLMNO"},"P":{"QRSTUVWX":"YAB"},"CDEFGHIJ":"KLMNOP","QRSTUVWX":"YABCDEFGHI"}],"JKLMNOPQRSTUVW":null,"XYABCDEF":"GHIJ"},{"KLMNOPQR":{"STUVWXY":{"ABCDEFGH":"IJKLMN_OPQ_RST","U":{"VWXY":"A","BCDEFGHI":"JKLMNOPQRS"},"TUVWXYAB":"CDEFG","HIJKLMNO":"PQRSTUVWXY"},"ABCDEFGH":"IJKLMNOP!Q41R8ST98U00V204W9800998XYA8427B","CDEFGHIJ":"KLMNOP","QRSTUVWX":"YABCDEFGHI"},"JKLMNOPQRS":null,"TUVWXYABCDEFGH":null,"IJKLMNOP":"QRST"}],"UVWXYABC":"DEFGH","IJKLMNOP":"QRSTUVWXY"},"ABCDEFGH":9,"IJKL":"MNOPQRST/UVWXYABCDE//FGHIJKLMNOPQRST!4UV2WXYABC7826D7659EF223GH40I91J","KLMNOPQRST":[{"UVWXYABCDEFG":null,"HIJKLMNO":0,"PQRS":"T","UVWX":{"YABC":{"DEFG":"HIJK/LMNO/PQRSTU","VWXYABCD":"EFGHIJKLM","NOPQRSTU":"VWXY"},"ABCDEFGH":"IJKLMNO","PQRSTUVW":"XYAB"},"CDEFGHIJ":"KLMNOPQR"}],"STUVWXYA":null,"BCDEFGH":null,"IJKLMN":null,"OPQRSTUVWXYABC":null,"DEFGHIJK":"LMNOPQRS"},{"TUVW":{"XYAB":[{"CDEFGHIJ":{"KLMN":"OPQRSTUV/WXYABCDEFG//HIJKLMNOPQRSTUV!4WX2YABCDE7826F7659GH223IJ40K91L","MNOPQRST":"UVWXYABCDE"},"FGHIJKLMNO":[{"PQRS":"T","UVWXYABC":"DEFGHIJKLM"}],"NOPQRSTUVWXYAB":null,"CDEFGHIJ":"KLMN"}],"OPQRSTUV":"WXYAB","CDEFGHIJ":"KLMNOPQRS"},"TUVWXYAB":9,"CDEF":"GHIJKLMN/OPQRSTUVWX//YABCDEFGHIJKLM!4NO2PQRSTU7826V7659WX223YA40B91C","DEFGHIJKLM":[{"NOPQRSTUVWXY":null,"ABCDEFGH":0,"IJKL":"M","NOPQ":{"RSTU":{"VWXY":"ABCD/EFGH/IJKLMN","OPQRSTUV":"WXYABCDEF","GHIJKLMN":"OPQR"},"STUVWXYA":"BCDEFGH","IJKLMNOP":"QRST"},"UVWXYABC":"DEFGHIJK"}],"LMNOPQRS":null,"TUVWXYA":null,"BCDEFG":null,"HIJKLMNOPQRSTU":null,"VWXYABCD":"EFGHIJKL"},{"MNOP":{"QRST":[{"UVWXYABC":0,"DEFG":["HIJK"],"LMNO":[{"PQRS":{"TUVW":"XYABCDEF/GHIJKLMNOP","QRSTUVWX":"YABCDEFGH","IJKLMNOP":"QRST"},"UVWXYABC":"DEFGHIJ","KLMNOPQR":"STUV"}],"WXYAB":[{"CDEF":{"GHIJ":{"KLMN":"OPQRSTUV/WXYABCDEFG","HIJKLMNO":"PQRSTUVWX","YABCDEFG":"HIJK"},"LMNOPQRS":"TUVWXYA","BCDEFGHI":"JKLM"},"NOPQRSTU":"VWX"}],"YABCDEFG":"HIJKLMNOPQR","STUVWXYA":"BCDEFGHIJ"},{"KLMNOPQR":"=","S":{"TUVWXYA":{"BCDE":"FGHI","JKLMNOPQ":"RSTUVWXYAB"},"CDEFGHIJ":"@KLMN/OPQR/STUVWX","YABCDEFG":"HIJKLM","NOPQRSTU":"VWXYABCDEF"},"G":{"HIJKLMNO":{"PQRS":"TUVW/XYAB/CDEFGH//IJKLMN!O41P8QR98S00T204U9800998VWX8427Y","ABCDEFGH":"IJKLMNOPQR"},"STUVWXYABC":null,"DEFGHIJKLMNOPQ":null,"RSTUVWXY":"ABCD"},"EFGHIJKL":"MNOPQR","STUVWXYA":"BCDEFGHIJK"},{"LMNOPQR":[{"STUV":"WXYA","BCDEFGHI":"JKLMNOPQRS"}],"TUVWXYAB":"CDEFGH","IJKLMNOP":"QRSTUVWXY"}],"ABCDEFGH":"IJKLM","NOPQRSTU":"VWXYABCDE"},"FGHIJKLM":37,"NOPQ":"RSTUVWXY/ABCDEFGHIJ//KLMNOPQRST!U41V8WX98Y00A204B9800998CDE8427F","GHIJKLMNOP":null,"QRSTUVWX":null,"YABCDEF":[{"GHIJKLMNOPQR":null,"STUVWXYA":0,"BCDE":"","FGHI":{"JKLM":{"NOPQ":"RSTUVWXY/ABCDEFGHIJ","KLMNOPQR":"STUVWXYAB","CDEFGHIJ":"KLMN"},"OPQRSTUV":"WXYABCD","EFGHIJKL":"MNOP"},"QRSTUVWX":"YABCDEFG"}],"HIJKLM":null,"NOPQRSTUVWXYAB":null,"CDEFGHIJ":"KLMNOPQR"}],"STUVWXYABC":null,"DEFGHIJK":[{"LMNO":{"PQRS":"TUVW/XYAB/CDEFGH","IJKLMNOP":"QRSTUVWXY","ABCDEFGH":"IJKL"},"MNOPQRST":"UVWXYAB","CDEFGHIJ":"KLMN"}],"OPQRSTUV":1,"WXYA":"BCDEFGHI/JKLMNOPQRS","TUVWXYAB":null,"CDEFGHIJKLMNOP":null,"QRSTUVWX":"YABCDE","FGHIJKLM":"NOPQ"}]}`)
	expected := stdjson.Valid(data)
	actual := json.Valid(data)
	if expected != actual {
		t.Fatalf("failed to valid: expected %v but got %v", expected, actual)
	}
}

type example struct {
	compact string
	indent  string
}

var examples = []example{
	{`1`, `1`},
	{`{}`, `{}`},
	{`[]`, `[]`},
	{`{"":2}`, "{\n\t\"\": 2\n}"},
	{`[3]`, "[\n\t3\n]"},
	{`[1,2,3]`, "[\n\t1,\n\t2,\n\t3\n]"},
	{`{"x":1}`, "{\n\t\"x\": 1\n}"},
	{ex1, ex1i},
	{"{\"\":\"<>&\u2028\u2029\"}", "{\n\t\"\": \"<>&\u2028\u2029\"\n}"}, // See golang.org/issue/34070
}

var ex1 = `[true,false,null,"x",1,1.5,0,-5e+2]`

var ex1i = `[
	true,
	false,
	null,
	"x",
	1,
	1.5,
	0,
	-5e+2
]`

func TestCompact(t *testing.T) {
	var buf bytes.Buffer
	for _, tt := range examples {
		buf.Reset()
		t.Log("src = ", tt.compact)
		if err := json.Compact(&buf, []byte(tt.compact)); err != nil {
			t.Errorf("Compact(%#q): %v", tt.compact, err)
		} else if s := buf.String(); s != tt.compact {
			t.Errorf("Compact(%#q) = %#q, want original", tt.compact, s)
		}

		buf.Reset()
		if err := json.Compact(&buf, []byte(tt.indent)); err != nil {
			t.Errorf("Compact(%#q): %v", tt.indent, err)
			continue
		} else if s := buf.String(); s != tt.compact {
			t.Errorf("Compact(%#q) = %#q, want %#q", tt.indent, s, tt.compact)
		}
	}
	t.Run("invalid", func(t *testing.T) {
		for _, src := range []string{
			`invalid`,
			`}`,
			`]`,
			`{"a":1}}`,
			`{"a" 1}`,
			`{"a": 1 "b": 2}`,
			`["a" "b"]`,
			`"\`,
			`{"a":"\\""}`,
			`tr`,
			`{"a": tru, "b": 1}`,
			`fal`,
			`{"a": fals, "b": 1}`,
			`nu`,
			`{"a": nul, "b": 1}`,
			`1.234.567`,
			`[nul]`,
			`{}   1`,
		} {
			buf.Reset()
			if err := stdjson.Compact(&buf, []byte(src)); err == nil {
				t.Fatal("invalid test case")
			}
			buf.Reset()
			if err := json.Compact(&buf, []byte(src)); err == nil {
				t.Fatalf("%q: expected error", src)
			}
		}
	})
}

func TestCompactSeparators(t *testing.T) {
	// U+2028 and U+2029 should be escaped inside strings.
	// They should not appear outside strings.
	tests := []struct {
		in, compact string
	}{
		{"{\"\u2028\": 1}", "{\"\u2028\":1}"},
		{"{\"\u2029\" :2}", "{\"\u2029\":2}"},
	}
	for _, tt := range tests {
		var buf bytes.Buffer
		if err := json.Compact(&buf, []byte(tt.in)); err != nil {
			t.Errorf("Compact(%q): %v", tt.in, err)
		} else if s := buf.String(); s != tt.compact {
			t.Errorf("Compact(%q) = %q, want %q", tt.in, s, tt.compact)
		}
	}
}

func TestIndent(t *testing.T) {
	var buf bytes.Buffer
	for _, tt := range examples {
		buf.Reset()
		if err := json.Indent(&buf, []byte(tt.indent), "", "\t"); err != nil {
			t.Errorf("Indent(%#q): %v", tt.indent, err)
		} else if s := buf.String(); s != tt.indent {
			t.Errorf("Indent(%#q) = %#q, want original", tt.indent, s)
		}

		buf.Reset()
		if err := json.Indent(&buf, []byte(tt.compact), "", "\t"); err != nil {
			t.Errorf("Indent(%#q): %v", tt.compact, err)
			continue
		} else if s := buf.String(); s != tt.indent {
			t.Errorf("Indent(%#q) = %#q, want %#q", tt.compact, s, tt.indent)
		}
	}
	t.Run("invalid", func(t *testing.T) {
		for _, src := range []string{
			`invalid`,
			`}`,
			`]`,
			`{"a":1}}`,
			`{"a" 1}`,
			`{"a": 1 "b": 2}`,
			`["a" "b"]`,
			`"\`,
			`{"a":"\\""}`,
			`tr`,
			`{"a": tru, "b": 1}`,
			`fal`,
			`{"a": fals, "b": 1}`,
			`nu`,
			`{"a": nul, "b": 1}`,
			`1.234.567`,
			`[nul]`,
			`{}   1`,
		} {
			buf.Reset()
			if err := stdjson.Indent(&buf, []byte(src), "", " "); err == nil {
				t.Fatal("invalid test case")
			}
			buf.Reset()
			if err := json.Indent(&buf, []byte(src), "", " "); err == nil {
				t.Fatalf("%q: expected error", src)
			}
		}
	})
}

// Tests of a large random structure.
func TestCompactBig(t *testing.T) {
	initBig()
	var buf bytes.Buffer
	if err := json.Compact(&buf, jsonBig); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	b := buf.Bytes()
	if !bytes.Equal(b, jsonBig) {
		t.Error("Compact(jsonBig) != jsonBig")
		diff(t, b, jsonBig)
		return
	}
}

func TestIndentBig(t *testing.T) {
	//t.Parallel()
	initBig()
	var buf bytes.Buffer
	if err := json.Indent(&buf, jsonBig, "", "\t"); err != nil {
		t.Fatalf("Indent1: %v", err)
	}
	b := buf.Bytes()
	if len(b) == len(jsonBig) {
		// jsonBig is compact (no unnecessary spaces);
		// indenting should make it bigger
		t.Fatalf("Indent(jsonBig) did not get bigger")
	}

	// should be idempotent
	var buf1 bytes.Buffer
	if err := json.Indent(&buf1, b, "", "\t"); err != nil {
		t.Fatalf("Indent2: %v", err)
	}
	b1 := buf1.Bytes()
	if !bytes.Equal(b1, b) {
		t.Error("Indent(Indent(jsonBig)) != Indent(jsonBig)")
		diff(t, b1, b)
		return
	}

	// should get back to original
	buf1.Reset()
	if err := json.Compact(&buf1, b); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	b1 = buf1.Bytes()
	if !bytes.Equal(b1, jsonBig) {
		t.Error("Compact(Indent(jsonBig)) != jsonBig")
		diff(t, b1, jsonBig)
		return
	}
}

type indentErrorTest struct {
	in  string
	err error
}

var indentErrorTests = []indentErrorTest{
	{`{"X": "foo", "Y"}`, json.NewSyntaxError("invalid character '}' after object key", 17)},
	{`{"X": "foo" "Y": "bar"}`, json.NewSyntaxError("invalid character '\"' after object key:value pair", 13)},
}

func TestIndentErrors(t *testing.T) {
	for i, tt := range indentErrorTests {
		slice := make([]uint8, 0)
		buf := bytes.NewBuffer(slice)
		if err := json.Indent(buf, []uint8(tt.in), "", ""); err != nil {
			if !reflect.DeepEqual(err, tt.err) {
				t.Errorf("#%d: Indent: expected %#v but got %#v", i, tt.err, err)
				continue
			}
		}
	}
}

func diff(t *testing.T, a, b []byte) {
	t.Helper()
	for i := 0; ; i++ {
		if i >= len(a) || i >= len(b) || a[i] != b[i] {
			j := i - 10
			if j < 0 {
				j = 0
			}
			t.Errorf("diverge at %d: «%s» vs «%s»", i, trim(a[j:]), trim(b[j:]))
			return
		}
	}
}

func trim(b []byte) []byte {
	if len(b) > 20 {
		return b[0:20]
	}
	return b
}

// Generate a random JSON object.

var (
	jsonBig []byte
)

func initBig() {
	n := 10000
	if testing.Short() {
		n = 100
	}
	v := genValue(n)
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	jsonBig = b
}

func genValue(n int) interface{} {
	if n > 1 {
		switch rand.Intn(2) {
		case 0:
			return genArray(n)
		case 1:
			return genMap(n)
		}
	}
	switch rand.Intn(3) {
	case 0:
		return rand.Intn(2) == 0
	case 1:
		return rand.NormFloat64()
	case 2:
		return genString(30)
	}
	panic("unreachable")
}

func genString(stddev float64) string {
	n := int(math.Abs(rand.NormFloat64()*stddev + stddev/2))
	c := make([]rune, n)
	for i := range c {
		f := math.Abs(rand.NormFloat64()*64 + 32)
		if f > 0x10ffff {
			f = 0x10ffff
		}
		c[i] = rune(f)
	}
	return string(c)
}

func genArray(n int) []interface{} {
	f := int(math.Abs(rand.NormFloat64()) * math.Min(10, float64(n/2)))
	if f > n {
		f = n
	}
	if f < 1 {
		f = 1
	}
	x := make([]interface{}, f)
	for i := range x {
		x[i] = genValue(((i+1)*n)/f - (i*n)/f)
	}
	return x
}

func genMap(n int) map[string]interface{} {
	f := int(math.Abs(rand.NormFloat64()) * math.Min(10, float64(n/2)))
	if f > n {
		f = n
	}
	if n > 0 && f == 0 {
		f = 1
	}
	x := make(map[string]interface{})
	for i := 0; i < f; i++ {
		x[genString(10)] = genValue(((i+1)*n)/f - (i*n)/f)
	}
	return x
}
