package lz4block

import (
	"bytes"
	"strings"
	"testing"
)

func TestBlockDecode(t *testing.T) {
	appendLen := func(p []byte, size int) []byte {
		for size > 0xFF {
			p = append(p, 0xFF)
			size -= 0xFF
		}

		p = append(p, byte(size))
		return p
	}

	emitSeq := func(lit string, offset uint16, matchLen int) []byte {
		var b byte
		litLen := len(lit)
		if litLen < 15 {
			b = byte(litLen << 4)
			litLen = -1
		} else {
			b = 0xF0
			litLen -= 15
		}

		if matchLen < 4 || offset == 0 {
			out := []byte{b}
			if litLen >= 0 {
				out = appendLen(out, litLen)
			}
			return append(out, lit...)
		}

		matchLen -= 4
		if matchLen < 15 {
			b |= byte(matchLen)
			matchLen = -1
		} else {
			b |= 0x0F
			matchLen -= 15
		}

		out := []byte{b}
		if litLen >= 0 {
			out = appendLen(out, litLen)
		}

		if len(lit) > 0 {
			out = append(out, lit...)
		}

		out = append(out, byte(offset), byte(offset>>8))

		if matchLen >= 0 {
			out = appendLen(out, matchLen)
		}

		return out
	}
	concat := func(in ...[]byte) []byte {
		var p []byte
		for _, b := range in {
			p = append(p, b...)
		}
		return p
	}

	tests := []struct {
		name string
		src  []byte
		exp  []byte
	}{
		{
			"empty_input",
			[]byte{0},
			[]byte{},
		},
		{
			"empty_input_nil_dst",
			[]byte{0},
			nil,
		},
		{
			"literal_only_short",
			emitSeq("hello", 0, 0),
			[]byte("hello"),
		},
		{
			"literal_only_long",
			emitSeq(strings.Repeat("A", 15+255+255+1), 0, 0),
			bytes.Repeat([]byte("A"), 15+255+255+1),
		},
		{
			"literal_only_long_1",
			emitSeq(strings.Repeat("A", 15), 0, 0),
			bytes.Repeat([]byte("A"), 15),
		},
		{
			"literal_only_long_2",
			emitSeq(strings.Repeat("A", 16), 0, 0),
			bytes.Repeat([]byte("A"), 16),
		},
		{
			"repeat_match_len",
			emitSeq("a", 1, 4),
			[]byte("aaaaa"),
		},
		{
			"repeat_match_len_2_seq",
			concat(emitSeq("a", 1, 4), emitSeq("B", 1, 4)),
			[]byte("aaaaaBBBBB"),
		},
		{
			"long_match",
			emitSeq("A", 1, 16),
			bytes.Repeat([]byte("A"), 17),
		},
		{
			// Triggers a case in the amd64 decoder where its last action
			// is a call to runtime.memmove.
			"memmove_last",
			emitSeq(strings.Repeat("ABCD", 20), 80, 60),
			bytes.Repeat([]byte("ABCD"), 36)[:140],
		},
		{
			"repeat_match_log_len_2_seq",
			concat(emitSeq("a", 1, 15), emitSeq("B", 1, 15), emitSeq("end", 0, 0)),
			[]byte(strings.Repeat("a", 16) + strings.Repeat("B", 16) + "end"),
		},
		{
			"fuzz-3e4ce8cc0da392ca5a353b6ffef6d08f400ac5f9",
			[]byte("0000\x01\x00"),
			[]byte("0000000"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			buf := make([]byte, len(test.exp))
			n := decodeBlock(buf, test.src, nil)
			if n != len(test.exp) {
				t.Errorf("expected %d, got %d", len(test.exp), n)
			}
			if !bytes.Equal(buf, test.exp) {
				t.Fatalf("expected %q got %q", test.exp, buf)
			}
		})
	}
}

func TestDecodeBlockInvalid(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		src  string
		size int // Output size to try.
	}{
		{
			"empty_input",
			"",
			100,
		},
		{
			// A string of zeros could be interpreted as empty match+empty literal repeated,
			// but only the last block may have an empty match (with the offset missing).
			"repeated_zero_few",
			string(make([]byte, 6)),
			100,
		},
		{
			"repeated_zero_many",
			string(make([]byte, 100)),
			100,
		},
		{
			"final_lit_too_short",
			"\x20a", // litlen = 2 but only a single-byte literal
			100,
		},
		{
			"no_space_for_offset",
			"\x01", // mLen > 0 but no following offset.
			10,
		},
		{
			"write_beyond_len_dst",
			"\x1b0\x01\x00000000000000",
			len("\x1b0\x01\x00000000000000"),
		},
		{
			"bounds-crasher", // Triggered a broken bounds check in amd64 decoder.
			"\x000000",
			10,
		},
		{
			// Zero offset in a short literal.
			"zero_offset",
			"\xe1abcdefghijklmn\x00\x00\xe0abcdefghijklmn",
			40,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			dst := make([]byte, test.size+8)
			for i := range dst {
				dst[i] = byte(i)
			}
			dst = dst[:test.size]

			r := decodeBlock(dst, []byte(test.src), nil)
			if r >= 0 {
				t.Errorf("no error for %s", test.name)
			}

			dst = dst[:cap(dst)]
			for i := test.size; i < len(dst); i++ {
				if dst[i] != byte(i) {
					t.Error("decodeBlock wrote out of bounds")
					break
				}
			}
		})
	}
}

// Literal lengths should be checked for overflow.
//
// This test exists primarily for 32-bit platforms. Since a length n takes
// around n/255 bytes to encode, overflow can only occur on a 64-bit processor
// in blocks larger than 32PiB and decodeBlock will error out because the
// literal is too small.
func TestLongLengths(t *testing.T) {
	// n + 15 is large enough to overflow uint32.
	const (
		n      = (1 << 32) / 255
		remain = (255*n + 15) % (1 << 32)
	)

	src := make([]byte, 0, 100+n) // Just over 16MiB.
	src = append(src, '\xf0')
	for i := 0; i < n; i++ {
		src = append(src, 255)
	}
	src = append(src, 0)
	for i := 0; i < remain; i++ {
		src = append(src, 'A'+byte(i))
	}

	dst := make([]byte, 2*remain)
	r := decodeBlock(dst, src, nil)
	if r >= 0 {
		t.Errorf("want error, got %d (remain=%d)", r, remain)
	}
}

func TestDecodeWithDict(t *testing.T) {
	for _, c := range []struct {
		src, dict, want string // If want == "", expect an error.
	}{
		// Entire match in dictionary.
		{"\x11b\x0a\x00\x401234", "barbazquux", "barbaz1234"},

		// First part in dictionary, rest in dst.
		{"\x35foo\x09\x00\x401234", "0barbaz", "foobarbazfoo1234"},

		// Copy end of dictionary three times, then a literal.
		{"\x08\x04\x00\x50abcde", "---1234", "123412341234abcde"},

		// First part in dictionary, rest in dst, copied multiple times.
		{"\x1a1\x05\x00\x50abcde", "---2345", "123451234512345abcde"},

		// First part in dictionary, rest in dst, but >16 bytes before the end,
		// to test the short match shortcut in the amd64 decoder.
		{"\x35abc\x09\x00\xf0\x0f0123456789abcdefghijklmnopqrst", "012defghi",
			"abcdefghiabc0123456789abcdefghijklmnopqrst"},

		// Offset points before start of dictionary.
		{"\x35foo\xff\xff\x401234", "XYZ", ""},
	} {
		// Add sentinel for debugging.
		dict := []byte(c.dict + "ABCD")[:len(c.dict)]
		dst := make([]byte, 100)

		r := decodeBlock(dst, []byte(c.src), dict)

		if c.want == "" {
			if r >= 0 {
				t.Error("expected an error, got", r)
			}
		} else {
			switch {
			case r < 0:
				t.Errorf("error %d for %q", r, c.want)
			case string(dst[:r]) != c.want:
				t.Errorf("expected %q, got %q", c.want, dst[:r])
			}
		}
	}
}
