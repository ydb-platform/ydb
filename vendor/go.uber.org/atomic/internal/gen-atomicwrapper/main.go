// Copyright (c) 2020-2022 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

// gen-atomicwrapper generates wrapper types around other atomic types.
//
// It supports plugging in functions which convert the value inside the atomic
// type to the user-facing value. For example,
//
// Given, atomic.Value and the functions,
//
//	func packString(string) interface{}
//	func unpackString(interface{}) string
//
// We can run the following command:
//
//	gen-atomicwrapper -name String -wrapped Value \
//	  -type string -pack fromString -unpack tostring
//
// This wil generate approximately,
//
//	type String struct{ v Value }
//
//	func (s *String) Load() string {
//	  return unpackString(v.Load())
//	}
//
//	func (s *String) Store(s string) {
//	  return s.v.Store(packString(s))
//	}
//
// The packing/unpacking logic allows the stored value to be different from
// the user-facing value.
package main

import (
	"bytes"
	"embed"
	"errors"
	"flag"
	"fmt"
	"go/format"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"text/template"
	"time"
)

func main() {
	log.SetFlags(0)
	if err := run(os.Args[1:]); err != nil {
		log.Fatalf("%+v", err)
	}
}

type stringList []string

func (sl *stringList) String() string {
	return strings.Join(*sl, ",")
}

func (sl *stringList) Set(s string) error {
	for _, i := range strings.Split(s, ",") {
		*sl = append(*sl, strings.TrimSpace(i))
	}
	return nil
}

func run(args []string) error {
	var opts struct {
		Name    string
		Wrapped string
		Type    string

		Imports      stringList
		Pack, Unpack string

		CAS            bool
		CompareAndSwap bool
		Swap           bool
		JSON           bool

		File   string
		ToYear int
	}

	opts.ToYear = time.Now().Year()

	flag := flag.NewFlagSet("gen-atomicwrapper", flag.ContinueOnError)

	// Required flags
	flag.StringVar(&opts.Name, "name", "",
		"name of the generated type (e.g. Duration)")
	flag.StringVar(&opts.Wrapped, "wrapped", "",
		"name of the wrapped atomic (e.g. Int64)")
	flag.StringVar(&opts.Type, "type", "",
		"name of the type exposed by the atomic (e.g. time.Duration)")

	// Optional flags
	flag.Var(&opts.Imports, "imports",
		"comma separated list of imports to add")
	flag.StringVar(&opts.Pack, "pack", "",
		"function to transform values with before storage")
	flag.StringVar(&opts.Unpack, "unpack", "",
		"function to reverse packing on loading")
	flag.StringVar(&opts.File, "file", "",
		"output file path (default: stdout)")

	// Switches for individual methods. Underlying atomics must support
	// these.
	flag.BoolVar(&opts.CAS, "cas", false,
		"generate a deprecated `CAS(old, new) bool` method; requires -pack")
	flag.BoolVar(&opts.CompareAndSwap, "compareandswap", false,
		"generate a `CompareAndSwap(old, new) bool` method; requires -pack")
	flag.BoolVar(&opts.Swap, "swap", false,
		"generate a `Swap(new) old` method; requires -pack and -unpack")
	flag.BoolVar(&opts.JSON, "json", false,
		"generate `MarshalJSON/UnmarshJSON` methods")

	if err := flag.Parse(args); err != nil {
		return err
	}

	if len(opts.Name) == 0 ||
		len(opts.Wrapped) == 0 ||
		len(opts.Type) == 0 ||
		len(opts.Pack) == 0 ||
		len(opts.Unpack) == 0 {
		return errors.New("flags -name, -wrapped, -pack, -unpack and -type are required")
	}

	if opts.CAS {
		opts.CompareAndSwap = true
	}

	var w io.Writer = os.Stdout
	if file := opts.File; len(file) > 0 {
		f, err := os.Create(file)
		if err != nil {
			return fmt.Errorf("create %q: %v", file, err)
		}
		defer f.Close()

		w = f
	}

	// Import encoding/json if needed.
	if opts.JSON {
		found := false
		for _, imp := range opts.Imports {
			if imp == "encoding/json" {
				found = true
				break
			}
		}

		if !found {
			opts.Imports = append(opts.Imports, "encoding/json")
		}
	}

	sort.Strings([]string(opts.Imports))

	var buff bytes.Buffer
	if err := _tmpl.ExecuteTemplate(&buff, "wrapper.tmpl", opts); err != nil {
		return fmt.Errorf("render template: %v", err)
	}

	bs, err := format.Source(buff.Bytes())
	if err != nil {
		return fmt.Errorf("reformat source: %v", err)
	}

	io.WriteString(w, "// @generated Code generated by gen-atomicwrapper.\n\n")
	_, err = w.Write(bs)
	return err
}

var (
	//go:embed *.tmpl
	_tmplFS embed.FS

	_tmpl = template.Must(template.New("atomicwrapper").ParseFS(_tmplFS, "*.tmpl"))
)
