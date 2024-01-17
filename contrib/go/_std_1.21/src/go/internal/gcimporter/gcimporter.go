// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package gcimporter implements Import for gc-generated object files.
package gcimporter // import "go/internal/gcimporter"

import (
	"bufio"
	"bytes"
	"fmt"
	"go/build"
	"go/token"
	"go/types"
	"internal/pkgbits"
	"internal/saferio"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
)

// debugging/development support
const debug = false

var exportMap sync.Map // package dir → func() (string, bool)

// lookupGorootExport returns the location of the export data
// (normally found in the build cache, but located in GOROOT/pkg
// in prior Go releases) for the package located in pkgDir.
//
// (We use the package's directory instead of its import path
// mainly to simplify handling of the packages in src/vendor
// and cmd/vendor.)
func lookupGorootExport(pkgDir string) (string, bool) {
	f, ok := exportMap.Load(pkgDir)
	if !ok {
		var (
			listOnce   sync.Once
			exportPath string
		)
		f, _ = exportMap.LoadOrStore(pkgDir, func() (string, bool) {
			listOnce.Do(func() {
				cmd := exec.Command(filepath.Join(build.Default.GOROOT, "bin", "go"), "list", "-export", "-f", "{{.Export}}", pkgDir)
				cmd.Dir = build.Default.GOROOT
				cmd.Env = append(cmd.Environ(), "GOROOT="+build.Default.GOROOT)
				var output []byte
				output, err := cmd.Output()
				if err != nil {
					return
				}

				exports := strings.Split(string(bytes.TrimSpace(output)), "\n")
				if len(exports) != 1 {
					return
				}

				exportPath = exports[0]
			})

			return exportPath, exportPath != ""
		})
	}

	return f.(func() (string, bool))()
}

var pkgExts = [...]string{".a", ".o"} // a file from the build cache will have no extension

// FindPkg returns the filename and unique package id for an import
// path based on package information provided by build.Import (using
// the build.Default build.Context). A relative srcDir is interpreted
// relative to the current working directory.
// If no file was found, an empty filename is returned.
func FindPkg(path, srcDir string) (filename, id string) {
	if path == "" {
		return
	}

	var noext string
	switch {
	default:
		// "x" -> "$GOPATH/pkg/$GOOS_$GOARCH/x.ext", "x"
		// Don't require the source files to be present.
		if abs, err := filepath.Abs(srcDir); err == nil { // see issue 14282
			srcDir = abs
		}
		bp, _ := build.Import(path, srcDir, build.FindOnly|build.AllowBinary)
		if bp.PkgObj == "" {
			var ok bool
			if bp.Goroot && bp.Dir != "" {
				filename, ok = lookupGorootExport(bp.Dir)
			}
			if !ok {
				id = path // make sure we have an id to print in error message
				return
			}
		} else {
			noext = strings.TrimSuffix(bp.PkgObj, ".a")
		}
		id = bp.ImportPath

	case build.IsLocalImport(path):
		// "./x" -> "/this/directory/x.ext", "/this/directory/x"
		noext = filepath.Join(srcDir, path)
		id = noext

	case filepath.IsAbs(path):
		// for completeness only - go/build.Import
		// does not support absolute imports
		// "/x" -> "/x.ext", "/x"
		noext = path
		id = path
	}

	if false { // for debugging
		if path != id {
			fmt.Printf("%s -> %s\n", path, id)
		}
	}

	if filename != "" {
		if f, err := os.Stat(filename); err == nil && !f.IsDir() {
			return
		}
	}
	// try extensions
	for _, ext := range pkgExts {
		filename = noext + ext
		if f, err := os.Stat(filename); err == nil && !f.IsDir() {
			return
		}
	}

	filename = "" // not found
	return
}

// Import imports a gc-generated package given its import path and srcDir, adds
// the corresponding package object to the packages map, and returns the object.
// The packages map must contain all packages already imported.
func Import(fset *token.FileSet, packages map[string]*types.Package, path, srcDir string, lookup func(path string) (io.ReadCloser, error)) (pkg *types.Package, err error) {
	var rc io.ReadCloser
	var id string
	if lookup != nil {
		// With custom lookup specified, assume that caller has
		// converted path to a canonical import path for use in the map.
		if path == "unsafe" {
			return types.Unsafe, nil
		}
		id = path

		// No need to re-import if the package was imported completely before.
		if pkg = packages[id]; pkg != nil && pkg.Complete() {
			return
		}
		f, err := lookup(path)
		if err != nil {
			return nil, err
		}
		rc = f
	} else {
		var filename string
		filename, id = FindPkg(path, srcDir)
		if filename == "" {
			if path == "unsafe" {
				return types.Unsafe, nil
			}
			return nil, fmt.Errorf("can't find import: %q", id)
		}

		// no need to re-import if the package was imported completely before
		if pkg = packages[id]; pkg != nil && pkg.Complete() {
			return
		}

		// open file
		f, err := os.Open(filename)
		if err != nil {
			return nil, err
		}
		defer func() {
			if err != nil {
				// add file name to error
				err = fmt.Errorf("%s: %v", filename, err)
			}
		}()
		rc = f
	}
	defer rc.Close()

	buf := bufio.NewReader(rc)
	hdr, size, err := FindExportData(buf)
	if err != nil {
		return
	}

	switch hdr {
	case "$$\n":
		err = fmt.Errorf("import %q: old textual export format no longer supported (recompile library)", path)

	case "$$B\n":
		var exportFormat byte
		if exportFormat, err = buf.ReadByte(); err != nil {
			return
		}
		size--

		// The unified export format starts with a 'u'; the indexed export
		// format starts with an 'i'; and the older binary export format
		// starts with a 'c', 'd', or 'v' (from "version"). Select
		// appropriate importer.
		switch exportFormat {
		case 'u':
			var data []byte
			var r io.Reader = buf
			if size >= 0 {
				if data, err = saferio.ReadData(r, uint64(size)); err != nil {
					return
				}
			} else if data, err = io.ReadAll(r); err != nil {
				return
			}
			s := string(data)
			s = s[:strings.LastIndex(s, "\n$$\n")]

			input := pkgbits.NewPkgDecoder(id, s)
			pkg = readUnifiedPackage(fset, nil, packages, input)
		case 'i':
			pkg, err = iImportData(fset, packages, buf, id)
		default:
			err = fmt.Errorf("import %q: old binary export format no longer supported (recompile library)", path)
		}

	default:
		err = fmt.Errorf("import %q: unknown export data header: %q", path, hdr)
	}

	return
}

type byPath []*types.Package

func (a byPath) Len() int           { return len(a) }
func (a byPath) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byPath) Less(i, j int) bool { return a[i].Path() < a[j].Path() }
