package main

import (
	"flag"
	"fmt"
	"go/importer"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	usageTemplate = "Usage: %s [-benchmarks] [-examples] [-tests] import-path\n"
)

func findObjectByName(pkg *types.Package, re *regexp.Regexp, name string) types.Object {
	if pkg != nil && re != nil && len(name) > 0 {
		if obj := pkg.Scope().Lookup(name); obj != nil {
			if re.MatchString(obj.Type().String()) {
				return obj
			}
		}
	}
	return nil
}

func isTestName(name, prefix string) bool {
	ok := false
	if strings.HasPrefix(name, prefix) {
		if len(name) == len(prefix) {
			ok = true
		} else {
			rune, _ := utf8.DecodeRuneInString(name[len(prefix):])
			ok = !unicode.IsLower(rune)
		}
	}
	return ok
}

func main() {
	testsPtr := flag.Bool("tests", false, "report tests")
	benchmarksPtr := flag.Bool("benchmarks", false, "report benchmarks")
	examplesPtr := flag.Bool("examples", false, "report examples")

	flag.Usage = func() {
		_, _ = fmt.Fprintf(flag.CommandLine.Output(), usageTemplate, filepath.Base(os.Args[0]))
		flag.PrintDefaults()
	}

	flag.Parse()

	// Check if the number of positional parameters matches
	args := flag.Args()
	argsCount := len(args)
	if argsCount != 1 {
		exitCode := 0
		if argsCount > 1 {
			fmt.Println("Error: invalid number of parameters...")
			exitCode = 1
		}
		flag.Usage()
		os.Exit(exitCode)
	}

	importPath := args[0]

	var fset token.FileSet
	imp := importer.ForCompiler(&fset, runtime.Compiler, nil)
	pkg, err := imp.Import(importPath)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}

	if !*testsPtr && !*benchmarksPtr && !*examplesPtr {
		// Nothing to do, just exit normally
		os.Exit(0)
	}

	// // First approach: just dump the package scope as a string
	// // package "junk/snermolaev/libmath" scope 0xc0000df540 {
	// // .  func junk/snermolaev/libmath.Abs(a int) int
	// // .  func junk/snermolaev/libmath.AbsReport(s string)
	// // .  func junk/snermolaev/libmath.Sum(a int, b int) int
	// // .  func junk/snermolaev/libmath.TestAbs(t *testing.T)
	// // .  func junk/snermolaev/libmath.TestSum(t *testing.T)
	// // .  func junk/snermolaev/libmath.init()
	// // }
	// // and then collect all functions that match test function signature
	// pkgPath := pkg.Path()
	// scopeContent := strings.Split(pkg.Scope().String(), "\n")
	// re := regexp.MustCompile("^\\.\\s*func\\s*" + pkgPath + "\\.(Test\\w*)\\(\\s*\\w*\\s*\\*\\s*testing\\.T\\s*\\)$")
	// for _, name := range scopeContent {
	//     match := re.FindAllStringSubmatch(name, -1)
	//     if len(match) > 0 {
	//         fmt.Println(match[0][1])
	//     }
	// }

	// Second approach: look through all names defined in the pkg scope
	// and collect those functions that match test function signature
	// Unfortunately I failed to employ reflection mechinary for signature
	// comparison for unknown reasons (this needs additional investigation
	// I am going to use regexp as workaround for a while)
	// testFunc := func (*testing.T) {}
	//     for ...
	//        ...
	//        if reflect.DeepEqual(obj.Type(), reflect.TypeOf(testFunc)) {
	//            // this condition doesn't work
	//        }
	reBenchmark := regexp.MustCompile(`^func\(\w*\s*\*testing\.B\)$`)
	reExample := regexp.MustCompile(`^func\(\s*\)$`)
	reTest := regexp.MustCompile(`^func\(\w*\s*\*testing\.T\)$`)
	reTestMain := regexp.MustCompile(`^func\(\w*\s*\*testing\.M\)$`)

	var re *regexp.Regexp
	names := pkg.Scope().Names()

	var testFns []types.Object

	for _, name := range names {
		if name == "TestMain" && findObjectByName(pkg, reTestMain, name) != nil {
			fmt.Println("#TestMain")
			continue
		}

		switch {
		case *benchmarksPtr && isTestName(name, "Benchmark"):
			re = reBenchmark
		case *examplesPtr && isTestName(name, "Example"):
			re = reExample
		case *testsPtr && isTestName(name, "Test"):
			re = reTest
		default:
			continue
		}

		if obj := findObjectByName(pkg, re, name); obj != nil {
			testFns = append(testFns, obj)
		}
	}

	sort.Slice(testFns, func(i, j int) bool {
		iPos := testFns[i].Pos()
		jPos := testFns[j].Pos()

		if !iPos.IsValid() || !jPos.IsValid() {
			return iPos < jPos
		}

		iPosition := fset.PositionFor(iPos, true)
		jPosition := fset.PositionFor(jPos, true)

		return iPosition.Filename < jPosition.Filename ||
			(iPosition.Filename == jPosition.Filename && iPosition.Line < jPosition.Line)
	})

	for _, testFn := range testFns {
		fmt.Println(testFn.Name())
	}
}
