// Package yatest provides access to testing context, when running under ya make -t.
package yatest

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sync"
)

type TestContext struct {
	Build struct {
		BuildType string            `json:"build_type"`
		Flags     map[string]string `json:"flags"`
		Sanitizer string            `json:"sanitizer"`
	} `json:"build"`

	Runtime struct {
		BuildRoot              string            `json:"build_root"`
		OutputPath             string            `json:"output_path"`
		ProjectPath            string            `json:"project_path"`
		PythonBin              string            `json:"python_bin"`
		PythonLibPath          string            `json:"python_lib_path"`
		RAMDrivePath           string            `json:"ram_drive_path"`
		YtHDDPath              string            `json:"yt_hdd_path"`
		TestOutputRAMDrivePath string            `json:"test_output_ram_drive_path"`
		SourceRoot             string            `json:"source_root"`
		WorkPath               string            `json:"work_path"`
		TestToolPath           string            `json:"test_tool_path"`
		TestParameters         map[string]string `json:"test_params"`
	} `json:"runtime"`

	Resources struct {
		Global map[string]string `json:"global"`
	} `json:"resources"`

	Internal struct {
		EnvFile string `json:"env_file"`
	} `json:"internal"`

	Initialized bool
}

var (
	context              TestContext
	isRunningUnderGoTest bool
	initOnce             sync.Once
)

func lazyInit() {
	initOnce.Do(doInit)
}

func verifyContext() {
	if !context.Initialized {
		panic("test context isn't initialized")
	}
}

func initTestContext() {
	data, err := os.ReadFile(getenv("YA_TEST_CONTEXT_FILE"))
	if err != nil {
		panic(err)
	}

	err = json.Unmarshal([]byte(data), &context)
	if err != nil {
		panic(err)
	}
	setupEnv()
	context.Initialized = true
}

func setupEnv() {
	file, err := os.Open(context.Internal.EnvFile)
	if err != nil {
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		var objmap map[string]json.RawMessage
		var val string
		err := json.Unmarshal([]byte(line), &objmap)
		if err != nil {
			panic(err)
		}
		for k, v := range objmap {
			err := json.Unmarshal(v, &val)
			if err != nil {
				panic(err)
			}
			err = os.Setenv(k, val)
			if err != nil {
				panic(err)
			}
		}
	}
}

func HasYaTestContext() bool {
	lazyInit()
	return context.Initialized
}

// GlobalResourcePath returns absolute path to a directory
// containing global build resource.
func GlobalResourcePath(name string) string {
	resource, ok := RelaxedGlobalResourcePath(name)
	if !ok {
		panic(fmt.Sprintf("global resource %s is not defined", name))
	}
	return resource
}

func RelaxedGlobalResourcePath(name string) (string, bool) {
	lazyInit()
	verifyContext()
	resource, ok := context.Resources.Global[name]
	return resource, ok
}

func getenv(name string) string {
	value := os.Getenv(name)
	if value == "" {
		panic(fmt.Sprintf("environment variable %s is not set", name))
	}
	return value
}

func CCompilerPath() string {
	lazyInit()
	return getenv("YA_CC")
}

func CxxCompilerPath() string {
	lazyInit()
	return getenv("YA_CXX")
}

// PythonBinPath returns absolute path to the python
//
// Warn: if you are using build with system python (-DUSE_SYSTEM_PYTHON=X) beware that some python bundles
// are built in a stripped-down form that is needed for building, not running tests.
// See comments in the file below to find out which version of python is compatible with tests.
// https://a.yandex-team.ru/arc/trunk/arcadia/build/platform/python/resources.inc
func PythonBinPath() string {
	lazyInit()
	verifyContext()
	return context.Runtime.PythonBin
}

func PythonLibPath() string {
	lazyInit()
	verifyContext()
	return context.Runtime.PythonLibPath
}

// SourcePath returns absolute path to source directory.
//
// arcadiaPath must be declared using DATA macro inside ya.make.
func SourcePath(arcadiaPath string) string {
	lazyInit()
	if path.IsAbs(arcadiaPath) {
		panic(fmt.Sprintf("relative path expected, but got %q", arcadiaPath))
	}

	// Don't verify context for SourcePath - it can be mined without context
	return filepath.Join(context.Runtime.SourceRoot, arcadiaPath)
}

// BuildPath returns absolute path to the build directory.
func BuildPath(dataPath string) string {
	lazyInit()
	if path.IsAbs(dataPath) {
		panic(fmt.Sprintf("relative path expected, but got %q", dataPath))
	}

	verifyContext()
	return filepath.Join(context.Runtime.BuildRoot, dataPath)
}

// WorkPath returns absolute path to the work directory (initial test cwd).
func WorkPath(dataPath string) string {
	lazyInit()
	if path.IsAbs(dataPath) {
		panic(fmt.Sprintf("relative path expected, but got %q", dataPath))
	}

	verifyContext()
	return filepath.Join(context.Runtime.WorkPath, dataPath)
}

// OutputPath returns absolute path to the output directory (testing_out_stuff).
func OutputPath(dataPath string) string {
	lazyInit()
	verifyContext()
	return filepath.Join(context.Runtime.OutputPath, dataPath)
}

// RAMDrivePath returns absolute path to the ramdrive directory
func RAMDrivePath(dataPath string) string {
	lazyInit()
	if path.IsAbs(dataPath) {
		panic(fmt.Sprintf("relative path expected, but got %q", dataPath))
	}

	verifyContext()
	return filepath.Join(context.Runtime.RAMDrivePath, dataPath)
}

// YtHDDPath returns absolute path to the directory mounted to ext4 fs in YT
func YtHDDPath(dataPath string) string {
	lazyInit()
	if path.IsAbs(dataPath) {
		panic(fmt.Sprintf("relative path expected, but got %q", dataPath))
	}

	verifyContext()
	return filepath.Join(context.Runtime.YtHDDPath, dataPath)
}

// OutputRAMDrivePath returns absolute path to the ramdrive output directory
func OutputRAMDrivePath(dataPath string) string {
	lazyInit()
	if path.IsAbs(dataPath) {
		panic(fmt.Sprintf("relative path expected, but got %q", dataPath))
	}

	verifyContext()
	return filepath.Join(context.Runtime.TestOutputRAMDrivePath, dataPath)
}

// HasRAMDrive returns true if ramdrive is enabled in tests
func HasRAMDrive() bool {
	lazyInit()
	verifyContext()
	return context.Runtime.RAMDrivePath != ""
}

func BinaryPath(dataPath string) (string, error) {
	if runtime.GOOS == "windows" {
		dataPath += ".exe"
	}
	buildPath := BuildPath("")
	binaryPath := filepath.Join(buildPath, dataPath)
	if _, err := os.Stat(binaryPath); os.IsNotExist(err) {
		return "", fmt.Errorf("cannot find binary %s: make sure it was added in the DEPENDS section", dataPath)
	} else {
		return binaryPath, nil
	}
}

// ProjectPath returns arcadia-relative path to the test project
func ProjectPath() string {
	lazyInit()
	verifyContext()
	return context.Runtime.ProjectPath
}

// BuildType returns build type that was used to compile the test
func BuildType() string {
	lazyInit()
	verifyContext()
	return context.Build.BuildType
}

// BuildFlag returns the value of the requested build flag
func BuildFlag(name string) (string, bool) {
	lazyInit()
	verifyContext()
	val, ok := context.Build.Flags[name]
	return val, ok
}

// TestParam returns the value of the requested test parameter
func TestParam(name string) (string, bool) {
	lazyInit()
	verifyContext()
	val, ok := context.Runtime.TestParameters[name]
	return val, ok
}

// Sanitizer returns sanitizer name that was used to compile the test
func Sanitizer() string {
	lazyInit()
	verifyContext()
	return context.Build.Sanitizer
}

func EnvFile() string {
	lazyInit()
	verifyContext()
	return context.Internal.EnvFile
}

func TestToolPath() string {
	lazyInit()
	verifyContext()
	return context.Runtime.TestToolPath
}
