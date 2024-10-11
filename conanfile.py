from conan import ConanFile


class App(ConanFile):

    settings = "os", "compiler", "build_type", "arch"

    default_options = {}

    def build_requirements(self):
        self.tool_requires("bison/3.8.2")
        self.tool_requires("m4/1.4.19")
        self.tool_requires("ragel/6.10")
        self.tool_requires("yasm/1.3.0")

    generators = "cmake_find_package", "cmake_paths"

    def imports(self):
        self.copy(pattern="*yasm*", src="bin", dst="./bin")
        self.copy(pattern="bison*", src="bin", dst="./bin/bison/bin")
        self.copy(pattern="m4*", src="bin", dst="./bin/m4/bin")
        self.copy(pattern="ragel*", src="bin", dst="./bin")
        self.copy(pattern="ytasm*", src="bin", dst="./bin")
        self.copy(pattern="*", src="res", dst="./bin/bison/res")
