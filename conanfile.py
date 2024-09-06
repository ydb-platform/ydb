from conan import ConanFile


class App(ConanFile):

    settings = "os", "compiler", "build_type", "arch"

    options = {"libiconv:shared": "True"}

    tool_requires = "bison/3.8.2", "m4/1.4.19", "ragel/6.10", "yasm/1.3.0"

    def requirements(self):
        requires = [ "libiconv/1.15" ]
        for require in requires:
            if ("linux-headers" in require) and (self.settings.os != "Linux"):
                continue
            self.requires(require)

    generators = "cmake_find_package", "cmake_paths"

    def imports(self):
        self.copy(pattern="*yasm*", src="bin", dst="./bin")
        self.copy(pattern="bison*", src="bin", dst="./bin/bison/bin")
        self.copy(pattern="m4*", src="bin", dst="./bin/m4/bin")
        self.copy(pattern="ragel*", src="bin", dst="./bin")
        self.copy(pattern="ytasm*", src="bin", dst="./bin")
        self.copy(pattern="*", src="res", dst="./bin/bison/res")
