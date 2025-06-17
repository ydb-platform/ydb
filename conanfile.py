from conan import ConanFile

import os

from conan.tools.files import copy
from conan.tools.cmake import CMakeToolchain, CMakeDeps, cmake_layout
from conan.tools.env import Environment


class App(ConanFile):

    settings = "os", "compiler", "build_type", "arch"

    default_options = {}

    def build_requirements(self):
        self.tool_requires("bison/3.8.2")
        self.tool_requires("m4/1.4.19")
        self.tool_requires("ragel/6.10")
        self.tool_requires("yasm/1.3.0")

    def generate(self):
        CMakeDeps(self).generate()
        CMakeToolchain(self).generate()

        for dep in self.dependencies.values():
            for bindir in dep.cpp_info.bindirs:
                copy(self, pattern="*yasm*", src=bindir, dst=self.build_folder + "../../../.././bin")
            for bindir in dep.cpp_info.bindirs:
                copy(self, pattern="bison*", src=bindir, dst=self.build_folder + "../../../.././bin/bison/bin")
            for bindir in dep.cpp_info.bindirs:
                copy(self, pattern="m4*", src=bindir, dst=self.build_folder + "../../../.././bin/m4/bin")
            for bindir in dep.cpp_info.bindirs:
                copy(self, pattern="ragel*", src=bindir, dst=self.build_folder + "../../../.././bin")
            for bindir in dep.cpp_info.bindirs:
                copy(self, pattern="ytasm*", src=bindir, dst=self.build_folder + "../../../.././bin")
            for resdir in dep.cpp_info.resdirs:
                copy(self, pattern="*", src=resdir, dst=self.build_folder + "../../../.././bin/bison/res")

    def layout(self):
        cmake_layout(self)
