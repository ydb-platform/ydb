test:
	meson setup -Dbuild_tests=true -Duse_openmp=false --warnlevel 2 --werror --buildtype release builddir
	cd builddir && ninja

test_openmp:
	meson setup -Dbuild_tests=true -Duse_openmp=true --warnlevel 2 --werror --buildtype release builddir
	cd builddir && ninja

bench:
	meson setup -Dbuild_benchmarks=true --warnlevel 2 --werror --buildtype release builddir
	cd builddir && ninja

debug:
	meson setup -Dbuild_tests=true --warnlevel 2 --werror --buildtype debug debug
	cd debug && ninja

sharedlib:
	meson setup --warnlevel 2 --werror --buildtype release builddir
	cd builddir && ninja

install:
	meson setup --warnlevel 2 --werror --buildtype release builddir
	cd builddir && meson install

clean:
	$(RM) -rf $(TESTOBJS) $(BENCHOBJS) $(UTILOBJS) testexe benchexe builddir debug
