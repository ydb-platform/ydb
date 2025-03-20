self: super: with self; {
  boost_throw_exception = stdenv.mkDerivation rec {
    pname = "boost_throw_exception";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "throw_exception";
      rev = "boost-${version}";
      hash = "sha256-qNUf32xhEHnEXeMlqHaqhJhez543ETf/lQ/9HR5RPDw=";
    };
  };
}
