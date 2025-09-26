self: super: with self; {
  boost_throw_exception = stdenv.mkDerivation rec {
    pname = "boost_throw_exception";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "throw_exception";
      rev = "boost-${version}";
      hash = "sha256-kX0kizEaDbcmpGmm/DWN6kZieyWe0HtnXHdmN+mKYjE=";
    };
  };
}
