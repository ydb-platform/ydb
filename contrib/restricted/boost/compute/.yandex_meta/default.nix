self: super: with self; {
  boost_compute = stdenv.mkDerivation rec {
    pname = "boost_compute";
    version = "1.90.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "compute";
      rev = "boost-${version}";
      hash = "sha256-OwoB+PLiDp5QdKy77qDFfBMJ6e8WzLjAC8OBWTiIB+Q=";
    };
  };
}
