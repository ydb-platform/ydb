self: super: with self; {
  boost_random = stdenv.mkDerivation rec {
    pname = "boost_random";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "random";
      rev = "boost-${version}";
      hash = "sha256-lMGnW8zFFI5Dbnp8g014X+EpZGVEGRaBITY/daFIr1U=";
    };
  };
}
