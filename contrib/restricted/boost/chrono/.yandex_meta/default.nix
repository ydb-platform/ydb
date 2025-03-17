self: super: with self; {
  boost_chrono = stdenv.mkDerivation rec {
    pname = "boost_chrono";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "chrono";
      rev = "boost-${version}";
      hash = "sha256-pwuPMr+mo5u8xHJgV8+tysVO5NFMJwzsktULyR8cdo0=";
    };
  };
}
