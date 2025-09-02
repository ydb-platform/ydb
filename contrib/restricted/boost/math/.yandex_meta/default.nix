self: super: with self; {
  boost_math = stdenv.mkDerivation rec {
    pname = "boost_math";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "math";
      rev = "boost-${version}";
      hash = "sha256-hrpMduFTOu9xqajj6duHVNieWOHDu3Zl2uZbYBTKCSA=";
    };
  };
}
