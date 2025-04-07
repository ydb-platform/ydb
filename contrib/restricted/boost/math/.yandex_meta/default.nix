self: super: with self; {
  boost_math = stdenv.mkDerivation rec {
    pname = "boost_math";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "math";
      rev = "boost-${version}";
      hash = "sha256-qrv6ySO++gKkHhXMqnmuh21twljH0QnDtfGg65uM4eo=";
    };
  };
}
