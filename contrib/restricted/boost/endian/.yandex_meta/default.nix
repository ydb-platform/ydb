self: super: with self; {
  boost_endian = stdenv.mkDerivation rec {
    pname = "boost_endian";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "endian";
      rev = "boost-${version}";
      hash = "sha256-PcuHs2/tBV8ZE8Cu3LN6B68WBCc7S8G+Laoe+cu9xD8=";
    };
  };
}
