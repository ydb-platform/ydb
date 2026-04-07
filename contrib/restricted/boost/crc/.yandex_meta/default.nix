self: super: with self; {
  boost_crc = stdenv.mkDerivation rec {
    pname = "boost_crc";
    version = "1.90.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "crc";
      rev = "boost-${version}";
      hash = "sha256-PeCMDZX9Iwoi/R8HFYMgOL4EOiyjDnaUma4SnRc5xIk=";
    };
  };
}
