self: super: with self; {
  boost_spirit = stdenv.mkDerivation rec {
    pname = "boost_spirit";
    version = "1.92.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "spirit";
      rev = "boost-${version}";
      hash = "sha256-tbre37nHvdNp6M99y1Dp8FwDU5zhkZq6q6D/J1dGGAs=";
    };
  };
}
