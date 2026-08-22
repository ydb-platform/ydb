self: super: with self; {
  boost_tuple = stdenv.mkDerivation rec {
    pname = "boost_tuple";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "tuple";
      rev = "boost-${version}";
      hash = "sha256-iDSxeu2+h3KsomlKzGGHvAP5pIt7EoUd/fpxcaeoVbg=";
    };
  };
}
