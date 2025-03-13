self: super: with self; {
  boost_regex = stdenv.mkDerivation rec {
    pname = "boost_regex";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "regex";
      rev = "boost-${version}";
      hash = "sha256-lUHGrMsA6q2scH4DKE49JmHhCpcrVtfR1T/5Tu9rQwI=";
    };
  };
}
