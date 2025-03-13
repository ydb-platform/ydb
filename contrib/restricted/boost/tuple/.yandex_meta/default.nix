self: super: with self; {
  boost_tuple = stdenv.mkDerivation rec {
    pname = "boost_tuple";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "tuple";
      rev = "boost-${version}";
      hash = "sha256-Xw5lI5HhfkoQGlsMORzfyY81Eb73TZTJVSBiNTWt9zY=";
    };
  };
}
