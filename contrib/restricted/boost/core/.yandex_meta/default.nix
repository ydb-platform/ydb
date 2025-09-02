self: super: with self; {
  boost_core = stdenv.mkDerivation rec {
    pname = "boost_core";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "core";
      rev = "boost-${version}";
      hash = "sha256-VMu7/fPXlGWxkasp7MmBEW/Jya6nUpGjZbp3KYUZ/5U=";
    };
  };
}
