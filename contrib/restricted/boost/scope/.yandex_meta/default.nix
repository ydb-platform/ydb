self: super: with self; {
  boost_scope = stdenv.mkDerivation rec {
    pname = "boost_scope";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "scope";
      rev = "boost-${version}";
      hash = "sha256-5j6Y+7v2scCRXuqWg7bC9unPFEGwQ/F9Dre2y/uNAus=";
    };
  };
}
