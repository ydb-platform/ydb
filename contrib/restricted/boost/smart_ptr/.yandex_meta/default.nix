self: super: with self; {
  boost_smart_ptr = stdenv.mkDerivation rec {
    pname = "boost_smart_ptr";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "smart_ptr";
      rev = "boost-${version}";
      hash = "sha256-AdzrB7sIIBJ7VL4MC7b2ks9iihaHwKdEFDYFxutKni4=";
    };
  };
}
