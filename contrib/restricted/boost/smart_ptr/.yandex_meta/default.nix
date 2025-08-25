self: super: with self; {
  boost_smart_ptr = stdenv.mkDerivation rec {
    pname = "boost_smart_ptr";
    version = "1.88.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "smart_ptr";
      rev = "boost-${version}";
      hash = "sha256-FIByCGi/kWE4NQxQ3pcKGjmkfSt2WBeCM32eTPkch5w=";
    };
  };
}
