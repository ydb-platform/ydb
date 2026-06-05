self: super: with self; {
  boost_charconv = stdenv.mkDerivation rec {
    pname = "boost_charconv";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "charconv";
      rev = "boost-${version}";
      hash = "sha256-f3zuQbJBx1AFrxHjaN98DTuPFljj7bMcdRUPADoNF/w=";
    };
  };
}
