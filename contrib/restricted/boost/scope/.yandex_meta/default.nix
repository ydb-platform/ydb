self: super: with self; {
  boost_scope = stdenv.mkDerivation rec {
    pname = "boost_scope";
    version = "1.88.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "scope";
      rev = "boost-${version}";
      hash = "sha256-vxVFs8V0EfNkif+mcr3mrOQFNqoDuR3bpzmpzS5mOcU=";
    };
  };
}
