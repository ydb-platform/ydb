self: super: with self; {
  boost_context = stdenv.mkDerivation rec {
    pname = "boost_context";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "context";
      rev = "boost-${version}";
      hash = "sha256-f8+ECYXwDBF9/cyLdsuqeGe7LVycj6shHyAhhhl8uYQ=";
    };
  };
}
