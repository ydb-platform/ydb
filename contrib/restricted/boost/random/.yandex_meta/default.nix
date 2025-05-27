self: super: with self; {
  boost_random = stdenv.mkDerivation rec {
    pname = "boost_random";
    version = "1.88.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "random";
      rev = "boost-${version}";
      hash = "sha256-cdB9ZHf2P9//plPnZD7ke+OR6c1iJwpRytYrB4lXKJY=";
    };
  };
}
