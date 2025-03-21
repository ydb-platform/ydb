self: super: with self; {
  boost_mp11 = stdenv.mkDerivation rec {
    pname = "boost_mp11";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "mp11";
      rev = "boost-${version}";
      hash = "sha256-7lctl8+UTmeN5AzmJi0yRRYrH7gx5kSbik0xo3vWWHg=";
    };
  };
}
