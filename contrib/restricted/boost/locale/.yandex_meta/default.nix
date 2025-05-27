self: super: with self; {
  boost_locale = stdenv.mkDerivation rec {
    pname = "boost_locale";
    version = "1.88.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "locale";
      rev = "boost-${version}";
      hash = "sha256-2GL7iq6xpJDYbHk6B85CKTyV1E7873phb9MERw4Z2Cc=";
    };
  };
}
