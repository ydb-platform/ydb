self: super: with self; {
  boost_intrusive = stdenv.mkDerivation rec {
    pname = "boost_intrusive";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "intrusive";
      rev = "boost-${version}";
      hash = "sha256-gwryUXNxNvnhzMxb+H4rxsAZSnfUJ/aqf++FZnkSYCw=";
    };
  };
}
