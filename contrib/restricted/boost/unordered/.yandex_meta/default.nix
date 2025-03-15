self: super: with self; {
  boost_unordered = stdenv.mkDerivation rec {
    pname = "boost_unordered";
    version = "1.77.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "unordered";
      rev = "boost-${version}";
      hash = "sha256-BTr6u7gQVQeU2eTBDPRpFBInlT6hnGpaeu2h8OCQANg=";
    };
  };
}
