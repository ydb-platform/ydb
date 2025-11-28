self: super: with self; {
  expected-lite = stdenv.mkDerivation rec {
    pname = "expected-lite";
    version = "0.9.0";

    src = fetchFromGitHub {
      owner = "martinmoene";
      repo = "expected-lite";
      rev = "v${version}";
      hash = "sha256-LRXxUaDQT5q9dXK2uYFvCgEuGWEHKr95lfdGTGjke0g=";
    };

    nativeBuildInputs = [ cmake ];
  };
}
