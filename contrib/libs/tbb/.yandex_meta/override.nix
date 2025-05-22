pkgs: attrs: with pkgs; rec {
  version = "2022.1.0";

  src = fetchFromGitHub {
      owner = "uxlfoundation";
      repo = "oneTBB";
      rev = "v${version}";
      hash = "sha256-DqJkNlC94cPJSXnhyFcEqWYGCQPunMfIfb05UcFGynw=";
  };

  patches = [];

  nativeBuildInputs = [ cmake ];
}
