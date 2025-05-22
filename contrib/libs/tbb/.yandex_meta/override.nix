pkgs: attrs: with pkgs; rec {
  version = "2022.0.0";

  src = fetchFromGitHub {
      owner = "uxlfoundation";
      repo = "oneTBB";
      rev = "v${version}";
      hash = "sha256-XOlC1+rf65oEGKDba9N561NuFo1YJhn3Q1CTGtvkn7A=";
  };

  patches = [];

  nativeBuildInputs = [ cmake ];
}
