pkgs: attrs: with pkgs; rec {
  version = "2022.2.0";

  src = fetchFromGitHub {
      owner = "uxlfoundation";
      repo = "oneTBB";
      rev = "v${version}";
      hash = "sha256-ASQPAGm5e4q7imvTVWlmj5ON4fGEao1L5m2C5wF7EhI=";
  };

  patches = [];

  nativeBuildInputs = [ cmake ];
}
