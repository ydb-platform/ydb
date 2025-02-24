pkgs: attrs: with pkgs; rec {
  version = "2021.10.0";

  src = fetchFromGitHub {
      owner = "uxlfoundation";
      repo = "oneTBB";
      rev = "v${version}";
      hash = "sha256-HhZ4TBXqIZKkMB6bafOs8kt4EqkqStFjpoVQ3G+Rn4M=";
  };

  patches = [];

  nativeBuildInputs = [ cmake ];
}
