{
  description = "rust-format-testing";
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };
  outputs = { self, nixpkgs, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
        };
        lib = nixpkgs.lib;
        buildInputs = with pkgs; [
          rustup
          #git

          clang
          llvm
          llvmPackages.libclang

          # pkg-config
          # openssl
        ];
        buildEnvironment = {
          LIBCLANG_PATH = pkgs.lib.makeLibraryPath [ pkgs.llvmPackages.libclang.lib ];
        };
      in
      {
        devShells.build = pkgs.mkShell (buildEnvironment // { inherit buildInputs; });
        devShells.default = pkgs.mkShell (buildEnvironment // {
          buildInputs = buildInputs ++ (with pkgs; [
            # dev tools
            rust-analyzer
          ]);
        });
      }
    );
}